#![forbid(unsafe_code)]

//! Streams that arrive as rows of one `SQLite` file. One row is one Stream.
//!
//! A `SQLite` file is the queue two processes on one box already share: a
//! producer inserts, an integrator polls, and the file's own locking is the
//! broker. Nothing is installed and nothing listens. A Send Location inserts
//! the Stream as one row of `xmip_transport` — `id`, `target`, `payload`,
//! `claimed` — and a Receive Location reads the oldest unclaimed rows and
//! marks each claimed only when its receive cycle accepted it: a refused
//! row stays unclaimed for the next receive, and two nodes polling the same
//! file may both read a row until one accepts it. What is spoken is SQL to
//! the engine compiled into this crate; the file is the wire.
//!
//! The row is the artefact, and the `claimed` column is its claim, per
//! ADR-0024: taken at the endpoint, atomic because the engine makes it so,
//! and visible to anything else that opens the file. `claim.rs` speaks it.
//! Nothing is deleted — a claimed row stays as the record of what passed,
//! which is ADR-0040's word on deleting — and no ceiling: a payload is a
//! BLOB, and the engine takes a mebibyte as readily as a byte.
//!
//! The origin URI names the row: `sqlite:///C:/queue/inbox.sqlite?row=41`.
//! A send target is the row's `target` column — a Send Location's name for
//! where the row is going, which a Receive Location may filter on.

pub mod claim;
pub mod receipt;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub use claim::RowClaim;
use rusqlite::{Connection, ErrorCode, params};
use transport::arrived::next_arrival;
use transport::claim::ResourceClaim;
use transport::error::{Result, TransportError};
use transport::held::Held;
use transport::loopback::{FarEnd, Loopback};
use transport::{Arrived, Configured, Directions, Transport};
use xcore::settings::{Applies, Kind, Presence, Read, Setting, Settings};

/// The one table, created the first time the file is sent to.
pub const CREATE: &str = "CREATE TABLE IF NOT EXISTS xmip_transport (\
    id INTEGER PRIMARY KEY, \
    target TEXT NOT NULL, \
    payload BLOB NOT NULL, \
    claimed INTEGER NOT NULL DEFAULT 0)";
const INSERT: &str = "INSERT INTO xmip_transport (target, payload, claimed) VALUES (?1, ?2, 0)";
const UNCLAIMED_ALL: &str = "SELECT id, payload FROM xmip_transport WHERE claimed = 0 ORDER BY id";
const UNCLAIMED_TARGET: &str =
    "SELECT id, payload FROM xmip_transport WHERE claimed = 0 AND target = ?1 ORDER BY id";

pub struct SqliteTransport {
    path: PathBuf,
    only: Option<String>,
    claim: RowClaim,
}

impl SqliteTransport {
    /// The queue in the database file at `path`; the file, its directory
    /// and the table are created on first send.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        Self {
            claim: RowClaim::new(&path),
            path,
            only: None,
        }
    }

    /// Take only rows sent to `target`; every row otherwise.
    #[must_use]
    pub fn only(mut self, target: impl Into<String>) -> Self {
        self.only = Some(target.into());
        self
    }

    /// Where the queue is.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The origin every row of this file shares, up to the row id:
    /// `sqlite:///<path>?row=`.
    #[must_use]
    pub fn origin(&self) -> String {
        format!("sqlite://{}?row=", net::uri::path_of(&self.path))
    }

    /// The file, opened with its table in place.
    ///
    /// # Errors
    /// Where the directory could not be made or the file is not a database.
    pub fn open(&self) -> Result<Connection> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| transport::error::classify("creating the queue directory", &e))?;
        }
        let connection = Connection::open(&self.path).map_err(|error| engine_error(&error))?;
        connection
            .execute(CREATE, [])
            .map_err(|error| engine_error(&error))?;
        Ok(connection)
    }

    /// The oldest unclaimed rows, oldest first, read in one statement; the
    /// accepted ones are claimed in one transaction once every row has its
    /// verdict, and a refused row stays unclaimed for the next receive
    /// ([`receipt`]). One connection serves the receive and its rows'
    /// verdicts.
    ///
    /// # Errors
    /// Where the file could not be opened or the engine refused — a file
    /// another writer holds is retryable, a file that is not a database is
    /// not.
    pub fn take(&self) -> Result<Vec<Arrived>> {
        let connection = self.open()?;
        let rows = {
            let (sql, filter): (&str, Vec<&str>) = match &self.only {
                Some(target) => (UNCLAIMED_TARGET, vec![target.as_str()]),
                None => (UNCLAIMED_ALL, Vec::new()),
            };
            let mut statement = connection
                .prepare(sql)
                .map_err(|error| engine_error(&error))?;
            statement
                .query_map(rusqlite::params_from_iter(filter), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(|error| engine_error(&error))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| engine_error(&error))?
        };
        Ok(receipt::arrivals(connection, rows, &self.origin()))
    }
}

impl Transport for SqliteTransport {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    fn arrivals(&self) -> transport::Arrivals {
        transport::Arrivals::Ordered("a poll reads again what is not yet told")
    }

    /// The oldest unclaimed rows, each claimed on `Accepted` and left
    /// unclaimed on `Refused` ([`Self::take`]). A file nobody has sent to
    /// yet is not an error: an empty vector.
    fn receive(&self) -> Result<Vec<Arrived>> {
        if !self.path.is_file() {
            return Ok(Vec::new());
        }
        self.take()
    }

    /// Insert the bytes as one row addressed to `target`.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let connection = self.open()?;
        connection
            .execute(INSERT, params![target, bytes])
            .map_err(|error| engine_error(&error))?;
        Ok(())
    }

    /// The row's own claim: its `claimed` column.
    fn claims(&self) -> Option<&dyn ResourceClaim> {
        Some(&self.claim)
    }
}

impl Configured for SqliteTransport {
    /// The address is the database file: where a Send Location inserts and a
    /// Receive Location takes rows.
    const SETTINGS: &'static Settings = &Settings {
        technology: env!("CARGO_PKG_NAME"),
        settings: &[Setting {
            name: "target",
            kind: Kind::Text,
            presence: Presence::Optional,
            meaning: "The target a Receive Location takes rows sent to; every row when left \
                      out.",
            applies: Applies::Receive,
        }],
    };

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        let transport = Self::new(address);
        Ok(match settings.optional_text("target") {
            Some(target) => transport.only(target),
            None => transport,
        })
    }
}

impl SqliteTransport {
    /// Both ends in one directory: send a row into a file there, take it
    /// back from the same file. Nothing listens; the file is the wire, so
    /// there is no port and no timeout.
    #[must_use]
    pub fn loopback(root: impl Into<PathBuf>) -> Self {
        Self::new(root)
    }

    /// One file per thread: pairs driven at once from several threads would
    /// otherwise contend for one file's write lock, which the engine
    /// answers with busy rather than waiting. Per thread rather than per
    /// round so the table is made once.
    fn thread_file(&self) -> PathBuf {
        self.path
            .join(format!("t{:?}.sqlite", std::thread::current().id()))
    }
}

/// Rounds begun, so each round's row is addressed to that round alone and
/// a round that failed after its send leaves nothing the next one takes.
static ROUNDS: AtomicU64 = AtomicU64::new(1);

impl Loopback for SqliteTransport {
    /// The row a round is addressed to. Nothing waits: the round is in
    /// order.
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let round = ROUNDS.fetch_add(1, Ordering::Relaxed);
        let target = format!("round-{round}");
        let file = self.thread_file();
        Ok(Box::new(Held::new(target.clone(), move || {
            next_arrival(
                SqliteTransport::new(file).only(target).receive()?,
                "sent, but it did not come back",
            )?
            .taken()
        })))
    }

    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        Self::new(self.thread_file()).send(address, payload)
    }

    /// In order on one thread: a file does not listen, so the insert goes
    /// first and the take finds it.
    fn exchanges_in_order(&self) -> bool {
        true
    }
}

/// What the engine said, judged: a busy or locked file will free up, a
/// file that is not a database will not.
#[must_use]
pub fn engine_error(error: &rusqlite::Error) -> TransportError {
    let retryable = matches!(
        error.sqlite_error_code(),
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked | ErrorCode::SystemIoFailure)
    );
    TransportError {
        message: format!("the engine: {error}"),
        retryable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read as _;
    use transport::payload::edge_payloads;

    #[test]
    fn sqlite_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert_eq!(SqliteTransport::SETTINGS.problems(), Vec::<String>::new());
        let given = [("target".to_string(), Given::Text("orders".to_string()))];
        let built = <SqliteTransport as Configured>::open("queue.sqlite", Applies::Receive, &given)
            .expect("configured");
        assert_eq!(built.path(), Path::new("queue.sqlite"));
        assert_eq!(built.only.as_deref(), Some("orders"));
        let Err(refused) =
            <SqliteTransport as Configured>::open("queue.sqlite", Applies::Send, &given)
        else {
            panic!("a Send Location takes no rows");
        };
        assert!(refused.message.contains("\"target\""), "{refused}");
    }

    fn scratch(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos());
        let dir = std::env::temp_dir().join(format!(
            "xmip-transport-sqlite-{name}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::remove_dir_all(&dir).ok();
        dir
    }

    #[test]
    fn a_sent_row_is_received_once_oldest_first_and_stays_claimed() {
        let dir = scratch("queue");
        let queue = SqliteTransport::new(dir.join("inbox.sqlite"));
        queue.send("orders", b"first").expect("sending");
        queue.send("orders", &[0xff, 0x00, 0xfe]).expect("sending");
        queue.send("invoices", b"third").expect("sending");
        let arrived = taken_all(&queue);
        assert_eq!(arrived.len(), 3);
        assert_eq!(arrived[0].bytes, b"first");
        assert_eq!(arrived[1].bytes, [0xff, 0x00, 0xfe], "bytes as they are");
        assert!(arrived[0].origin_uri.starts_with("sqlite:///"));
        assert!(arrived[0].origin_uri.ends_with("inbox.sqlite?row=1"));
        assert!(arrived[2].origin_uri.ends_with("?row=3"));
        assert!(!arrived[0].origin_uri.contains(char::from(92)));
        assert!(
            queue.receive().expect("again").is_empty(),
            "a claimed row is not taken twice"
        );
        let count: i64 = queue
            .open()
            .expect("open")
            .query_row(
                "SELECT COUNT(*) FROM xmip_transport WHERE claimed = 1",
                [],
                |r| r.get(0),
            )
            .expect("count");
        assert_eq!(count, 3, "nothing is deleted");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Every row a receive found, read and accepted.
    fn taken_all(queue: &SqliteTransport) -> Vec<transport::Taken> {
        queue
            .receive()
            .expect("receiving")
            .into_iter()
            .map(|one| one.taken().expect("taken"))
            .collect()
    }

    #[test]
    fn a_failed_row_stays_unclaimed_and_an_accepted_and_a_refused_one_are_claimed() {
        let dir = scratch("verdict");
        let queue = SqliteTransport::new(dir.join("inbox.sqlite"));
        queue.send("orders", b"first").expect("sending");
        queue.send("orders", b"second").expect("sending");
        queue.send("orders", b"third").expect("sending");
        let mut arrived = queue.receive().expect("receiving");
        assert!(arrived.iter().all(Arrived::defers));
        // The first read and failed, the second failed unread, the third
        // refused.
        let (_, mut body, acknowledgement) = arrived.remove(0).into_parts();
        let mut read = Vec::new();
        body.read_to_end(&mut read).expect("reading");
        assert_eq!(read, b"first");
        drop(body);
        acknowledgement
            .acknowledge(transport::Verdict::Failed)
            .expect("failed");
        arrived.remove(0).failed().expect("failed");
        arrived
            .remove(0)
            .refused(transport::Refusal::Unacceptable)
            .expect("refused");
        let again = taken_all(&queue);
        assert_eq!(again.len(), 2, "failed rows are taken again, refused not");
        assert_eq!(
            (again[0].bytes.as_slice(), again[1].bytes.as_slice()),
            (&b"first"[..], &b"second"[..])
        );
        assert!(
            queue.receive().expect("once more").is_empty(),
            "accepted and refused rows are claimed"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_receive_filtered_on_a_target_leaves_the_other_rows_unclaimed() {
        let dir = scratch("only");
        let path = dir.join("inbox.sqlite");
        let queue = SqliteTransport::new(&path);
        queue.send("orders", b"order").expect("sending");
        queue.send("invoices", b"invoice").expect("sending");
        let invoices = SqliteTransport::new(&path).only("invoices");
        let arrived = taken_all(&invoices);
        assert_eq!(arrived.len(), 1);
        assert_eq!(arrived[0].bytes, b"invoice");
        let rest = taken_all(&queue);
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].bytes, b"order");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_file_nobody_has_sent_to_is_empty_and_a_file_that_is_no_database_is_refused() {
        let dir = scratch("absent");
        let queue = SqliteTransport::new(dir.join("never.sqlite"));
        assert!(queue.receive().expect("not an error").is_empty());
        assert!(!dir.exists(), "a receive creates nothing");
        std::fs::create_dir_all(&dir).expect("dir");
        let garbage = dir.join("garbage.sqlite");
        std::fs::write(
            &garbage,
            b"this is not a database, and it is long enough to say so",
        )
        .expect("write");
        let error = SqliteTransport::new(&garbage)
            .receive()
            .expect_err("not a database");
        assert!(!error.retryable, "{error}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_row_is_the_claim_and_the_transport_says_so() {
        let dir = scratch("claim");
        let queue = SqliteTransport::new(dir.join("inbox.sqlite"));
        assert_eq!(queue.name(), "sqlite");
        assert_eq!(queue.directions(), Directions::BOTH);
        assert!(queue.claims().is_some(), "the claimed column");
        assert!(queue.origin().starts_with("sqlite:///"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_loopback_sends_a_row_and_takes_it_back_from_the_same_file() {
        let dir = scratch("loopback");
        let pair = SqliteTransport::loopback(&dir);
        let arrived = pair.round(b"a row").expect("round");
        assert_eq!(arrived.bytes, b"a row");
        assert!(arrived.origin_uri.starts_with("sqlite:///"));
        assert!(arrived.origin_uri.contains("?row="));
        let again = pair.round(b"another").expect("a second round");
        assert_eq!(again.bytes, b"another");
        assert_ne!(
            arrived.origin_uri, again.origin_uri,
            "each round its own row"
        );
        assert_eq!(pair.name(), "sqlite");
        assert_eq!(pair.ceiling(), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole() {
        let dir = scratch("edges");
        let pair = SqliteTransport::loopback(&dir);
        for (name, payload) in edge_payloads() {
            assert!(pair.refuses(&payload).is_none(), "{name}");
            let arrived = pair.round(&payload).expect(name);
            assert_eq!(arrived.bytes, payload, "{name}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
