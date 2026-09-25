#![forbid(unsafe_code)]

//! Streams that arrive as rows of one `SQLite` file. One row is one Stream.
//!
//! A `SQLite` file is the queue two processes on one box already share: a
//! producer inserts, an integrator polls, and the file's own locking is the
//! broker. Nothing is installed and nothing listens. A Send Location inserts
//! the Stream as one row of `xmip_transport` — `id`, `target`, `payload`,
//! `claimed` — and a Receive Location takes the oldest unclaimed rows and
//! marks them claimed in one transaction, so two nodes polling the same
//! file never take the same row. What is spoken is SQL to the engine
//! compiled into this crate; the file is the wire.
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

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub use claim::RowClaim;
use rusqlite::{Connection, ErrorCode, TransactionBehavior, params};
use transport::arrived::next_arrival;
use transport::claim::ResourceClaim;
use transport::error::{Result, TransportError};
use transport::held::Held;
use transport::loopback::{FarEnd, Loopback};
use transport::{Arrived, Directions, Transport};

/// The one table, created the first time the file is sent to.
pub const CREATE: &str = "CREATE TABLE IF NOT EXISTS xmip_transport (\
    id INTEGER PRIMARY KEY, \
    target TEXT NOT NULL, \
    payload BLOB NOT NULL, \
    claimed INTEGER NOT NULL DEFAULT 0)";
const INSERT: &str = "INSERT INTO xmip_transport (target, payload, claimed) VALUES (?1, ?2, 0)";
const TAKE_ALL: &str = "UPDATE xmip_transport SET claimed = 1 \
    WHERE claimed = 0 RETURNING id, target, payload";
const TAKE_TARGET: &str = "UPDATE xmip_transport SET claimed = 1 \
    WHERE claimed = 0 AND target = ?1 RETURNING id, target, payload";

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

    /// Take the oldest unclaimed rows, marking them claimed in the same
    /// transaction, oldest first.
    ///
    /// # Errors
    /// Where the file could not be opened or the engine refused — a file
    /// another writer holds is retryable, a file that is not a database is
    /// not.
    pub fn take(&self) -> Result<Vec<Arrived>> {
        let mut connection = self.open()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| engine_error(&error))?;
        let origin = self.origin();
        let rows = {
            let (sql, filter): (&str, Vec<&str>) = match &self.only {
                Some(target) => (TAKE_TARGET, vec![target.as_str()]),
                None => (TAKE_ALL, Vec::new()),
            };
            let mut statement = transaction
                .prepare(sql)
                .map_err(|error| engine_error(&error))?;
            let mut rows = statement
                .query_map(rusqlite::params_from_iter(filter), |row| {
                    let id: i64 = row.get(0)?;
                    let payload: Vec<u8> = row.get(2)?;
                    Ok(Arrived::new(format!("{origin}{id}"), payload))
                })
                .map_err(|error| engine_error(&error))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| engine_error(&error))?;
            // RETURNING answers in no promised order; the queue is oldest first.
            rows.sort_by_key(|arrived| row_of(&arrived.origin_uri));
            rows
        };
        transaction.commit().map_err(|error| engine_error(&error))?;
        Ok(rows)
    }
}

impl Transport for SqliteTransport {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// The oldest unclaimed rows, claimed. A file nobody has sent to yet is
    /// not an error: an empty vector.
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
            )
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

/// The row an origin names, or zero where it names none.
fn row_of(origin: &str) -> i64 {
    origin
        .rsplit_once("?row=")
        .and_then(|(_, id)| id.parse().ok())
        .unwrap_or(0)
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
    use transport::payload::edge_payloads;

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
        let arrived = queue.receive().expect("receiving");
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

    #[test]
    fn a_receive_filtered_on_a_target_leaves_the_other_rows_unclaimed() {
        let dir = scratch("only");
        let path = dir.join("inbox.sqlite");
        let queue = SqliteTransport::new(&path);
        queue.send("orders", b"order").expect("sending");
        queue.send("invoices", b"invoice").expect("sending");
        let invoices = SqliteTransport::new(&path).only("invoices");
        let arrived = invoices.receive().expect("receiving");
        assert_eq!(arrived.len(), 1);
        assert_eq!(arrived[0].bytes, b"invoice");
        let rest = queue.receive().expect("the rest");
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
