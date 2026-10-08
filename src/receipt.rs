//! One receive's rows, and the one transaction that claims those accepted.
//!
//! A receive reads the unclaimed rows in one statement and hands each back
//! whole, a BLOB being one value; a statement per row would take and give
//! back the file's lock once a row. Once every row has its verdict
//! (`transport::together`), the accepted ones are claimed in one write
//! transaction — one durable commit for the receive, as when a receive
//! claimed every row it read, where a commit per row would cost the file's
//! flush to disk once a row. A refusal is not a consumption: a Stream
//! refused at a transport gate was never written to the Ledger, so the row
//! is the only copy. A refused row is left unclaimed and remembered by its
//! id with a hash of its payload ([`RefusedRows`]), and no receive hands it
//! on again while it is unchanged. A failed row is left unclaimed for the
//! next receive. A row whose verdict never comes is not claimed.

use std::hash::{DefaultHasher, Hash, Hasher};

use rusqlite::Connection;
use transport::Arrived;
use transport::Refused;
use transport::Verdict;
use transport::error::Result;
use transport::together::together;

use crate::claim::CLAIM;
use crate::engine_error;

/// The rows a Location refused and left unclaimed: each by its id, with a
/// hash of its payload.
pub type RefusedRows = Refused<i64, u64>;

/// The rows read on `connection`, id and payload, but those `refused`
/// holds as they lie, as arrivals from `origin` and the id; the last
/// verdict claims the accepted ones, and a refused one is remembered.
#[must_use]
pub fn arrivals(
    mut connection: Connection,
    rows: Vec<(i64, Vec<u8>)>,
    refused: &RefusedRows,
    origin: &str,
) -> Vec<Arrived> {
    let stamp = |payload: &Vec<u8>| {
        let mut hasher = DefaultHasher::new();
        payload.hash(&mut hasher);
        hasher.finish()
    };
    let rows = refused.sift(rows, |(id, _)| id, |(_, payload)| Some(stamp(payload)));
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let acknowledgements = together(
        rows.len(),
        |_, _| Ok(()),
        move |verdicts| {
            let consumed: Vec<i64> = ids
                .iter()
                .zip(verdicts)
                .filter(|(_, verdict)| matches!(verdict, Some(Verdict::Accepted)))
                .map(|(id, _)| *id)
                .collect();
            claim(&mut connection, &consumed)
        },
    );
    rows.into_iter()
        .zip(acknowledgements)
        .map(|((id, payload), acknowledgement)| {
            let told = refused.remembering(id, stamp(&payload), acknowledgement);
            Arrived::whole(format!("{origin}{id}"), payload, told).detected()
        })
        .collect()
}

/// Claim `consumed` in one transaction.
fn claim(connection: &mut Connection, consumed: &[i64]) -> Result<()> {
    if consumed.is_empty() {
        return Ok(());
    }
    let transaction = connection
        .transaction()
        .map_err(|error| engine_error(&error))?;
    {
        let mut claim = transaction
            .prepare_cached(CLAIM)
            .map_err(|error| engine_error(&error))?;
        for id in consumed {
            claim.execute([id]).map_err(|error| engine_error(&error))?;
        }
    }
    transaction.commit().map_err(|error| engine_error(&error))
}
