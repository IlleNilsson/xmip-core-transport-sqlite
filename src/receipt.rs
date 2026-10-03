//! One receive's rows, and the one transaction that claims those accepted.
//!
//! A receive reads the unclaimed rows in one statement and hands each back
//! whole, a BLOB being one value; a statement per row would take and give
//! back the file's lock once a row. Once every row has its verdict
//! (`transport::together`), the accepted and the refused ones are claimed
//! in one write transaction — one durable commit for the receive, as when
//! a receive claimed every row it read, where a commit per row would cost
//! the file's flush to disk once a row. A refused row is claimed because
//! the table has no place for it: the runtime audited the refusal, and the
//! Stream is kept in Xmip from Message creation on (ADR-0013); unclaimed,
//! it would be refused again on every receive. A failed row is left
//! unclaimed for the next receive. A row whose verdict never comes is not
//! claimed.

use rusqlite::Connection;
use transport::Arrived;
use transport::Verdict;
use transport::error::Result;
use transport::together::together;

use crate::claim::CLAIM;
use crate::engine_error;

/// The rows read on `connection`, id and payload, as arrivals from
/// `origin` and the id; the last verdict claims the accepted and the
/// refused ones.
#[must_use]
pub fn arrivals(
    mut connection: Connection,
    rows: Vec<(i64, Vec<u8>)>,
    origin: &str,
) -> Vec<Arrived> {
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let acknowledgements = together(
        rows.len(),
        |_, _| Ok(()),
        move |verdicts| {
            let consumed: Vec<i64> = ids
                .iter()
                .zip(verdicts)
                .filter(|(_, verdict)| {
                    matches!(verdict, Some(Verdict::Accepted | Verdict::Refused(_)))
                })
                .map(|(id, _)| *id)
                .collect();
            claim(&mut connection, &consumed)
        },
    );
    rows.into_iter()
        .zip(acknowledgements)
        .map(|((id, payload), acknowledgement)| {
            Arrived::whole(format!("{origin}{id}"), payload, acknowledgement)
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
