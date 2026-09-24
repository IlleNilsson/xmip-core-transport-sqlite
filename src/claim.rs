//! The row's own claim: its `claimed` column, flipped in one statement.
//!
//! ADR-0024 clause 4: the artefact, not the location. Two nodes may poll
//! one file at the same time and take different rows, because the engine
//! serialises the two `UPDATE`s and the second finds nothing left to flip.

use std::path::PathBuf;

use rusqlite::Connection;
use transport::claim::{Artefact, Claimed, ResourceClaim};
use transport::error::{Result, TransportError};

use crate::engine_error;

const IS_AVAILABLE: &str = "SELECT claimed FROM xmip_transport WHERE id = ?1";
const CLAIM: &str = "UPDATE xmip_transport SET claimed = 1 WHERE id = ?1 AND claimed = 0";
const RELEASE: &str = "UPDATE xmip_transport SET claimed = 0 WHERE id = ?1 AND claimed = 1";

/// The claim on one row of one file, addressed as the row's origin URI.
#[derive(Clone, Debug)]
pub struct RowClaim {
    path: PathBuf,
}

impl RowClaim {
    /// The claim over the rows of the file at `path`.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    fn open(&self) -> Result<Connection> {
        crate::SqliteTransport::new(&self.path).open()
    }

    /// The row an artefact names, refusing one of another file: a row id
    /// means nothing outside the file that issued it.
    fn row_of(&self, artefact: &Artefact) -> Result<i64> {
        let address = artefact.address();
        let (file, row) = address
            .rsplit_once("?row=")
            .ok_or_else(|| TransportError::permanent(format!("{address} does not name a row")))?;
        let expected = format!("sqlite://{}", net::uri::path_of(&self.path));
        if file != expected {
            return Err(TransportError::permanent(format!(
                "{address} is not a row of {}",
                self.path.display()
            )));
        }
        row.parse()
            .map_err(|_| TransportError::permanent(format!("{address} does not name a row")))
    }
}

impl ResourceClaim for RowClaim {
    fn is_available(&self, artefact: &Artefact) -> Result<bool> {
        let row = self.row_of(artefact)?;
        let claimed: Option<i64> = self
            .open()?
            .query_row(IS_AVAILABLE, [row], |r| r.get(0))
            .map_err(|error| engine_error(&error))?;
        Ok(claimed == Some(0))
    }

    fn claim(&self, artefact: &Artefact) -> Result<Claimed> {
        let row = self.row_of(artefact)?;
        let flipped = self
            .open()?
            .execute(CLAIM, [row])
            .map_err(|error| engine_error(&error))?;
        if flipped == 1 {
            Ok(Claimed::new(artefact.clone(), row.to_string()))
        } else {
            Err(TransportError::permanent(format!(
                "{artefact} is held, or is no row"
            )))
        }
    }

    fn release(&self, claimed: Claimed) -> Result<()> {
        let row = self.row_of(&claimed.artefact)?;
        self.open()?
            .execute(RELEASE, [row])
            .map_err(|error| engine_error(&error))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqliteTransport;
    use transport::Transport;

    fn scratch(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos());
        let dir = std::env::temp_dir().join(format!(
            "xmip-transport-sqlite-claim-{name}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::remove_dir_all(&dir).ok();
        dir
    }

    #[test]
    fn a_row_is_claimed_once_and_released_by_its_holder() {
        let dir = scratch("once");
        let queue = SqliteTransport::new(dir.join("inbox.sqlite"));
        queue.send("orders", b"order").expect("sending");
        let claim = RowClaim::new(queue.path());
        let artefact = Artefact::new(format!("{}1", queue.origin()));
        assert!(claim.is_available(&artefact).expect("asked"));
        let held = claim.claim(&artefact).expect("claimed");
        assert_eq!(held.token, "1");
        assert!(!claim.is_available(&artefact).expect("asked"));
        let second = claim.claim(&artefact).expect_err("held");
        assert!(!second.retryable);
        assert!(
            queue.receive().expect("receiving").is_empty(),
            "a claimed row is not received"
        );
        claim.release(held).expect("released");
        assert!(claim.is_available(&artefact).expect("asked"));
        assert_eq!(queue.receive().expect("receiving").len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_row_of_another_file_or_no_row_at_all_is_refused() {
        let dir = scratch("other");
        let queue = SqliteTransport::new(dir.join("inbox.sqlite"));
        queue.send("orders", b"order").expect("sending");
        let claim = RowClaim::new(queue.path());
        let other = Artefact::new("sqlite:///elsewhere/inbox.sqlite?row=1");
        assert!(claim.is_available(&other).is_err());
        assert!(claim.claim(&Artefact::new("sqlite:///x")).is_err());
        let missing = Artefact::new(format!("{}99", queue.origin()));
        assert!(claim.claim(&missing).is_err(), "no such row");
        std::fs::remove_dir_all(&dir).ok();
    }
}
