# xmip-core-transport-sqlite

SQLite transport: a database file as a queue — a send inserts a row, a receive reads the oldest unclaimed rows and marks each one claimed once its receive cycle accepted it, and the row is the claim. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Acknowledgement

A row is consumed only after the runtime's whole receive cycle. A receive reads the oldest unclaimed rows in one statement and hands each back whole, a BLOB being one value, unclaimed. `Accepted` notes it, and the last verdict of the receive (`transport::together`) flips the claim of every accepted row (`UPDATE … SET claimed = 1 WHERE id = ? AND claimed = 0`) in one write transaction: one durable commit per receive, as before, where a commit per row costs the file's flush to disk once a row. `Refused` claims nothing: a refusal is not a consumption — a Stream refused at a transport gate was never written to the Ledger, so the row is the only copy. The row stays unclaimed, and the Location remembers it by its id with a hash of its payload (`transport::Refused`, the stamp every SQL technology remembers a row by, `transport::sql::accept::stamp`) and does not receive it again while it is unchanged; the memory is the node process's, so a node that starts again, or another node, receives it once more. `Failed` leaves it unclaimed, and the next receive takes it again. Two nodes polling one file may therefore both read a row until one accepts it; the second's flip finds nothing left. Until 2026-10-02 a receive claimed every row it read before handing them back.

## The declaration

What a Location of this technology is configured with is two `const`s in its `declaration` module, `src/declaration.rs`, beside the code, which `Configured` reads (ADR-0064, amendment 2026-11-26): `declaration::SETTINGS`, the Receive Location's `target`, and `SCHEMES`, `sqlite`. Built with its `declaration` feature alone (`--no-default-features --features declaration`), the crate is those and nothing more: its code and every crate the code is built on — the SQLite engine compiled in, and the transport capability itself, which the declaration takes nothing from — are behind the default feature. So the library the surfaces load can carry it without the code, for the language server and the desktop to suggest and check a SQLite Location's settings though no node in their process loaded the technology, once the gathering crate of open problem 48 links it. `test/Declaration.Test.ps1` in the estate holds that the declaration builds nothing the transport capability does not.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
