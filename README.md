# xmip-core-transport-sqlite

SQLite transport: a database file as a queue — a send inserts a row, a receive reads the oldest unclaimed rows and marks each one claimed once its receive cycle accepted or refused it, and the row is the claim. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Acknowledgement

A row is consumed only after the runtime's whole receive cycle. A receive reads the oldest unclaimed rows in one statement and hands each back whole, a BLOB being one value, unclaimed. `Accepted` notes it, and the last verdict of the receive (`transport::together`) flips the claim of every accepted row (`UPDATE … SET claimed = 1 WHERE id = ? AND claimed = 0`) in one write transaction: one durable commit per receive, as before, where a commit per row costs the file's flush to disk once a row. `Refused` flips its claim in the same transaction: the table has no place for a refused row, the runtime audited the refusal, and from Message creation on the Stream is kept in Xmip (ADR-0013). `Failed` leaves it unclaimed, and the next receive takes it again. Two nodes polling one file may therefore both read a row until one accepts it; the second's flip finds nothing left. Until 2026-10-02 a receive claimed every row it read before handing them back.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
