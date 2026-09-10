# xmip-core-transport-sqlite

SQLite transport: a database file as a queue — a send inserts a row, a receive takes and marks the oldest unclaimed rows in one transaction, and the row is the claim. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
