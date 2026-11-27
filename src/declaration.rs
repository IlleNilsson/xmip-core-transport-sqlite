//! This technology's declaration: the transport guide's "The declaration",
//! `module/core/capability/transport/doc/adding-a-transport.md`.

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Every setting a Location takes beyond its address, the database file:
/// where a Send Location inserts and a Receive Location takes rows.
pub const SETTINGS: &Settings = &Settings {
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

/// This technology's schemes, as [`transport::Configured::SCHEMES`] defines
/// them.
pub const SCHEMES: &[&str] = &["sqlite"];
