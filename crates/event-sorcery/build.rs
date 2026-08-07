//! Track the workspace migrations directory for `sqlx::migrate!`.
//!
//! The macro embeds migration files at expansion time but cannot register
//! the directory with cargo, so added, removed, or restored migration files
//! would otherwise leave stale artifacts. See docs/sqlx.md.

fn main() {
    println!("cargo:rerun-if-changed=../../migrations");
}
