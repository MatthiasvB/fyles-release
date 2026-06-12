use rusqlite::{Connection, Result};
use semver::Version;
use std::fmt;
use tracing::info;

mod all_migrations;
pub use all_migrations::get_migrations;

#[allow(unused)]
pub struct Migration {
    applies_to_version: Version,
    target_version: Version,
    description: &'static str,
    up: Box<dyn Fn(&Connection) -> Result<()>>,
    // TODO: implement downgrades
    down: Box<dyn Fn(&Connection) -> Result<()>>,
}

impl fmt::Debug for Migration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Migration")
            .field("from", &self.applies_to_version)
            .field("to", &self.target_version)
            .field("description", &self.description)
            .finish()
    }
}

impl Migration {
    pub fn new<F1, F2>(
        applies_to_version: Version,
        target_version: Version,
        description: &'static str,
        up: F1,
        down: F2,
    ) -> Self
    where
        F1: Fn(&Connection) -> Result<()> + 'static,
        F2: Fn(&Connection) -> Result<()> + 'static,
    {
        Migration {
            applies_to_version,
            target_version,
            description,
            up: Box::new(up),
            down: Box::new(down),
        }
    }

    pub fn upgrade(&self, conn: &Connection) -> Result<()> {
        (self.up)(conn)?;
        conn.execute(
            "UPDATE schema_version SET version = ?",
            [self.target_version.to_string()],
        )?;
        Ok(())
    }

    #[allow(unused)]
    pub fn rollback(&self, conn: &Connection) -> Result<()> {
        (self.down)(conn)?;
        conn.execute(
            "UPDATE schema_version SET version = ?",
            [self.applies_to_version.to_string()],
        )?;
        Ok(())
    }
}

fn get_db_version(conn: &Connection) -> Result<Version> {
    let version_str: String =
        conn.query_row("SELECT version FROM schema_version", [], |row| row.get(0))?;
    Ok(Version::parse(&version_str).unwrap())
}

pub fn run_migrations(conn: &mut Connection) -> Result<()> {
    let current_version = get_db_version(conn)?;
    info!("Current DB version: {}", current_version);

    let migrations: Vec<Migration> = get_migrations();

    for migration in migrations
        .iter()
        .filter(|m| m.applies_to_version >= current_version)
    {
        info!(
            "Applying migration: {} ({} -> {})",
            migration.description, migration.applies_to_version, migration.target_version
        );
        let tx = conn.transaction()?;
        migration.upgrade(&tx)?;
        tx.commit()?;
        info!(
            "Applied migration: {} ({} -> {})",
            migration.description, migration.applies_to_version, migration.target_version
        );
    }
    Ok(())
}
