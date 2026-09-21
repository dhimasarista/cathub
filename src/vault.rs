use chrono::Utc;
use sqlx::AnyPool;

/// The database backend behind an `sqlx::Any` pool.
///
/// `sqlx::Any` does not normalize SQL between backends: placeholder syntax
/// (`$1` vs `?`) and UPSERT syntax (`ON CONFLICT` vs `ON DUPLICATE KEY UPDATE`)
/// differ, so every statement here is generated per-backend rather than
/// written once and assumed to be portable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Postgres,
    MySql,
    Sqlite,
}

impl Backend {
    /// Detects the backend from a `DATABASE_URL` scheme, mirroring the
    /// matching sqlx does internally for `AnyKind`.
    pub fn detect(database_url: &str) -> Option<Backend> {
        if database_url.starts_with("postgres:") || database_url.starts_with("postgresql:") {
            Some(Backend::Postgres)
        } else if database_url.starts_with("mysql:") || database_url.starts_with("mariadb:") {
            Some(Backend::MySql)
        } else if database_url.starts_with("sqlite:") {
            Some(Backend::Sqlite)
        } else {
            None
        }
    }

    fn create_table_sql(self) -> &'static str {
        match self {
            Backend::Postgres | Backend::Sqlite => {
                "CREATE TABLE IF NOT EXISTS telemetry_latest (
                    topic TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )"
            }
            Backend::MySql => {
                "CREATE TABLE IF NOT EXISTS telemetry_latest (
                    topic VARCHAR(255) PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at VARCHAR(64) NOT NULL
                )"
            }
        }
    }

    fn upsert_sql(self) -> &'static str {
        match self {
            Backend::Postgres => {
                "INSERT INTO telemetry_latest (topic, payload, updated_at) VALUES ($1, $2, $3)
                 ON CONFLICT (topic) DO UPDATE SET payload = EXCLUDED.payload, updated_at = EXCLUDED.updated_at"
            }
            Backend::Sqlite => {
                "INSERT INTO telemetry_latest (topic, payload, updated_at) VALUES (?, ?, ?)
                 ON CONFLICT(topic) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at"
            }
            Backend::MySql => {
                "INSERT INTO telemetry_latest (topic, payload, updated_at) VALUES (?, ?, ?)
                 ON DUPLICATE KEY UPDATE payload = VALUES(payload), updated_at = VALUES(updated_at)"
            }
        }
    }
}

/// Creates the `telemetry_latest` table if it doesn't already exist.
pub async fn ensure_schema(pool: &AnyPool, backend: Backend) -> Result<(), sqlx::Error> {
    sqlx::query(backend.create_table_sql()).execute(pool).await?;
    Ok(())
}

/// Idempotently persists the latest payload for `topic`. Values and
/// parameters are always bound, never interpolated into the SQL text.
pub async fn upsert_latest(
    pool: &AnyPool,
    backend: Backend,
    topic: &str,
    payload: &str,
) -> Result<(), sqlx::Error> {
    let updated_at = Utc::now().to_rfc3339();
    sqlx::query(backend.upsert_sql())
        .bind(topic)
        .bind(payload)
        .bind(updated_at)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_backend_from_url_scheme() {
        assert_eq!(Backend::detect("postgres://u:p@host/db"), Some(Backend::Postgres));
        assert_eq!(Backend::detect("postgresql://u:p@host/db"), Some(Backend::Postgres));
        assert_eq!(Backend::detect("mysql://u:p@host/db"), Some(Backend::MySql));
        assert_eq!(Backend::detect("mariadb://u:p@host/db"), Some(Backend::MySql));
        assert_eq!(Backend::detect("sqlite://cathub.db"), Some(Backend::Sqlite));
        assert_eq!(Backend::detect("mongodb://host/db"), None);
    }

    #[test]
    fn postgres_and_sqlite_use_distinct_placeholder_and_conflict_syntax() {
        assert!(Backend::Postgres.upsert_sql().contains('$'));
        assert!(!Backend::Sqlite.upsert_sql().contains('$'));
        assert!(Backend::MySql.upsert_sql().contains("ON DUPLICATE KEY UPDATE"));
        assert!(Backend::Postgres.upsert_sql().contains("ON CONFLICT"));
    }
}
