use anyhow::{anyhow, Result};

/// Converts a SQLAlchemy connection string to a Diesel-compatible one.
///
/// - For postgres: `postgresql+psycopg2://...` -> `postgresql://...`
/// - For mysql: `mysql+pymysql://...` -> `mysql://...`
/// - For sqlite: `sqlite:///path/to/db.db` -> `path/to/db.db`
pub fn to_diesel_conn_string(sqlalchemy_conn: &str) -> Result<String> {
    if let Some(conn) = sqlalchemy_conn.strip_prefix("sqlite:///") {
        return Ok(conn.to_string());
    }

    if let Some((scheme_part, rest)) = sqlalchemy_conn.split_once("://") {
        let db_type = if scheme_part.starts_with("postgresql") {
            "postgresql"
        } else if scheme_part.starts_with("mysql") {
            "mysql"
        } else {
            return Err(anyhow!(
                "Unsupported database type from connection string: {}",
                sqlalchemy_conn
            ));
        };
        return Ok(format!("{}://{}", db_type, rest));
    }

    Err(anyhow!(
        "Invalid SQLAlchemy connection string format: {}",
        sqlalchemy_conn
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_with_driver() {
        let pg_conn = "postgresql+psycopg2://user:pass@host:port/dbname";
        assert_eq!(
            to_diesel_conn_string(pg_conn).unwrap(),
            "postgresql://user:pass@host:port/dbname"
        );
    }

    #[test]
    fn test_postgres_no_driver() {
        let pg_conn_no_driver = "postgresql://user:pass@host:port/dbname";
        assert_eq!(
            to_diesel_conn_string(pg_conn_no_driver).unwrap(),
            "postgresql://user:pass@host:port/dbname"
        );
    }

    #[test]
    fn test_mysql_with_driver() {
        let mysql_conn = "mysql+pymysql://user:pass@host:port/dbname";
        assert_eq!(
            to_diesel_conn_string(mysql_conn).unwrap(),
            "mysql://user:pass@host:port/dbname"
        );
    }

    #[test]
    fn test_mysql_no_driver() {
        let mysql_conn_no_driver = "mysql://user:pass@host:port/dbname";
        assert_eq!(
            to_diesel_conn_string(mysql_conn_no_driver).unwrap(),
            "mysql://user:pass@host:port/dbname"
        );
    }

    #[test]
    fn test_sqlite() {
        let sqlite_conn = "sqlite:///path/to/my/db.db";
        assert_eq!(
            to_diesel_conn_string(sqlite_conn).unwrap(),
            "path/to/my/db.db"
        );
    }

    #[test]
    fn test_unsupported_db() {
        let unsupported_conn = "oracle+cx_oracle://user:pass@host:port/dbname";
        let err = to_diesel_conn_string(unsupported_conn).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Unsupported database type from connection string: oracle+cx_oracle://user:pass@host:port/dbname"
        );
    }

    #[test]
    fn test_invalid_format() {
        let invalid_conn = "not a connection string";
        let err = to_diesel_conn_string(invalid_conn).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid SQLAlchemy connection string format: not a connection string"
        );
    }
}
