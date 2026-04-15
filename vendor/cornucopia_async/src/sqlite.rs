use deadpool_postgres::Object;
use deadpool_postgres::Transaction as PGTransaction;
use rusqlite::Params;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};
use tokio_postgres::error::SqlState;

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum DbError {
    DuplicateViolation,
    ForeignKeyViolation,
    Other(String),
}

impl From<rusqlite::Error> for DbError {
    fn from(err: rusqlite::Error) -> Self {
        if let Some(sqlite) = err.sqlite_error() {
            match (sqlite.code, sqlite.extended_code) {
                (rusqlite::ErrorCode::ConstraintViolation, 2067 /* UNIQUE */) => {
                    return DbError::DuplicateViolation;
                }
                (rusqlite::ErrorCode::ConstraintViolation, 787 /* FOREIGN KEY */) => {
                    return DbError::ForeignKeyViolation;
                }
                _ => {}
            }
        }

        DbError::Other(err.to_string())
    }
}

impl From<tokio_postgres::Error> for DbError {
    fn from(err: tokio_postgres::Error) -> Self {
        if let Some(db) = &err.as_db_error() {
            if *db.code() == SqlState::UNIQUE_VIOLATION {
                return DbError::DuplicateViolation;
            } else if *db.code() == SqlState::FOREIGN_KEY_VIOLATION {
                return DbError::ForeignKeyViolation;
            }
        }

        DbError::Other(err.to_string())
    }
}

impl Display for DbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for DbError {}

#[derive(Debug, Clone)]
pub enum DatabaseSource {
    Postgres(deadpool_postgres::Pool),
    Sqlite(Arc<Mutex<rusqlite::Connection>>),
}

impl DatabaseSource {
    pub async fn client(&self) -> Result<Database, DbError> {
        Ok(match self {
            DatabaseSource::Postgres(p) => {
                Database::Postgres(p.get().await.map_err(|e| DbError::Other(e.to_string()))?)
            }
            DatabaseSource::Sqlite(p) => Database::Sqlite(SqliteWrapper::Connection(p.clone())),
        })
    }
}

pub enum SqliteWrapper {
    Connection(Arc<Mutex<rusqlite::Connection>>),
}

impl SqliteWrapper {
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        match self {
            SqliteWrapper::Connection(c) => {
                let c = c.lock().unwrap();
                c.execute(sql, params)
            }
        }
    }

    pub fn query_rows<P: Params, T, F: Fn(&rusqlite::Row) -> T>(
        &self,
        sql: &str,
        params: P,
        map: F,
    ) -> rusqlite::Result<Vec<T>> {
        match self {
            SqliteWrapper::Connection(c) => {
                let c = c.lock().unwrap();
                let mut statement = c.prepare(sql).unwrap();
                let results = statement.query(params)?;
                results.mapped(|r| Ok(map(r)))
                    .map(|r| r).collect()
            }
        }
    }
}

pub enum Database<'a> {
    Postgres(Object),
    PostgresTx(PGTransaction<'a>),
    Sqlite(SqliteWrapper),
}
