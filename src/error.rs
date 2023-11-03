use thiserror::Error;

#[derive(Error, Debug)]
pub enum ZakuError {
    #[error("CSV parsing error: {0}")]
    CsvError(#[from] csv::Error),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("SQL parsing error: {0}")]
    SqlParserError(#[from] sqlparser::parser::ParserError),
    #[error("Parse int error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("ZakuError: {0}")]
    InternalError(String),
}

impl ZakuError {
    pub fn new(msg: &str) -> ZakuError {
        ZakuError::InternalError(msg.to_string())
    }
}
