use std::error::Error;
use std::fmt::Display;

#[derive(Debug)]
pub struct ZakuError {
    msg: String,
}

impl ZakuError {
    pub fn new(msg: String) -> ZakuError {
        ZakuError { msg }
    }
}

impl Display for ZakuError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ZakuError: {}", self.msg)
    }
}

impl Error for ZakuError {}

impl From<csv::Error> for ZakuError {
    fn from(e: csv::Error) -> Self {
        ZakuError::new(e.to_string())
    }
}

impl From<std::io::Error> for ZakuError {
    fn from(e: std::io::Error) -> Self {
        ZakuError::new(e.to_string())
    }
}

impl From<sqlparser::parser::ParserError> for ZakuError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        ZakuError::new(e.to_string())
    }
}
