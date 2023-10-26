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
