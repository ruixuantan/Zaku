use std::io::{self, Write};

use crate::error::ZakuError;

#[derive(Debug, PartialEq)]
pub enum Command {
    Quit,
    Execute(String),
}

pub fn get_input() -> Result<Command, ZakuError> {
    let mut input = String::new();
    print!("Zaku >>> ");
    io::stdout()
        .flush()
        .map_err(|e| ZakuError::new(e.to_string()))?;
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| ZakuError::new(e.to_string()))?;
    input = input.trim().to_string();
    if input.as_str() == "quit" {
        return Ok(Command::Quit);
    }
    Ok(Command::Execute(input))
}
