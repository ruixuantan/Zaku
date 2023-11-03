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
    io::stdout().flush()?;
    io::stdin().read_line(&mut input)?;
    input = input.trim().to_string();
    if input.as_str() == "quit" {
        return Ok(Command::Quit);
    }
    Ok(Command::Execute(input))
}
