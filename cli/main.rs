use argparse::ArgumentParser;
use std::{io::Write, path::Path};
use zaku::{execute, Dataframe, ZakuError};

#[derive(Debug, PartialEq)]
pub enum Command {
    Quit,
    Execute(String),
}

pub fn get_input() -> Result<Command, ZakuError> {
    let mut input = String::new();
    print!("Zaku >>> ");
    std::io::stdout().flush()?;
    std::io::stdin().read_line(&mut input)?;
    input = input.trim().to_string();
    if input.as_str() == "quit" {
        return Ok(Command::Quit);
    }
    Ok(Command::Execute(input))
}

fn execute_sql(sql: &str, df: Dataframe) -> Result<String, ZakuError> {
    let res = execute(sql, df)?;
    Ok(res.pretty_print())
}

fn event_loop(df: Dataframe) {
    let mut prev_cmd = Command::Execute("".to_string());
    while prev_cmd != Command::Quit {
        match get_input().map(|cmd| match cmd {
            Command::Execute(sql) => match execute_sql(&sql, df.clone()) {
                Ok(res) => res,
                Err(e) => e.to_string(),
            },
            Command::Quit => {
                prev_cmd = Command::Quit;
                "Exiting Zaku...".to_string()
            }
        }) {
            Ok(msg) => println!("{}\n", msg),
            Err(e) => println!("{}\n", e),
        }
    }
}

fn main() {
    let mut path = Path::new("resources").join("test.csv");
    {
        let mut parser = ArgumentParser::new();
        parser.set_description("Zaku is a simple SQL query enginer on CSV files written in Rust");
        parser
            .refer(&mut path)
            .add_argument("path", argparse::Store, "Path to CSV file");
        parser.parse_args_or_exit();
    }

    match Dataframe::from_csv(
        path.to_str()
            .expect("File test.csv should exist in resources directory"),
    ) {
        Ok(df) => event_loop(df),
        Err(e) => println!("Failed to load CSV file: {}", e),
    }
}
