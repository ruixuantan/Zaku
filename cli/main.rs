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

async fn execute_sql(sql: &str, df: Dataframe) -> Result<String, ZakuError> {
    let res = execute(sql, df).await?;
    Ok(res.pretty_print())
}

async fn event_loop(df: Dataframe) {
    println!("Schema read as: {}", df.schema());
    loop {
        let input = get_input();
        match input {
            Ok(Command::Execute(sql)) => match execute_sql(&sql, df.clone()).await {
                Ok(res) => println!("{}\n", res),
                Err(e) => println!("{}\n", e),
            },
            Ok(Command::Quit) => {
                println!("Exiting Zaku...");
                std::process::exit(0);
            }
            Err(e) => println!("{}\n", e),
        }
    }
}

#[tokio::main]
async fn main() {
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
        Ok(df) => event_loop(df).await,
        Err(e) => println!("Failed to load CSV file: {}", e),
    }
}
