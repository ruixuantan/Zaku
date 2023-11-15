use argparse::ArgumentParser;
use std::{io::Write, path::Path};
use zaku::{execute, Dataframe, ZakuError};

#[derive(Debug, PartialEq)]
pub enum Command {
    Quit,
    Execute(String),
    Schema,
}

pub fn get_input() -> Result<Command, ZakuError> {
    let mut input = String::new();
    print!("Zaku >>> ");
    std::io::stdout().flush()?;
    std::io::stdin().read_line(&mut input)?;
    input = input.trim().to_string();
    if input.as_str() == "quit" {
        return Ok(Command::Quit);
    } else if input.as_str() == "schema" {
        return Ok(Command::Schema);
    }
    Ok(Command::Execute(input))
}

async fn execute_sql(sql: &str, df: Dataframe) -> Result<String, ZakuError> {
    let res = execute(sql, df).await?;
    let mut row_count = 0;
    for (i, rb) in res.iter().enumerate() {
        if i == 0 {
            println!("{}", rb.print(true));
        } else {
            println!("{}", rb.print(false));
        }
        row_count += rb.row_count();
        if i == res.num_batches() - 1 {
            break;
        }
        print!("(Press (c) to print next rows)");
        std::io::stdout().flush()?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if input.trim() != "c" {
            break;
        }
    }
    Ok(format!("({} rows)", row_count))
}

async fn event_loop(df: Dataframe) {
    loop {
        let input = get_input();
        match input {
            Ok(Command::Execute(sql)) => match execute_sql(&sql, df.clone()).await {
                Ok(res) => println!("{}\n", res),
                Err(e) => println!("{}\n", e),
            },
            Ok(Command::Schema) => {
                println!("{}\n", df.schema().to_record_batch().print(true));
            }
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
