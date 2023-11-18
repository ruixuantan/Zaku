#![feature(stmt_expr_attributes)]
#![feature(proc_macro_hygiene)]
#![feature(coroutines)]

use argparse::ArgumentParser;
use crossterm::event::{read, Event, KeyCode, KeyEvent, KeyModifiers};
use futures_async_stream::for_await;
use rustyline::{error::ReadlineError, DefaultEditor};
use std::path::Path;
use zaku::{execute, Dataframe, ZakuError};

async fn execute_sql(sql: &str, df: Dataframe) -> Result<String, ZakuError> {
    let mut row_count = 0;
    let res = execute(sql, df.clone()).await?;
    let mut is_first_batch = true;
    #[for_await]
    for rb in res.iter() {
        if !is_first_batch {
            println!("(Press (ENTER) to print next rows, any other key to stop)");
            match read().unwrap() {
                Event::Key(KeyEvent {
                    code: KeyCode::Enter,
                    modifiers: KeyModifiers::NONE,
                    kind: _,
                    state: _,
                }) => (),
                _ => break,
            }
        }

        let rb = rb?;
        if is_first_batch {
            println!("{}", rb.print(true));
            is_first_batch = false;
        } else {
            println!("{}", rb.print(false));
        }
        row_count += rb.row_count();
    }
    Ok(format!("({} rows)", row_count))
}

async fn event_loop(df: Dataframe) {
    let mut rl = match DefaultEditor::new() {
        Ok(e) => e,
        Err(err) => {
            println!("Failed to initialize Zaku cli: {}", err);
            return;
        }
    };

    loop {
        let readline = rl.readline("Zaku >>> ");
        match readline {
            Ok(line) => {
                match rl.add_history_entry(line.as_str()) {
                    Ok(_) => (),
                    Err(err) => {
                        println!("Failed to add history entry: {}", err);
                    }
                }
                match line.as_str() {
                    "quit" => {
                        println!("Exiting Zaku...");
                        break;
                    }
                    "schema" => println!("{}\n", df.schema().to_record_batch().print(true)),
                    _ => match execute_sql(&line, df.clone()).await {
                        Ok(res) => println!("{}\n", res),
                        Err(e) => println!("{}\n", e),
                    },
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Exiting Zaku...");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("Exiting Zaku...");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let mut path = Path::new("resources").join("test.csv");
    let mut delimiter = ',';
    {
        let mut parser = ArgumentParser::new();
        parser.set_description("Zaku is a simple SQL query enginer on CSV files written in Rust");
        parser
            .refer(&mut path)
            .add_argument("path", argparse::Store, "Path to CSV file");
        parser.refer(&mut delimiter).add_option(
            &["-d", "--delimiter"],
            argparse::Store,
            "Delimiter used in the CSV file. Defaults to ','",
        );
        parser.parse_args_or_exit();
    }

    match Dataframe::from_csv(
        path.to_str()
            .expect("File test.csv should exist in resources directory"),
        Some(delimiter as u8),
    ) {
        Ok(df) => event_loop(df).await,
        Err(e) => println!("Failed to load CSV file: {}", e),
    }
    std::process::exit(0);
}
