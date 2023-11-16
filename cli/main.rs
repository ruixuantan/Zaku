use argparse::ArgumentParser;
use crossterm::event::{read, Event, KeyCode, KeyEvent, KeyModifiers};
use rustyline::{error::ReadlineError, DefaultEditor};
use std::path::Path;
use zaku::{execute, Dataframe, ZakuError};

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
    std::process::exit(0);
}
