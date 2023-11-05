use argparse::ArgumentParser;
use std::io::Write;
use zaku::{execute, execute_with_plan, Dataframe, ZakuError};

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

fn execute_sql(sql: &str, df: Dataframe, print_execution_plan: bool) -> Result<String, ZakuError> {
    if print_execution_plan {
        let (res, plan_str) = execute_with_plan(sql, df)?;
        let prettystr = res.pretty_print();
        Ok(format!("{}\n\n{}", prettystr, plan_str))
    } else {
        let res = execute(sql, df)?;
        Ok(res.pretty_print())
    }
}

fn event_loop(df: Dataframe, print_execution_plan: bool) {
    let mut prev_cmd = Command::Execute("".to_string());
    while prev_cmd != Command::Quit {
        match get_input().map(|cmd| match cmd {
            Command::Execute(sql) => match execute_sql(&sql, df.clone(), print_execution_plan) {
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
    let mut execution_plan = false;
    let mut path = "resources/test.csv".to_string();
    {
        let mut parser = ArgumentParser::new();
        parser.set_description("Zaku is a simple SQL query enginer on CSV files written in Rust");
        parser.refer(&mut execution_plan).add_option(
            &["-e", "--execution-plan"],
            argparse::StoreTrue,
            "Prints the execution plan of the query",
        );
        parser
            .refer(&mut path)
            .add_argument("path", argparse::Store, "Path to CSV file");
        parser.parse_args_or_exit();
    }

    match Dataframe::from_csv(&path) {
        Ok(df) => event_loop(df, execution_plan),
        Err(e) => println!("Failed to load CSV file: {}", e),
    }
}
