use argparse::ArgumentParser;
use zaku::{
    error::ZakuError,
    frontend::{
        prettifier::prettify,
        ui::{get_input, Command},
    },
    logical_plans::dataframe::Dataframe,
    sql::parser::parse,
};

fn execute_sql(
    sql: String,
    df: Dataframe,
    print_execution_plan: bool,
) -> Result<String, ZakuError> {
    let select_df = parse(sql.as_str(), df)?;
    let plan = select_df.logical_plan().to_physical_plan()?;
    let res = plan.execute();
    let prettystr = prettify(&res);
    if print_execution_plan {
        return Ok(format!("{}\n\n{}", prettystr, plan.to_string()));
    }
    Ok(prettystr)
}

fn event_loop(df: Dataframe, print_execution_plan: bool) {
    let mut prev_cmd = Command::Execute("".to_string());
    while prev_cmd != Command::Quit {
        match get_input().map(|cmd| match cmd {
            Command::Execute(sql) => match execute_sql(sql, df.clone(), print_execution_plan) {
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
