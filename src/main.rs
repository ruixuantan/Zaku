use zaku::{
    error::ZakuError,
    frontend::{
        prettifier::prettify,
        ui::{get_input, Command},
    },
    logical_plans::dataframe::Dataframe,
    sql::parser::parse,
};

fn execute_sql(sql: String, df: Dataframe) -> Result<String, ZakuError> {
    let select_df = parse(sql.as_str(), df)?;
    let res = select_df.logical_plan().to_physical_plan()?.execute();
    Ok(prettify(&res))
}

fn main() {
    let df = Dataframe::from_csv("resources/test.csv").unwrap();
    let mut prev_cmd = Command::Execute("".to_string());
    while prev_cmd != Command::Quit {
        match get_input().map(|cmd| match cmd {
            Command::Execute(sql) => match execute_sql(sql, df.clone()) {
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
