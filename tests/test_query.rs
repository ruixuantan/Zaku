use std::path::Path;

use zaku::{execute, Dataframe, Datasink, ZakuError};

fn run(sql: &str) -> Result<Datasink, ZakuError> {
    let binding = Path::new("resources").join("test.csv");
    let path = binding.to_str().expect("test.csv file should exist");
    let df = Dataframe::from_csv(path)?;
    let res = execute(sql, df)?;
    Ok(res)
}

#[test]
fn basic_query() {
    let sql = "SELECT * FROM test";
    assert!(run(sql).is_ok());
}

#[test]
fn explain_query() {
    let sql = "EXPLAIN SELECT * FROM test";
    assert!(run(sql).is_ok());
}
