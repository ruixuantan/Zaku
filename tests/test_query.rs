use zaku::{execute, Dataframe, Datasink, ZakuError};

fn run(sql: &str) -> Result<Datasink, ZakuError> {
    let path = "resources/test.csv".to_string();
    let df = Dataframe::from_csv(&path)?;
    let res = execute(sql, df)?;
    Ok(res)
}

#[test]
fn basic_query() {
    let sql = "SELECT * FROM test";
    assert!(run(sql).is_ok());
}
