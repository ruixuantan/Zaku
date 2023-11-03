use zaku::{
    datatypes::record_batch::RecordBatch, error::ZakuError, logical_plans::dataframe::Dataframe,
    logical_plans::logical_plan::LogicalPlan, physical_plans::physical_plan::PhysicalPlan,
    sql::parser::parse,
};

fn execute(sql: &str) -> Result<RecordBatch, ZakuError> {
    let path = "resources/test.csv".to_string();
    let df = Dataframe::from_csv(&path)?;
    let select_df = parse(sql, df)?;
    let plan = select_df.logical_plan();
    Ok(plan.to_physical_plan()?.execute())
}

#[test]
fn basic_query() {
    let sql = "SELECT * FROM test";
    assert!(execute(sql).is_ok());
}
