use logical_plans::logical_plan::LogicalPlan;
use physical_plans::physical_plan::PhysicalPlan;

mod datasources;
mod datatypes;
mod error;
mod logical_plans;
mod physical_plans;
mod sql;

pub use datasources::datasink::Datasink;
pub use datatypes::record_batch::RecordBatch;
pub use error::ZakuError;
pub use logical_plans::dataframe::Dataframe;
pub use logical_plans::logical_plan::LogicalPlans;

pub fn execute(sql: &str, df: Dataframe) -> Result<Datasink, ZakuError> {
    let select_df = sql::parser::parse(sql, df)?;
    let mut res: Vec<RecordBatch> = vec![];
    select_df
        .logical_plan()
        .to_physical_plan()?
        .execute()
        .for_each(|rb| res.push(rb.clone()));
    Ok(Datasink::from_record_batches(res))
}

pub fn execute_with_plan(sql: &str, df: Dataframe) -> Result<(Datasink, String), ZakuError> {
    let select_df = sql::parser::parse(sql, df)?;
    let plan = select_df.logical_plan();
    let plan_str = format!("{}", plan);
    let mut res: Vec<RecordBatch> = vec![];
    select_df
        .logical_plan()
        .to_physical_plan()?
        .execute()
        .for_each(|rb| res.push(rb.clone()));
    Ok((Datasink::from_record_batches(res), plan_str))
}
