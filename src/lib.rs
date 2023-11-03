use logical_plans::logical_plan::LogicalPlan;
use physical_plans::physical_plan::PhysicalPlan;

mod datasources;
mod datatypes;
mod error;
mod logical_plans;
mod physical_plans;
mod sql;

pub use datatypes::record_batch::RecordBatch;
pub use error::ZakuError;
pub use logical_plans::dataframe::Dataframe;
pub use logical_plans::logical_plan::LogicalPlans;

pub fn execute(sql: &str, df: Dataframe) -> Result<RecordBatch, ZakuError> {
    let select_df = sql::parser::parse(sql, df)?;
    Ok(select_df.logical_plan().to_physical_plan()?.execute())
}

pub fn execute_with_plan(sql: &str, df: Dataframe) -> Result<(RecordBatch, String), ZakuError> {
    let select_df = sql::parser::parse(sql, df)?;
    let plan = select_df.logical_plan();
    let plan_str = format!("{}", plan);
    Ok((plan.to_physical_plan()?.execute(), plan_str))
}
