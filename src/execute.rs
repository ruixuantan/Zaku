use std::{sync::Arc, vec};

use futures_async_stream::for_await;

use crate::{
    datasources::datasink::Datasink,
    datatypes::{
        column_vector::{ColumnVector, Vectors},
        record_batch::RecordBatch,
        schema::{Field, Schema},
        types::{DataType, Value},
    },
    error::ZakuError,
    logical_plans::{dataframe::Dataframe, logical_plan::LogicalPlan},
    sql::{self, stmt::Stmt},
};

async fn execute_select(df: Dataframe) -> Result<Datasink, ZakuError> {
    let plan = df.logical_plan().to_physical_plan()?;
    let mut data = vec![];
    #[for_await]
    for rb in plan.execute() {
        data.push(rb?);
    }
    Ok(Datasink::new(data))
}

fn execute_explain(df: Dataframe) -> Result<Datasink, ZakuError> {
    let plan = df.logical_plan();
    let plan_str = format!("{}", plan);
    let col = vec![Arc::new(Vectors::ColumnVector(ColumnVector::new(
        DataType::Text,
        vec![Value::Text(plan_str)],
    )))];
    let schema = Schema::new(vec![Field::new("Query Plan".to_string(), DataType::Text)]);
    Ok(Datasink::new(vec![RecordBatch::new(schema, col)]))
}

async fn execute_copy(df: Dataframe, path: &String) -> Result<Datasink, ZakuError> {
    let plan = df.logical_plan().to_physical_plan()?;
    let mut data = vec![];
    #[for_await]
    for rb in plan.execute() {
        data.push(rb?);
    }
    let ds = Datasink::new(data);
    let _ = ds.to_csv(path);
    Ok(ds)
}

pub async fn execute(sql: &str, df: Dataframe) -> Result<Datasink, ZakuError> {
    let select_df = sql::parser::parse(sql, df)?;
    match select_df {
        Stmt::Select(df) => execute_select(df).await,
        Stmt::Explain(df) => execute_explain(df),
        Stmt::CopyTo(df, path) => execute_copy(df, &path).await,
    }
}
