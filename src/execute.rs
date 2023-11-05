use std::{sync::Arc, vec};

use crate::{
    datasources::datasink::Datasink,
    datatypes::{
        column_vector::{LiteralVector, Vectors},
        schema::{Field, Schema},
        types::{DataType, Value},
    },
    error::ZakuError,
    logical_plans::{dataframe::Dataframe, logical_plan::LogicalPlan},
    physical_plans::physical_plan::PhysicalPlan,
    sql::{self, stmt::Stmt},
};

fn execute_select(df: Dataframe) -> Result<Datasink, ZakuError> {
    let res = df.logical_plan().to_physical_plan()?.execute().collect();
    Ok(Datasink::from_record_batches(res))
}

fn execute_explain(df: Dataframe) -> Result<Datasink, ZakuError> {
    let plan = df.logical_plan();
    let plan_str = format!("{}", plan);
    let col = vec![Arc::new(Vectors::LiteralVector(LiteralVector::new(
        DataType::Text,
        Value::Text(plan_str),
        1,
    )))];
    let schema = Schema::new(vec![Field::new("Query Plan".to_string(), DataType::Text)]);
    Ok(Datasink::new(schema, col))
}

pub fn execute(sql: &str, df: Dataframe) -> Result<Datasink, ZakuError> {
    let select_df = sql::parser::parse(sql, df)?;
    match select_df {
        Stmt::Select(df) => execute_select(df),
        Stmt::Explain(df) => execute_explain(df),
    }
}
