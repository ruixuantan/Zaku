use std::fmt::Display;

use super::{
    logical_expr::{LogicalExpr, LogicalExprs},
    logical_plan::LogicalPlans,
};
use crate::{datatypes::schema::Field, ZakuError};
use crate::{datatypes::types::DataType, physical_plans::accumulator::AggregateExpressions};

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateExprs {
    Count(Box<LogicalExprs>),
    Sum(Box<LogicalExprs>),
    Avg(Box<LogicalExprs>),
    Min(Box<LogicalExprs>),
    Max(Box<LogicalExprs>),
}

impl AggregateExprs {
    pub fn from_str(func: &str, func_arg: LogicalExprs) -> Result<AggregateExprs, ZakuError> {
        match func.to_lowercase().as_str() {
            "count" => Ok(AggregateExprs::Count(Box::new(func_arg))),
            "sum" => Ok(AggregateExprs::Sum(Box::new(func_arg))),
            "avg" => Ok(AggregateExprs::Avg(Box::new(func_arg))),
            "min" => Ok(AggregateExprs::Min(Box::new(func_arg))),
            "max" => Ok(AggregateExprs::Max(Box::new(func_arg))),
            _ => Err(ZakuError::new("Unknown aggregate function")),
        }
    }

    pub fn to_field(&self, input: &LogicalPlans) -> Result<Field, ZakuError> {
        match self {
            AggregateExprs::Count(_) => Ok(Field::new("count".to_string(), DataType::Number)),
            AggregateExprs::Sum(_) => Ok(Field::new("sum".to_string(), DataType::Number)),
            AggregateExprs::Avg(_) => Ok(Field::new("avg".to_string(), DataType::Number)),
            AggregateExprs::Min(expr) => Ok(Field::new(
                "min".to_string(),
                *expr.to_field(input)?.datatype(),
            )),
            AggregateExprs::Max(expr) => Ok(Field::new(
                "max".to_string(),
                *expr.to_field(input)?.datatype(),
            )),
        }
    }

    pub fn input(&self) -> &LogicalExprs {
        match self {
            AggregateExprs::Count(expr) => expr,
            AggregateExprs::Sum(expr) => expr,
            AggregateExprs::Avg(expr) => expr,
            AggregateExprs::Min(expr) => expr,
            AggregateExprs::Max(expr) => expr,
        }
    }

    pub fn to_physical_aggregate(
        &self,
        plan: &LogicalPlans,
    ) -> Result<AggregateExpressions, ZakuError> {
        match self {
            AggregateExprs::Count(expr) => {
                Ok(AggregateExpressions::Count(expr.to_physical_expr(plan)?))
            }
            AggregateExprs::Sum(expr) => {
                Ok(AggregateExpressions::Sum(expr.to_physical_expr(plan)?))
            }
            AggregateExprs::Avg(expr) => {
                Ok(AggregateExpressions::Avg(expr.to_physical_expr(plan)?))
            }
            AggregateExprs::Min(expr) => {
                Ok(AggregateExpressions::Min(expr.to_physical_expr(plan)?))
            }
            AggregateExprs::Max(expr) => {
                Ok(AggregateExpressions::Max(expr.to_physical_expr(plan)?))
            }
        }
    }
}

impl Display for AggregateExprs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = match self {
            AggregateExprs::Count(expr) => write!(f, "COUNT({})", expr),
            AggregateExprs::Sum(expr) => write!(f, "SUM({})", expr),
            AggregateExprs::Avg(expr) => write!(f, "AVG({})", expr),
            AggregateExprs::Min(expr) => write!(f, "MIN({})", expr),
            AggregateExprs::Max(expr) => write!(f, "MAX({})", expr),
        };
        Ok(())
    }
}
