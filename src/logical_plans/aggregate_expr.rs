use std::fmt::Display;

use super::{
    logical_expr::{LogicalExpr, LogicalExprs},
    logical_plan::LogicalPlans,
};
use crate::{datatypes::schema::Field, ZakuError};
use crate::{datatypes::types::DataType, physical_plans::accumulator::AggregateExpressions};

#[derive(Debug, Clone)]
pub enum AggregateExprs {
    Count(Box<LogicalExprs>),
    Sum(Box<LogicalExprs>),
    Avg(Box<LogicalExprs>),
    Min(Box<LogicalExprs>),
    Max(Box<LogicalExprs>),
}

impl AggregateExprs {
    pub fn to_field(&self, plan: &LogicalPlans) -> Result<Field, ZakuError> {
        match self {
            AggregateExprs::Count(_) => Ok(Field::new("count".to_string(), DataType::Integer)),
            AggregateExprs::Sum(expr) => expr.to_field(plan),
            AggregateExprs::Avg(expr) => expr.to_field(plan),
            AggregateExprs::Min(expr) => expr.to_field(plan),
            AggregateExprs::Max(expr) => expr.to_field(plan),
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
