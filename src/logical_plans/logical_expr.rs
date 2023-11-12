use std::fmt::Display;

use crate::{
    datatypes::{schema::Field, types::DataType},
    error::ZakuError,
    physical_plans::physical_expr::PhysicalExprs,
};

use super::{
    aggregate_expr::AggregateExprs,
    binary_expr::BinaryExpr,
    binary_expr::BinaryExprs,
    logical_plan::{LogicalPlan, LogicalPlans},
};

pub trait LogicalExpr {
    fn to_field(&self, input: &LogicalPlans) -> Result<Field, ZakuError>;

    fn to_physical_expr(&self, input: &LogicalPlans) -> Result<PhysicalExprs, ZakuError>;
}

#[derive(Debug, Clone)]
pub enum LogicalExprs {
    Column(Column, Option<String>),
    LiteralText(String, Option<String>),
    LiteralBoolean(bool, Option<String>),
    LiteralInteger(i32, Option<String>),
    LiteralFloat(f32, Option<String>),
    BinaryExpr(BinaryExprs, Option<String>),
    AggregateExpr(AggregateExprs, Option<String>),
}

impl LogicalExprs {
    fn fmt(root: String, alias: &Option<String>) -> String {
        match alias {
            Some(alias) => format!("{} AS {}", root, alias),
            None => root,
        }
    }

    pub fn set_alias(&self, alias: String) -> Self {
        match self {
            LogicalExprs::Column(column, _) => LogicalExprs::Column(column.clone(), Some(alias)),
            LogicalExprs::LiteralText(value, _) => {
                LogicalExprs::LiteralText(value.clone(), Some(alias))
            }
            LogicalExprs::LiteralBoolean(value, _) => {
                LogicalExprs::LiteralBoolean(*value, Some(alias))
            }
            LogicalExprs::LiteralInteger(value, _) => {
                LogicalExprs::LiteralInteger(*value, Some(alias))
            }
            LogicalExprs::LiteralFloat(value, _) => LogicalExprs::LiteralFloat(*value, Some(alias)),
            LogicalExprs::BinaryExpr(expr, _) => {
                LogicalExprs::BinaryExpr(expr.clone(), Some(alias))
            }
            LogicalExprs::AggregateExpr(expr, _) => {
                LogicalExprs::AggregateExpr(expr.clone(), Some(alias))
            }
        }
    }

    pub fn is_aggregate(&self) -> bool {
        matches!(self, LogicalExprs::AggregateExpr(_, _))
    }

    pub fn as_aggregate(&self) -> AggregateExprs {
        match self {
            LogicalExprs::AggregateExpr(expr, _) => expr.clone(),
            _ => panic!("Only AggregateExprs can be converted"),
        }
    }
}

impl LogicalExpr for LogicalExprs {
    fn to_field(&self, input: &LogicalPlans) -> Result<Field, ZakuError> {
        match self {
            LogicalExprs::Column(column, alias) => column.column_to_field(input, alias),
            LogicalExprs::LiteralText(value, alias) => Ok(Field::with_alias(
                value.clone(),
                alias.clone(),
                DataType::Text,
            )),
            LogicalExprs::LiteralBoolean(value, alias) => Ok(Field::with_alias(
                value.to_string(),
                alias.clone(),
                DataType::Boolean,
            )),
            LogicalExprs::LiteralInteger(value, alias) => Ok(Field::with_alias(
                value.to_string(),
                alias.clone(),
                DataType::Integer,
            )),
            LogicalExprs::LiteralFloat(value, alias) => Ok(Field::with_alias(
                value.to_string(),
                alias.clone(),
                DataType::Float,
            )),
            LogicalExprs::BinaryExpr(expr, alias) => {
                let mut f = expr.to_field(input)?;
                f.set_alias(alias);
                Ok(f)
            }
            LogicalExprs::AggregateExpr(expr, alias) => {
                let mut f = expr.to_field(input)?;
                f.set_alias(alias);
                Ok(f)
            }
        }
    }

    fn to_physical_expr(&self, input: &LogicalPlans) -> Result<PhysicalExprs, ZakuError> {
        match self {
            LogicalExprs::Column(column, _) => column.column_to_physical_expr(input),
            LogicalExprs::LiteralText(value, _) => Ok(PhysicalExprs::LiteralText(value.clone())),
            LogicalExprs::LiteralBoolean(value, _) => Ok(PhysicalExprs::LiteralBoolean(*value)),
            LogicalExprs::LiteralInteger(value, _) => Ok(PhysicalExprs::LiteralInteger(*value)),
            LogicalExprs::LiteralFloat(value, _) => Ok(PhysicalExprs::LiteralFloat(*value)),
            LogicalExprs::BinaryExpr(expr, _) => expr.to_physical_expr(input),
            LogicalExprs::AggregateExpr(_, _) => {
                panic!("AggregateExprs should not be converted to PhysicalExprs")
            }
        }
    }
}

impl Display for LogicalExprs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            LogicalExprs::Column(column, alias) => {
                format!("#{}", LogicalExprs::fmt(column.name().clone(), alias))
            }
            LogicalExprs::LiteralText(value, alias) => LogicalExprs::fmt(value.clone(), alias),
            LogicalExprs::LiteralBoolean(value, alias) => {
                LogicalExprs::fmt(value.to_string(), alias)
            }
            LogicalExprs::LiteralInteger(value, alias) => {
                LogicalExprs::fmt(value.to_string(), alias)
            }
            LogicalExprs::LiteralFloat(value, alias) => LogicalExprs::fmt(value.to_string(), alias),
            LogicalExprs::BinaryExpr(expr, alias) => LogicalExprs::fmt(expr.to_string(), alias),
            LogicalExprs::AggregateExpr(expr, alias) => LogicalExprs::fmt(expr.to_string(), alias),
        };
        write!(f, "{}", string)
    }
}
#[derive(Debug, Clone)]
pub struct Column {
    name: String,
}

impl Column {
    pub fn new(name: String) -> Column {
        Column { name }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    fn column_to_field(
        &self,
        input: &LogicalPlans,
        alias: &Option<String>,
    ) -> Result<Field, ZakuError> {
        let schema = input.schema();
        let mut f = schema.get_field(&self.name)?.clone();
        f.set_alias(alias);
        Ok(f)
    }

    fn column_to_physical_expr(&self, input: &LogicalPlans) -> Result<PhysicalExprs, ZakuError> {
        let index = input.schema().get_index(&self.name)?;
        Ok(PhysicalExprs::Column(index))
    }
}
