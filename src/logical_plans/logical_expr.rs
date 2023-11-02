use std::fmt::Display;

use crate::{
    datatypes::{schema::Field, types::DataType},
    error::ZakuError,
    physical_plans::physical_expr::PhysicalExpr,
};

use super::{
    binary_expr::BinaryExpr,
    binary_expr::BinaryExprTrait,
    logical_plan::{LogicalPlan, LogicalPlanTrait},
};

#[derive(Debug, Clone)]
pub enum LogicalExpr {
    Column(Column, Option<String>),
    LiteralText(String, Option<String>),
    LiteralBoolean(bool, Option<String>),
    LiteralInteger(i32, Option<String>),
    LiteralFloat(f32, Option<String>),
    BinaryExpr(BinaryExpr, Option<String>),
}

impl LogicalExpr {
    pub fn to_field(&self, input: &LogicalPlan) -> Result<Field, ZakuError> {
        match self {
            LogicalExpr::Column(column, alias) => column.column_to_field(input, alias),
            LogicalExpr::LiteralText(value, alias) => Ok(Field::with_alias(
                value.clone(),
                alias.clone(),
                DataType::Text,
            )),
            LogicalExpr::LiteralBoolean(value, alias) => Ok(Field::with_alias(
                value.to_string(),
                alias.clone(),
                DataType::Boolean,
            )),
            LogicalExpr::LiteralInteger(value, alias) => Ok(Field::with_alias(
                value.to_string(),
                alias.clone(),
                DataType::Integer,
            )),
            LogicalExpr::LiteralFloat(value, alias) => Ok(Field::with_alias(
                value.to_string(),
                alias.clone(),
                DataType::Float,
            )),
            LogicalExpr::BinaryExpr(expr, alias) => {
                let mut f = expr.to_field(input)?;
                f.set_alias(alias);
                Ok(f)
            }
        }
    }

    fn fmt(root: String, alias: &Option<String>) -> String {
        match alias {
            Some(alias) => format!("{} AS {}", root, alias),
            None => root,
        }
    }

    pub fn to_physical_expr(&self, input: &LogicalPlan) -> Result<PhysicalExpr, ZakuError> {
        match self {
            LogicalExpr::Column(column, _) => column.column_to_physical_expr(input),
            LogicalExpr::LiteralText(value, _) => Ok(PhysicalExpr::LiteralTextExpr(value.clone())),
            LogicalExpr::LiteralBoolean(value, _) => Ok(PhysicalExpr::LiteralBooleanExpr(*value)),
            LogicalExpr::LiteralInteger(value, _) => Ok(PhysicalExpr::LiteralIntegerExpr(*value)),
            LogicalExpr::LiteralFloat(value, _) => Ok(PhysicalExpr::LiteralFloatExpr(*value)),
            LogicalExpr::BinaryExpr(expr, _) => expr.to_physical_expr(input),
        }
    }

    pub fn set_alias(&self, alias: String) -> Self {
        match self {
            LogicalExpr::Column(column, _) => LogicalExpr::Column(column.clone(), Some(alias)),
            LogicalExpr::LiteralText(value, _) => {
                LogicalExpr::LiteralText(value.clone(), Some(alias))
            }
            LogicalExpr::LiteralBoolean(value, _) => {
                LogicalExpr::LiteralBoolean(*value, Some(alias))
            }
            LogicalExpr::LiteralInteger(value, _) => {
                LogicalExpr::LiteralInteger(*value, Some(alias))
            }
            LogicalExpr::LiteralFloat(value, _) => LogicalExpr::LiteralFloat(*value, Some(alias)),
            LogicalExpr::BinaryExpr(expr, _) => LogicalExpr::BinaryExpr(expr.clone(), Some(alias)),
        }
    }
}

impl Display for LogicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            LogicalExpr::Column(column, alias) => {
                format!("#{}", LogicalExpr::fmt(column.name().clone(), alias))
            }
            LogicalExpr::LiteralText(value, alias) => LogicalExpr::fmt(value.clone(), alias),
            LogicalExpr::LiteralBoolean(value, alias) => LogicalExpr::fmt(value.to_string(), alias),
            LogicalExpr::LiteralInteger(value, alias) => LogicalExpr::fmt(value.to_string(), alias),
            LogicalExpr::LiteralFloat(value, alias) => LogicalExpr::fmt(value.to_string(), alias),
            LogicalExpr::BinaryExpr(expr, alias) => LogicalExpr::fmt(expr.to_string(), alias),
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
        input: &LogicalPlan,
        alias: &Option<String>,
    ) -> Result<Field, ZakuError> {
        let schema = input.schema();
        let mut f = schema.get_field(&self.name)?.clone();
        f.set_alias(alias);
        Ok(f)
    }

    fn column_to_physical_expr(&self, input: &LogicalPlan) -> Result<PhysicalExpr, ZakuError> {
        let index = input.schema().get_index(&self.name)?;
        Ok(PhysicalExpr::ColumnExpr(index))
    }
}
