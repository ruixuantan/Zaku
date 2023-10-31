use crate::{
    datatypes::{schema::Field, types::DataType},
    error::ZakuError,
    physical_plans::physical_expr::PhysicalExpr,
};

use super::{binary_expr::BinaryExpr, logical_plan::LogicalPlan};

#[derive(Debug, Clone)]
pub enum LogicalExpr {
    Column(Column),
    LiteralText(String),
    LiteralBoolean(bool),
    LiteralInteger(i32),
    LiteralFloat(f32),
    BinaryExpr(BinaryExpr),
}

impl LogicalExpr {
    pub fn to_field(&self, input: &LogicalPlan) -> Result<Field, ZakuError> {
        match self {
            LogicalExpr::Column(column) => column.column_to_field(input),
            LogicalExpr::LiteralText(value) => Ok(Field::new(value.clone(), DataType::Text)),
            LogicalExpr::LiteralBoolean(value) => {
                Ok(Field::new(value.to_string(), DataType::Boolean))
            }
            LogicalExpr::LiteralInteger(value) => {
                Ok(Field::new(value.to_string(), DataType::Integer))
            }
            LogicalExpr::LiteralFloat(value) => Ok(Field::new(value.to_string(), DataType::Float)),
            LogicalExpr::BinaryExpr(expr) => expr.to_field(input),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            LogicalExpr::Column(column) => column.name().clone(),
            LogicalExpr::LiteralText(value) => value.clone(),
            LogicalExpr::LiteralBoolean(value) => value.to_string(),
            LogicalExpr::LiteralInteger(value) => value.to_string(),
            LogicalExpr::LiteralFloat(value) => value.to_string(),
            LogicalExpr::BinaryExpr(expr) => expr.to_string(),
        }
    }

    pub fn to_physical_expr(&self, input: &LogicalPlan) -> Result<PhysicalExpr, ZakuError> {
        match self {
            LogicalExpr::Column(column) => column.column_to_physical_expr(input),
            LogicalExpr::LiteralText(value) => Ok(PhysicalExpr::LiteralTextExpr(value.clone())),
            LogicalExpr::LiteralBoolean(value) => Ok(PhysicalExpr::LiteralBooleanExpr(*value)),
            LogicalExpr::LiteralInteger(value) => Ok(PhysicalExpr::LiteralIntegerExpr(*value)),
            LogicalExpr::LiteralFloat(value) => Ok(PhysicalExpr::LiteralFloatExpr(*value)),
            LogicalExpr::BinaryExpr(expr) => expr.to_physical_expr(input),
        }
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

    fn column_to_field(&self, input: &LogicalPlan) -> Result<Field, ZakuError> {
        Ok(input.schema().get_field(&self.name)?.clone())
    }

    fn column_to_physical_expr(&self, input: &LogicalPlan) -> Result<PhysicalExpr, ZakuError> {
        let index = input.schema().get_index(&self.name)?;
        Ok(PhysicalExpr::ColumnExpr(index))
    }
}
