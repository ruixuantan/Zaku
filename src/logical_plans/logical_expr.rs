use crate::{
    datatypes::{schema::Field, types::DataType},
    error::ZakuError,
    physical_plans::physical_expr::PhysicalExpr,
};

use super::logical_plan::LogicalPlan;

#[derive(Debug, Clone)]
pub enum LogicalExpr {
    Column(String),
    LiteralText(String),
    LiteralBoolean(bool),
    LiteralInteger(i32),
    LiteralFloat(f32),
}

impl LogicalExpr {
    pub fn to_field(&self, input: &LogicalPlan) -> Result<Field, ZakuError> {
        match self {
            LogicalExpr::Column(name) => column_to_field(input, name),
            LogicalExpr::LiteralText(value) => Ok(Field::new(value.clone(), DataType::Text)),
            LogicalExpr::LiteralBoolean(value) => {
                Ok(Field::new(value.to_string(), DataType::Boolean))
            }
            LogicalExpr::LiteralInteger(value) => {
                Ok(Field::new(value.to_string(), DataType::Integer))
            }
            LogicalExpr::LiteralFloat(value) => Ok(Field::new(value.to_string(), DataType::Float)),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            LogicalExpr::Column(name) => name.clone(),
            LogicalExpr::LiteralText(value) => value.clone(),
            LogicalExpr::LiteralBoolean(value) => value.to_string(),
            LogicalExpr::LiteralInteger(value) => value.to_string(),
            LogicalExpr::LiteralFloat(value) => value.to_string(),
        }
    }

    pub fn to_physical_expr(&self, input: &LogicalPlan) -> Result<PhysicalExpr, ZakuError> {
        match self {
            LogicalExpr::Column(name) => column_to_physical_expr(input, name),
            LogicalExpr::LiteralText(value) => Ok(PhysicalExpr::LiteralTextExpr(value.clone())),
            LogicalExpr::LiteralBoolean(value) => Ok(PhysicalExpr::LiteralBooleanExpr(*value)),
            LogicalExpr::LiteralInteger(value) => Ok(PhysicalExpr::LiteralIntegerExpr(*value)),
            LogicalExpr::LiteralFloat(value) => Ok(PhysicalExpr::LiteralFloatExpr(*value)),
        }
    }
}

fn column_to_field(input: &LogicalPlan, name: &String) -> Result<Field, ZakuError> {
    Ok(input.schema().get_field(name)?.clone())
}

fn column_to_physical_expr(input: &LogicalPlan, name: &String) -> Result<PhysicalExpr, ZakuError> {
    let index = input.schema().get_index(name)?;
    Ok(PhysicalExpr::ColumnExpr(index))
}
