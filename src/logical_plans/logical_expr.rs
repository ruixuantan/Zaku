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
    Column(Column),
    LiteralText(String),
    LiteralBoolean(bool),
    LiteralInteger(i32),
    LiteralFloat(f32),
    BinaryExpr(BinaryExprs),
    AggregateExpr(AggregateExprs),
    AliasExpr(AliasExpr),
}

impl LogicalExprs {
    pub fn is_aggregate(&self) -> bool {
        matches!(self, LogicalExprs::AggregateExpr(_))
    }

    pub fn as_aggregate(&self) -> AggregateExprs {
        match self {
            LogicalExprs::AggregateExpr(expr) => expr.clone(),
            _ => panic!("Only AggregateExprs can be converted"),
        }
    }
}

impl LogicalExpr for LogicalExprs {
    fn to_field(&self, input: &LogicalPlans) -> Result<Field, ZakuError> {
        match self {
            LogicalExprs::Column(column) => column.column_to_field(input),
            LogicalExprs::LiteralText(value) => Ok(Field::new(value.clone(), DataType::Text)),
            LogicalExprs::LiteralBoolean(value) => {
                Ok(Field::new(value.to_string(), DataType::Boolean))
            }
            LogicalExprs::LiteralInteger(value) => {
                Ok(Field::new(value.to_string(), DataType::Integer))
            }
            LogicalExprs::LiteralFloat(value) => Ok(Field::new(value.to_string(), DataType::Float)),
            LogicalExprs::BinaryExpr(expr) => expr.to_field(input),
            LogicalExprs::AggregateExpr(expr) => expr.to_field(input),
            LogicalExprs::AliasExpr(expr) => expr.to_field(input),
        }
    }

    fn to_physical_expr(&self, input: &LogicalPlans) -> Result<PhysicalExprs, ZakuError> {
        match self {
            LogicalExprs::Column(column) => column.column_to_physical_expr(input),
            LogicalExprs::LiteralText(value) => Ok(PhysicalExprs::LiteralText(value.clone())),
            LogicalExprs::LiteralBoolean(value) => Ok(PhysicalExprs::LiteralBoolean(*value)),
            LogicalExprs::LiteralInteger(value) => Ok(PhysicalExprs::LiteralInteger(*value)),
            LogicalExprs::LiteralFloat(value) => Ok(PhysicalExprs::LiteralFloat(*value)),
            LogicalExprs::BinaryExpr(expr) => expr.to_physical_expr(input),
            LogicalExprs::AliasExpr(expr) => expr.to_physical_expr(input),
            LogicalExprs::AggregateExpr(_) => {
                panic!("AggregateExprs should not be converted to PhysicalExprs")
            }
        }
    }
}

impl Display for LogicalExprs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            LogicalExprs::Column(column) => {
                format!("#{}", column.name())
            }
            LogicalExprs::LiteralText(value) => value.clone(),
            LogicalExprs::LiteralBoolean(value) => value.to_string(),
            LogicalExprs::LiteralInteger(value) => value.to_string(),
            LogicalExprs::LiteralFloat(value) => value.to_string(),
            LogicalExprs::BinaryExpr(expr) => expr.to_string(),
            LogicalExprs::AggregateExpr(expr) => expr.to_string(),
            LogicalExprs::AliasExpr(expr) => expr.to_string(),
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

    fn column_to_field(&self, input: &LogicalPlans) -> Result<Field, ZakuError> {
        Ok(input.schema().get_field(&self.name)?.clone())
    }

    fn column_to_physical_expr(&self, input: &LogicalPlans) -> Result<PhysicalExprs, ZakuError> {
        let index = input.schema().get_index(&self.name)?;
        Ok(PhysicalExprs::Column(index))
    }
}

#[derive(Debug, Clone)]
pub struct AliasExpr {
    expr: Box<LogicalExprs>,
    alias: String,
}

impl AliasExpr {
    pub fn new(expr: LogicalExprs, alias: String) -> AliasExpr {
        AliasExpr {
            expr: Box::new(expr),
            alias,
        }
    }
}

impl LogicalExpr for AliasExpr {
    fn to_field(&self, input: &LogicalPlans) -> Result<Field, ZakuError> {
        Ok(Field::new(
            self.alias.clone(),
            *self.expr.to_field(input)?.datatype(),
        ))
    }

    fn to_physical_expr(&self, input: &LogicalPlans) -> Result<PhysicalExprs, ZakuError> {
        self.expr.to_physical_expr(input)
    }
}

impl Display for AliasExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} as {}", self.expr, self.alias)
    }
}
