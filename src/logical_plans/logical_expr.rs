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
    ColumnIndex(usize),
    LiteralText(String),
    LiteralBoolean(bool),
    LiteralInteger(i32),
    LiteralFloat(f32),
    BinaryExpr(BinaryExprs),
    AggregateExpr(AggregateExprs),
    AliasExpr(AliasExpr),
}

impl LogicalExprs {
    // extracts all nested aggregate functions
    pub fn as_aggregate(&self) -> Vec<AggregateExprs> {
        match self {
            LogicalExprs::AggregateExpr(expr) => vec![expr.clone()],
            LogicalExprs::AliasExpr(expr) => expr.expr.as_aggregate(),
            LogicalExprs::BinaryExpr(expr) => {
                let mut exprs = vec![];
                let mut l = expr.get_l().as_aggregate();
                exprs.append(&mut l);
                let mut r = expr.get_r().as_aggregate();
                exprs.append(&mut r);
                exprs
            }
            _ => vec![],
        }
    }
}

impl LogicalExpr for LogicalExprs {
    fn to_field(&self, input: &LogicalPlans) -> Result<Field, ZakuError> {
        match self {
            LogicalExprs::Column(column) => column.column_to_field(input),
            LogicalExprs::ColumnIndex(index) => {
                Ok(input.schema().get_field_by_index(index)?.clone())
            }
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
            LogicalExprs::ColumnIndex(index) => Ok(PhysicalExprs::Column(*index)),
            LogicalExprs::LiteralText(value) => Ok(PhysicalExprs::LiteralText(value.clone())),
            LogicalExprs::LiteralBoolean(value) => Ok(PhysicalExprs::LiteralBoolean(*value)),
            LogicalExprs::LiteralInteger(value) => Ok(PhysicalExprs::LiteralInteger(*value)),
            LogicalExprs::LiteralFloat(value) => Ok(PhysicalExprs::LiteralFloat(*value)),
            LogicalExprs::BinaryExpr(expr) => expr.to_physical_expr(input),
            LogicalExprs::AliasExpr(expr) => expr.to_physical_expr(input),
            LogicalExprs::AggregateExpr(expr) => expr.input().to_physical_expr(input),
        }
    }
}

impl Display for LogicalExprs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            LogicalExprs::Column(column) => {
                format!("#{}", column.name())
            }
            LogicalExprs::ColumnIndex(index) => format!("#{}", index),
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

    pub fn alias(&self) -> &String {
        &self.alias
    }

    pub fn expr(&self) -> &LogicalExprs {
        &self.expr
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
