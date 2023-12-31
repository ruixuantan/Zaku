use std::{fmt::Display, sync::Arc};

use bigdecimal::BigDecimal;
use chrono::NaiveDate;

use crate::datatypes::{
    column_vector::{LiteralVector, Vectors},
    record_batch::RecordBatch,
    types::{DataType, Value},
};

use super::binary_expr::{BooleanExpr, MathExpr};

pub trait PhysicalExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Arc<Vectors>;
}

#[derive(Clone)]
pub enum PhysicalExprs {
    Column(usize),
    LiteralText(String),
    LiteralBoolean(bool),
    LiteralNumber(BigDecimal),
    LiteralDate(NaiveDate),
    BooleanExpr(BooleanExpr),
    MathExpr(MathExpr),
}

impl PhysicalExpr for PhysicalExprs {
    fn evaluate(&self, batch: &RecordBatch) -> Arc<Vectors> {
        let size = batch.row_count();
        match self {
            PhysicalExprs::Column(index) => batch
                .get(index)
                .expect("Expected column to be in record batch"),
            PhysicalExprs::LiteralText(value) => {
                create_literal(Value::Text(value.to_string()), size)
            }
            PhysicalExprs::LiteralBoolean(value) => create_literal(Value::Boolean(*value), size),
            PhysicalExprs::LiteralNumber(value) => {
                create_literal(Value::Number(value.clone()), size)
            }
            PhysicalExprs::LiteralDate(value) => create_literal(Value::Date(*value), size),
            PhysicalExprs::BooleanExpr(expr) => expr.evaluate(batch),
            PhysicalExprs::MathExpr(expr) => expr.evaluate(batch),
        }
    }
}

impl Display for PhysicalExprs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PhysicalExprs::Column(index) => write!(f, "#{}", index),
            PhysicalExprs::LiteralText(value) => write!(f, "{}", value),
            PhysicalExprs::LiteralBoolean(value) => write!(f, "{}", value),
            PhysicalExprs::LiteralNumber(value) => write!(f, "{}", value),
            PhysicalExprs::LiteralDate(value) => write!(f, "{}", value),
            PhysicalExprs::BooleanExpr(expr) => write!(f, "{}", expr),
            PhysicalExprs::MathExpr(expr) => write!(f, "{}", expr),
        }
    }
}

fn create_literal(val: Value, size: usize) -> Arc<Vectors> {
    match val {
        Value::Text(_) => Arc::new(Vectors::LiteralVector(LiteralVector::new(
            DataType::Text,
            val,
            size,
        ))),
        Value::Boolean(_) => Arc::new(Vectors::LiteralVector(LiteralVector::new(
            DataType::Boolean,
            val,
            size,
        ))),
        Value::Number(_) => Arc::new(Vectors::LiteralVector(LiteralVector::new(
            DataType::Number,
            val,
            size,
        ))),
        Value::Date(_) => Arc::new(Vectors::LiteralVector(LiteralVector::new(
            DataType::Date,
            val,
            size,
        ))),
        Value::Null => Arc::new(Vectors::LiteralVector(LiteralVector::new(
            DataType::default(),
            val,
            size,
        ))),
    }
}
