use std::sync::Arc;

use bigdecimal::BigDecimal;

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
            PhysicalExprs::BooleanExpr(expr) => expr.evaluate(batch),
            PhysicalExprs::MathExpr(expr) => expr.evaluate(batch),
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
        Value::Null => Arc::new(Vectors::LiteralVector(LiteralVector::new(
            DataType::default(),
            val,
            size,
        ))),
    }
}
