use std::sync::Arc;

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
    ColumnExpr(usize),
    LiteralTextExpr(String),
    LiteralBooleanExpr(bool),
    LiteralIntegerExpr(i32),
    LiteralFloatExpr(f32),
    BooleanExpr(BooleanExpr),
    MathExpr(MathExpr),
}

impl PhysicalExpr for PhysicalExprs {
    fn evaluate(&self, batch: &RecordBatch) -> Arc<Vectors> {
        let size = batch.row_count();
        match self {
            PhysicalExprs::ColumnExpr(index) => batch
                .get(index)
                .expect("Expected column to be in record batch"),
            PhysicalExprs::LiteralTextExpr(value) => {
                create_literal(Value::Text(value.to_string()), size)
            }
            PhysicalExprs::LiteralBooleanExpr(value) => {
                create_literal(Value::Boolean(*value), size)
            }
            PhysicalExprs::LiteralIntegerExpr(value) => {
                create_literal(Value::Integer(*value), size)
            }
            PhysicalExprs::LiteralFloatExpr(value) => create_literal(Value::Float(*value), size),
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
        Value::Integer(_) => Arc::new(Vectors::LiteralVector(LiteralVector::new(
            DataType::Integer,
            val,
            size,
        ))),
        Value::Float(_) => Arc::new(Vectors::LiteralVector(LiteralVector::new(
            DataType::Float,
            val,
            size,
        ))),
    }
}
