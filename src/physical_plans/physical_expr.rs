use std::sync::Arc;

use crate::datatypes::{
    column_vector::{LiteralVector, Vector},
    record_batch::RecordBatch,
    types::{DataType, Value},
};

#[derive(Clone)]
pub enum PhysicalExpr {
    ColumnExpr(usize),
    LiteralTextExpr(String),
    LiteralBooleanExpr(bool),
    LiteralIntegerExpr(i32),
    LiteralFloatExpr(f32),
}

impl PhysicalExpr {
    pub fn evaluate(&self, batch: &RecordBatch) -> Arc<Vector> {
        let size = batch.row_count();
        match self {
            PhysicalExpr::ColumnExpr(index) => batch
                .get(&index)
                .expect("Expected column to be in record batch"),
            PhysicalExpr::LiteralTextExpr(value) => {
                create_literal(Value::Text(value.to_string()), size)
            }
            PhysicalExpr::LiteralBooleanExpr(value) => create_literal(Value::Boolean(*value), size),
            PhysicalExpr::LiteralIntegerExpr(value) => create_literal(Value::Integer(*value), size),
            PhysicalExpr::LiteralFloatExpr(value) => create_literal(Value::Float(*value), size),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            PhysicalExpr::ColumnExpr(expr) => format!("Column[{}]", expr),
            PhysicalExpr::LiteralTextExpr(expr) => format!("{}", expr),
            PhysicalExpr::LiteralBooleanExpr(expr) => format!("{}", expr),
            PhysicalExpr::LiteralIntegerExpr(expr) => format!("{}", expr),
            PhysicalExpr::LiteralFloatExpr(expr) => format!("{}", expr),
        }
    }
}

fn create_literal(val: Value, size: usize) -> Arc<Vector> {
    match val {
        Value::Text(_) => Arc::new(Vector::LiteralVector(LiteralVector::new(
            DataType::Text,
            val,
            size,
        ))),
        Value::Boolean(_) => Arc::new(Vector::LiteralVector(LiteralVector::new(
            DataType::Boolean,
            val,
            size,
        ))),
        Value::Integer(_) => Arc::new(Vector::LiteralVector(LiteralVector::new(
            DataType::Integer,
            val,
            size,
        ))),
        Value::Float(_) => Arc::new(Vector::LiteralVector(LiteralVector::new(
            DataType::Float,
            val,
            size,
        ))),
    }
}
