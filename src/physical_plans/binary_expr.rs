use std::sync::Arc;

use crate::{
    datatypes::{
        column_vector::{ColumnVector, Vector},
        record_batch::RecordBatch,
        types::{DataType, Value},
    },
    error::ZakuError,
};

use super::physical_expr::PhysicalExpr;

#[derive(Clone)]
pub enum BooleanOp {
    And,
    Or,
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl BooleanOp {
    pub fn from_str(op: &str) -> Result<BooleanOp, ZakuError> {
        match op.to_lowercase().as_str() {
            "and" => Ok(BooleanOp::And),
            "or" => Ok(BooleanOp::Or),
            "=" => Ok(BooleanOp::Eq),
            "<>" => Ok(BooleanOp::Neq),
            ">" => Ok(BooleanOp::Gt),
            ">=" => Ok(BooleanOp::Gte),
            "<" => Ok(BooleanOp::Lt),
            "<=" => Ok(BooleanOp::Lte),
            _ => Err(ZakuError::new("Invalid boolean operator".to_string())),
        }
    }
}

#[derive(Clone)]
pub struct BooleanExpr {
    l: Box<PhysicalExpr>,
    op: BooleanOp,
    r: Box<PhysicalExpr>,
}

impl BooleanExpr {
    pub fn new(l: Box<PhysicalExpr>, op: BooleanOp, r: Box<PhysicalExpr>) -> Self {
        Self { l, op, r }
    }

    fn evaluate_row(&self, l: &Value, r: &Value) -> Value {
        match self.op {
            BooleanOp::And => l.and(r),
            BooleanOp::Or => l.or(r),
            BooleanOp::Eq => l.eq(r),
            BooleanOp::Neq => l.neq(r),
            BooleanOp::Gt => l.gt(r),
            BooleanOp::Gte => l.gte(r),
            BooleanOp::Lt => l.lt(r),
            BooleanOp::Lte => l.lte(r),
        }
    }

    pub fn evaluate(&self, record_batch: &RecordBatch) -> Arc<Vector> {
        let row_num = record_batch.row_count();
        let l = self.l.evaluate(record_batch);
        let r = self.r.evaluate(record_batch);

        let vector: Vec<Value> = (0..row_num)
            .map(|i| {
                let l_val = l.get_value(&i);
                let r_val = r.get_value(&i);
                self.evaluate_row(l_val, r_val)
            })
            .collect();
        Arc::new(Vector::ColumnVector(ColumnVector::new(
            DataType::Boolean,
            vector,
        )))
    }
}

#[derive(Clone)]
pub enum MathOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl MathOp {
    pub fn from_str(op: &str) -> Result<MathOp, ZakuError> {
        match op.to_lowercase().as_str() {
            "+" => Ok(MathOp::Add),
            "-" => Ok(MathOp::Sub),
            "*" => Ok(MathOp::Mul),
            "/" => Ok(MathOp::Div),
            "%" => Ok(MathOp::Mod),
            _ => Err(ZakuError::new("Invalid math operator".to_string())),
        }
    }
}

#[derive(Clone)]
pub struct MathExpr {
    l: Box<PhysicalExpr>,
    op: MathOp,
    r: Box<PhysicalExpr>,
}

impl MathExpr {
    pub fn new(l: Box<PhysicalExpr>, op: MathOp, r: Box<PhysicalExpr>) -> Self {
        Self { l, op, r }
    }

    fn evaluate_row(&self, l: &Value, r: &Value) -> Value {
        match self.op {
            MathOp::Add => l.add(r),
            MathOp::Sub => l.sub(r),
            MathOp::Mul => l.mul(r),
            MathOp::Div => l.div(r),
            MathOp::Mod => l.modulo(r),
        }
    }

    pub fn evaluate(&self, record_batch: &RecordBatch) -> Arc<Vector> {
        let row_num = record_batch.row_count();
        let l = self.l.evaluate(record_batch);
        let r = self.r.evaluate(record_batch);
        let datatype = l.get_type();

        let vector: Vec<Value> = (0..row_num)
            .map(|i| {
                let l_val = l.get_value(&i);
                let r_val = r.get_value(&i);
                self.evaluate_row(l_val, r_val)
            })
            .collect();
        Arc::new(Vector::ColumnVector(ColumnVector::new(
            datatype.clone(),
            vector,
        )))
    }
}
