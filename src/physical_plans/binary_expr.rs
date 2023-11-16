use std::{fmt::Display, sync::Arc};

use crate::{
    datatypes::{
        column_vector::{ColumnVector, Vector, Vectors},
        record_batch::RecordBatch,
        types::{DataType, Value},
    },
    sql::operators::{BinaryOp, BooleanOp, MathOp},
};

use super::physical_expr::{PhysicalExpr, PhysicalExprs};

#[derive(Clone)]
pub struct BooleanExpr {
    l: Box<PhysicalExprs>,
    op: BooleanOp,
    r: Box<PhysicalExprs>,
}

impl BooleanExpr {
    pub fn new(l: Box<PhysicalExprs>, op: BooleanOp, r: Box<PhysicalExprs>) -> Self {
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
}

impl Display for BooleanExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let l = self.l.to_string();
        let r = self.r.to_string();
        write!(f, "{} {} {}", l, self.op.to_string(), r)
    }
}

impl PhysicalExpr for BooleanExpr {
    fn evaluate(&self, record_batch: &RecordBatch) -> Arc<Vectors> {
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
        Arc::new(Vectors::ColumnVector(ColumnVector::new(
            DataType::Boolean,
            vector,
        )))
    }
}

#[derive(Clone)]
pub struct MathExpr {
    l: Box<PhysicalExprs>,
    op: MathOp,
    r: Box<PhysicalExprs>,
}

impl MathExpr {
    pub fn new(l: Box<PhysicalExprs>, op: MathOp, r: Box<PhysicalExprs>) -> Self {
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
}

impl Display for MathExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let l = self.l.to_string();
        let r = self.r.to_string();
        write!(f, "{} {} {}", l, self.op.to_string(), r)
    }
}

impl PhysicalExpr for MathExpr {
    fn evaluate(&self, record_batch: &RecordBatch) -> Arc<Vectors> {
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
        Arc::new(Vectors::ColumnVector(ColumnVector::new(*datatype, vector)))
    }
}
