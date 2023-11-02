use std::sync::Arc;

use crate::{
    datatypes::{
        column_vector::{ColumnVector, Vector, VectorTrait},
        record_batch::RecordBatch,
        types::{DataType, Value},
    },
    sql::operators::{BooleanOp, MathOp},
};

use super::physical_expr::{PhysicalExpr, PhysicalExprTrait};

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
}

impl PhysicalExprTrait for BooleanExpr {
    fn evaluate(&self, record_batch: &RecordBatch) -> Arc<Vector> {
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
}

impl PhysicalExprTrait for MathExpr {
    fn evaluate(&self, record_batch: &RecordBatch) -> Arc<Vector> {
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
        Arc::new(Vector::ColumnVector(ColumnVector::new(*datatype, vector)))
    }
}
