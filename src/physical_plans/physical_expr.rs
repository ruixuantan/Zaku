use crate::datatypes::{column_vector::ColumnVector, record_batch::RecordBatch};

pub trait PhysicalExpr {
    fn evaluate(&self, batch: &RecordBatch) -> ColumnVector;

    fn to_string(&self) -> String;
}

pub struct ColumnExpr {
    index: usize,
}

impl ColumnExpr {
    pub fn new(index: usize) -> ColumnExpr {
        ColumnExpr { index }
    }
}

impl PhysicalExpr for ColumnExpr {
    fn evaluate(&self, batch: &RecordBatch) -> ColumnVector {
        batch.get(&self.index).expect("Index out of bounds")
    }

    fn to_string(&self) -> String {
        format!("Column: {}", self.index)
    }
}
