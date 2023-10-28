use crate::datatypes::{column_vector::ColumnVector, record_batch::RecordBatch};

#[derive(Clone)]
pub enum PhysicalExpr {
    ColumnExpr(usize),
}

impl PhysicalExpr {
    pub fn evaluate(&self, batch: &RecordBatch) -> ColumnVector {
        match self {
            PhysicalExpr::ColumnExpr(index) => {
                batch.get(&index).expect("Expected column to be in batch")
            }
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            PhysicalExpr::ColumnExpr(expr) => format!("Column: {}", expr),
        }
    }
}
