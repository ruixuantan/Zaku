use std::sync::Arc;

use crate::error::ZakuError;

use super::{column_vector::ColumnVector, schema::Schema};

#[derive(Debug, PartialEq, Clone)]
pub struct RecordBatch {
    schema: Schema,
    columns: Vec<Arc<ColumnVector>>,
}

impl RecordBatch {
    pub fn new(schema: Schema, columns: Vec<Arc<ColumnVector>>) -> RecordBatch {
        RecordBatch { schema, columns }
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn get_columns(&self) -> &Vec<Arc<ColumnVector>> {
        &self.columns
    }

    pub fn row_count(&self) -> usize {
        self.columns[0].size()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get(&self, index: &usize) -> Result<Arc<ColumnVector>, ZakuError> {
        if index >= &self.column_count() {
            return Err(ZakuError::new("Index out of bounds".to_string()));
        }
        Ok(self.columns[*index].clone())
    }
}
