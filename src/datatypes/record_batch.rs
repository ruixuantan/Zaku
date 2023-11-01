use std::sync::Arc;

use crate::error::ZakuError;

use super::{column_vector::Vector, schema::Schema};

#[derive(Debug, PartialEq, Clone)]
pub struct RecordBatch {
    schema: Schema,
    columns: Vec<Arc<Vector>>,
}

impl RecordBatch {
    pub fn new(schema: Schema, columns: Vec<Arc<Vector>>) -> RecordBatch {
        RecordBatch { schema, columns }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn iter(&self) -> RecordBatchIterator {
        RecordBatchIterator {
            record_batch: self,
            index: 0,
        }
    }

    pub fn columns(&self) -> &Vec<Arc<Vector>> {
        &self.columns
    }

    pub fn row_count(&self) -> usize {
        self.columns[0].size()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get(&self, index: &usize) -> Result<Arc<Vector>, ZakuError> {
        if index >= &self.column_count() {
            return Err(ZakuError::new("Index out of bounds".to_string()));
        }
        Ok(self.columns[*index].clone())
    }
}

pub struct RecordBatchIterator<'a> {
    record_batch: &'a RecordBatch,
    index: usize,
}

impl<'a> Iterator for RecordBatchIterator<'a> {
    type Item = &'a Arc<Vector>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.record_batch.column_count() {
            return None;
        } else {
            let col = &self.record_batch.columns[self.index];
            self.index += 1;
            return Some(col);
        }
    }
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use super::RecordBatch;
    use crate::datatypes::column_vector::{LiteralVector, Vector};
    use crate::datatypes::schema::{Field, Schema};
    use crate::datatypes::types::{DataType, Value};

    #[test]
    fn test_record_batch_iterator() {
        let schema = Schema::new(vec![
            Field::new("id".to_string(), DataType::Integer),
            Field::new("name".to_string(), DataType::Text),
        ]);
        let mut columns = Vec::new();
        columns.push(Arc::new(Vector::LiteralVector(LiteralVector::new(
            DataType::Integer,
            Value::Integer(0),
            10,
        ))));
        columns.push(Arc::new(Vector::LiteralVector(LiteralVector::new(
            DataType::Text,
            Value::Text("dummy".to_string()),
            10,
        ))));
        let record_batch = RecordBatch::new(schema, columns);

        for i in 0..record_batch.column_count() + 1 {
            if i == record_batch.column_count() {
                assert!(record_batch.get(&i).is_err());
            } else {
                let col = record_batch.get(&i).unwrap();
                assert_eq!(col.size(), 10);
            }
        }
    }
}
