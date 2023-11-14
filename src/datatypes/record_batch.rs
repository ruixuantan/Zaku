use std::sync::Arc;

use crate::error::ZakuError;

use super::{
    column_vector::{Vector, Vectors},
    schema::Schema,
};

pub static VECTOR_SIZE: usize = 1000;

#[derive(Debug, PartialEq, Clone)]
pub struct RecordBatch {
    schema: Schema,
    columns: Vec<Arc<Vectors>>,
}

impl RecordBatch {
    pub fn new(schema: Schema, columns: Vec<Arc<Vectors>>) -> RecordBatch {
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

    pub fn columns(&self) -> &Vec<Arc<Vectors>> {
        &self.columns
    }

    pub fn row_count(&self) -> usize {
        self.columns[0].size()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get(&self, index: &usize) -> Result<Arc<Vectors>, ZakuError> {
        if index >= &self.column_count() {
            return Err(ZakuError::new("Index out of bounds"));
        }
        Ok(self.columns[*index].clone())
    }

    pub fn sort(&self, keys: &[usize], asc: &[bool]) -> Result<RecordBatch, ZakuError> {
        let mut sorted_cols = self.columns.clone();
        let asc: Vec<&bool> = asc.iter().rev().collect();
        keys.iter().rev().enumerate().for_each(|(i, k)| {
            let mut new_sorted_cols = vec![];
            let mut indices = sorted_cols[*k].sort_indices();
            if !asc[i] {
                indices.reverse();
            }

            sorted_cols.iter().for_each(|col| {
                new_sorted_cols.push(Arc::new(col.reorder(&indices)));
            });
            sorted_cols = new_sorted_cols;
        });
        Ok(RecordBatch::new(self.schema.clone(), sorted_cols))
    }

    pub fn merge(&self, other: &RecordBatch) -> Result<RecordBatch, ZakuError> {
        if self.schema != other.schema {
            return Err(ZakuError::new("Schema mismatch"));
        }

        let mut merged_cols = vec![];
        self.columns
            .iter()
            .zip(other.columns.iter())
            .for_each(|(l, r)| {
                merged_cols.push(Arc::new(l.merge(r)));
            });
        Ok(RecordBatch::new(self.schema.clone(), merged_cols))
    }
}

pub struct RecordBatchIterator<'a> {
    record_batch: &'a RecordBatch,
    index: usize,
}

impl<'a> Iterator for RecordBatchIterator<'a> {
    type Item = &'a Arc<Vectors>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.record_batch.column_count() {
            None
        } else {
            let col = &self.record_batch.columns[self.index];
            self.index += 1;
            Some(col)
        }
    }
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use super::RecordBatch;
    use crate::datatypes::column_vector::{ColumnVector, LiteralVector, Vector, Vectors};
    use crate::datatypes::schema::{Field, Schema};
    use crate::datatypes::types::{DataType, Value};

    #[test]
    fn test_record_batch_iterator() {
        let schema = Schema::new(vec![
            Field::new("id".to_string(), DataType::Integer),
            Field::new("name".to_string(), DataType::Text),
        ]);
        let columns = vec![
            Arc::new(Vectors::LiteralVector(LiteralVector::new(
                DataType::Integer,
                Value::Integer(0),
                10,
            ))),
            Arc::new(Vectors::LiteralVector(LiteralVector::new(
                DataType::Text,
                Value::Text("dummy".to_string()),
                10,
            ))),
        ];
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

    #[test]
    fn test_sort() {
        let schema = Schema::new(vec![
            Field::new("1".to_string(), DataType::Integer),
            Field::new("2".to_string(), DataType::Text),
        ]);
        let columns = vec![
            Arc::new(Vectors::ColumnVector(ColumnVector::new(
                DataType::Integer,
                vec![
                    Value::Integer(0),
                    Value::Integer(2),
                    Value::Integer(1),
                    Value::Integer(3),
                ],
            ))),
            Arc::new(Vectors::ColumnVector(ColumnVector::new(
                DataType::Integer,
                vec![
                    Value::Integer(0),
                    Value::Integer(2),
                    Value::Integer(1),
                    Value::Integer(1),
                ],
            ))),
        ];
        let record_batch = RecordBatch::new(schema, columns);

        let sorted_batch = record_batch.sort(&[0, 1], &[true, false]).unwrap();

        let ex1 = [0, 1, 2, 3];
        sorted_batch.columns[0]
            .iter()
            .enumerate()
            .for_each(|(i, v)| {
                assert_eq!(v, &Value::Integer(ex1[i]));
            });

        let ex2 = [0, 1, 2, 1];
        sorted_batch.columns[1]
            .iter()
            .enumerate()
            .for_each(|(i, v)| {
                assert_eq!(v, &Value::Integer(ex2[i]));
            });
    }
}
