use std::sync::Arc;

use crate::error::ZakuError;

use super::{
    column_vector::{ColumnVector, Vector, Vectors},
    schema::Schema,
    types::Value,
};

pub static VECTOR_SIZE: usize = 1024;

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

    pub fn make_arc_cols(batch: Vec<Vec<Value>>, schema: &Schema) -> Vec<Arc<Vectors>> {
        batch
            .into_iter()
            .enumerate()
            .map(|(i, c)| {
                Arc::new(Vectors::ColumnVector(ColumnVector::new(
                    *schema
                        .get_datatype_from_index(&i)
                        .expect("Index out of bounds"),
                    c,
                )))
            })
            .collect()
    }

    // Takes a vector of vectors (column-format) and converts it to record batches
    pub fn to_record_batch(cols: Vec<Vec<Value>>, schema: &Schema) -> Vec<RecordBatch> {
        let schema_len = schema.fields().len();
        let num_batches = (cols.len() / VECTOR_SIZE) + 1;
        let mut seg_cols: Vec<Vec<Vec<Value>>> = (0..num_batches)
            .map(|_| {
                (0..schema_len)
                    .map(|_| Vec::with_capacity(VECTOR_SIZE))
                    .collect()
            })
            .collect();

        for (i, col) in cols.iter().enumerate() {
            for (j, val) in col.iter().enumerate() {
                let batch_no = j / VECTOR_SIZE;
                seg_cols[batch_no][i].push(val.clone());
            }
        }

        let mut rbs = vec![];
        for batch in seg_cols {
            let arc_cols = RecordBatch::make_arc_cols(batch, schema);
            rbs.push(RecordBatch::new(schema.clone(), arc_cols));
        }
        rbs
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
            Field::new("id".to_string(), DataType::Number),
            Field::new("name".to_string(), DataType::Text),
        ]);
        let columns = vec![
            Arc::new(Vectors::LiteralVector(LiteralVector::new(
                DataType::Number,
                Value::number("0"),
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
            Field::new("1".to_string(), DataType::Number),
            Field::new("2".to_string(), DataType::Text),
        ]);
        let columns = vec![
            Arc::new(Vectors::ColumnVector(ColumnVector::new(
                DataType::Number,
                vec![
                    Value::number("0"),
                    Value::number("2"),
                    Value::number("1"),
                    Value::number("3"),
                ],
            ))),
            Arc::new(Vectors::ColumnVector(ColumnVector::new(
                DataType::Number,
                vec![
                    Value::number("0"),
                    Value::number("2"),
                    Value::number("1"),
                    Value::number("1"),
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
                assert_eq!(v, &Value::number(ex1[i].to_string().as_str()));
            });

        let ex2 = [0, 1, 2, 1];
        sorted_batch.columns[1]
            .iter()
            .enumerate()
            .for_each(|(i, v)| {
                assert_eq!(v, &Value::number(ex2[i].to_string().as_str()));
            });
    }
}
