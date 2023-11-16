use std::vec;

use crate::datatypes::{column_vector::Vector, record_batch::RecordBatch};

const DIVIDER: &str = "|";

pub struct RecordBatchPrettifier<'a> {
    rb: &'a RecordBatch,
    with_schema: bool,
}

impl RecordBatchPrettifier<'_> {
    pub fn new(rb: &RecordBatch, with_schema: bool) -> RecordBatchPrettifier {
        RecordBatchPrettifier { rb, with_schema }
    }

    fn compute_cell_space(&self) -> Vec<usize> {
        let mut size = (0..self.rb.column_count()).map(|_| 0).collect();
        if self.with_schema {
            size = self
                .rb
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().len())
                .collect::<Vec<usize>>();
        }

        self.rb
            .iter()
            .zip(size)
            .map(|(col, curr_size)| {
                col.iter()
                    .map(|val| {
                        let max_val_string =
                            val.to_string()
                                .split('\n')
                                .fold(String::new(), |acc, value| {
                                    if acc.len() > value.len() {
                                        acc
                                    } else {
                                        value.to_string()
                                    }
                                });
                        std::cmp::max(curr_size, max_val_string.len())
                    })
                    .max()
                    .unwrap_or(curr_size)
            })
            .collect()
    }

    fn pad_value(value: String, space: usize) -> String {
        let mut result = format!(" {}", value);
        while result.len() < space + 2 {
            result.push(' ');
        }
        result
    }

    fn get_divider(cell_space: &[usize]) -> String {
        cell_space
            .iter()
            .map(|space| {
                let mut divider = String::new();
                while divider.len() < space + 2 {
                    divider.push('-');
                }
                divider
            })
            .collect::<Vec<String>>()
            .join("+")
    }

    pub fn prettify(&self) -> String {
        let schema = self.rb.schema();
        let cell_space = self.compute_cell_space();
        let mut results = vec![];

        if self.with_schema {
            let header = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    RecordBatchPrettifier::pad_value(field.name().clone(), cell_space[i])
                })
                .collect::<Vec<String>>()
                .join(DIVIDER);
            results.push(header);
        }

        let divider = RecordBatchPrettifier::get_divider(&cell_space);
        results.push(divider);

        let row_count = self.rb.row_count();
        let col_count = self.rb.column_count();

        (0..row_count).for_each(|i| {
            let result: Vec<String> = (0..col_count)
                .map(|j| {
                    let value = self
                        .rb
                        .get(&j)
                        .expect("Index of record batch should not exceed size")
                        .get_value(&i)
                        .to_string();
                    RecordBatchPrettifier::pad_value(value, cell_space[j])
                })
                .collect();
            results.push(result.join(DIVIDER));
        });

        results.join("\n")
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::{
        datatypes::prettifier::RecordBatchPrettifier,
        datatypes::{
            column_vector::{ColumnVector, Vectors},
            record_batch::RecordBatch,
            schema::{Field, Schema},
            types::{DataType, Value},
        },
    };

    #[test]
    fn test_compute_cell_space() {
        let schema = Schema::new(vec![
            Field::new("id".to_string(), DataType::Number),
            Field::new("name".to_string(), DataType::Text),
            Field::new("age".to_string(), DataType::Number),
        ]);
        let rb = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(Vectors::ColumnVector(ColumnVector::new(
                    DataType::Number,
                    vec![Value::number("1"), Value::number("2"), Value::number("3")],
                ))),
                Arc::new(Vectors::ColumnVector(ColumnVector::new(
                    DataType::Text,
                    vec![
                        Value::Text("Alice".to_string()),
                        Value::Text("Bob".to_string()),
                        Value::Text("Charlie".to_string()),
                    ],
                ))),
                Arc::new(Vectors::ColumnVector(ColumnVector::new(
                    DataType::Number,
                    vec![
                        Value::number("20"),
                        Value::number("21"),
                        Value::number("22"),
                    ],
                ))),
            ],
        );
        let prettifier = RecordBatchPrettifier::new(&rb, true);
        assert_eq!(prettifier.compute_cell_space(), vec![2, 7, 3]);
    }

    #[test]
    fn test_pad_value() {
        let value = "hello".to_string();
        let space = 10;
        let padded_value = RecordBatchPrettifier::pad_value(value, space);
        assert_eq!(padded_value, " hello      ");
    }

    #[test]
    fn test_get_divider() {
        let cell_space = vec![1, 2, 3];
        let divider = RecordBatchPrettifier::get_divider(&cell_space);
        assert_eq!(divider, "---+----+-----");
    }
}
