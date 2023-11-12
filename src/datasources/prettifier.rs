use std::vec;

use crate::datatypes::{
    column_vector::{Vector, Vectors},
    schema::Schema,
};

use super::datasink::Datasink;

const DIVIDER: &str = "|";

fn compute_cell_space(schema: &Schema, data: &Datasink) -> Vec<usize> {
    let size = schema.fields().iter().map(|f| f.alias().len());

    data.iter()
        .zip(size)
        .map(|(col, curr_size)| match col.as_ref() {
            Vectors::ColumnVector(vector) => vector
                .iter()
                .fold(curr_size, |acc, e| std::cmp::max(acc, e.to_string().len())),
            Vectors::LiteralVector(vector) => {
                let max_vector_string =
                    vector
                        .value()
                        .to_string()
                        .split('\n')
                        .fold(String::new(), |acc, value| {
                            if acc.len() > value.len() {
                                acc
                            } else {
                                value.to_string()
                            }
                        });
                std::cmp::max(curr_size, max_vector_string.len())
            }
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

pub fn prettify(data: &Datasink) -> String {
    let schema = data.schema();
    let cell_space = compute_cell_space(schema, data);
    let mut results = vec![];

    let header = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| pad_value(field.alias().clone(), cell_space[i]))
        .collect::<Vec<String>>()
        .join(DIVIDER);
    results.push(header);

    let divider = get_divider(&cell_space);
    results.push(divider);

    let row_count = data.row_count();
    let col_count = data.column_count();

    (0..row_count).for_each(|i| {
        let result: Vec<String> = (0..col_count)
            .map(|j| {
                let value = data
                    .get(&j)
                    .expect("Index of record batch should not exceed size")
                    .get_value(&i)
                    .to_string();
                pad_value(value, cell_space[j])
            })
            .collect();
        results.push(result.join(DIVIDER));
    });

    results.push(format!("({} rows)", row_count));

    results.join("\n")
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::{compute_cell_space, get_divider, pad_value, Datasink};
    use crate::datatypes::{
        column_vector::{ColumnVector, Vectors},
        schema::{Field, Schema},
        types::{DataType, Value},
    };

    #[test]
    fn test_compute_cell_space() {
        let schema = Schema::new(vec![
            Field::new("id".to_string(), DataType::Integer),
            Field::new("name".to_string(), DataType::Text),
            Field::new("age".to_string(), DataType::Integer),
        ]);
        let data = Datasink::new(
            schema.clone(),
            vec![
                Arc::new(Vectors::ColumnVector(ColumnVector::new(
                    DataType::Integer,
                    vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
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
                    DataType::Integer,
                    vec![Value::Integer(20), Value::Integer(21), Value::Integer(22)],
                ))),
            ],
        );
        let cell_space = compute_cell_space(&schema, &data);
        assert_eq!(cell_space, vec![2, 7, 3]);
    }

    #[test]
    fn test_pad_value() {
        let value = "hello".to_string();
        let space = 10;
        let padded_value = pad_value(value, space);
        assert_eq!(padded_value, " hello      ");
    }

    #[test]
    fn test_get_divider() {
        let cell_space = vec![1, 2, 3];
        let divider = get_divider(&cell_space);
        assert_eq!(divider, "---+----+-----");
    }
}
