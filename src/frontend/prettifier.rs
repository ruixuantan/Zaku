use std::vec;

use crate::datatypes::{column_vector::Vector, record_batch::RecordBatch, schema::Schema};

fn compute_cell_space(schema: &Schema, record_batch: &RecordBatch) -> Vec<usize> {
    let mut size = vec![];
    schema.fields().iter().for_each(|field| {
        size.push(field.name().len());
    });
    record_batch
        .columns()
        .iter()
        .enumerate()
        .for_each(|(i, col)| {
            let mut max = size[i];
            match col.as_ref() {
                Vector::ColumnVector(vector) => {
                    vector.values().iter().for_each(|val| {
                        if val.to_string().len() > max {
                            max = val.to_string().len();
                        }
                    });
                }
                Vector::LiteralVector(vector) => {
                    if vector.get_value(&i).to_string().len() > max {
                        max = vector.get_value(&i).to_string().len();
                    }
                }
            }
            size[i] = max;
        });
    size
}

fn pad_value(value: String, space: usize) -> String {
    let mut result = format!(" {}", value);
    while result.len() < space + 2 {
        result.push(' ');
    }
    result
}

fn get_divider(cell_space: &Vec<usize>) -> String {
    let mut results = vec![];
    cell_space.iter().for_each(|space| {
        let mut divider = String::new();
        while divider.len() < space + 2 {
            divider.push('-');
        }
        results.push(divider);
    });
    results.join("+")
}

pub fn prettify(record_batch: &RecordBatch) -> String {
    let schema = record_batch.schema();
    let cell_space = compute_cell_space(schema, record_batch);
    let mut results = vec![];

    let header = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| pad_value(field.name().clone(), cell_space[i]))
        .collect::<Vec<String>>()
        .join("|");
    results.push(header);

    let divider = get_divider(&cell_space);
    results.push(divider);

    let row_count = record_batch.row_count();
    let col_count = record_batch.column_count();
    let columns = record_batch.columns();

    (0..row_count).for_each(|i| {
        let mut result = vec![];
        (0..col_count).for_each(|j| {
            let value = columns[j].get_value(&i).to_string();
            let padded_value = pad_value(value, cell_space[j]);
            result.push(padded_value);
        });
        results.push(result.join("|"));
    });

    results.push(format!("({} rows)", row_count));

    results.join("\n")
}
