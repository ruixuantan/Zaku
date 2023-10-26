use crate::error::ZakuError;

use super::{column_vector::ColumnVector, schema::Schema, types::Value};

pub struct RecordBatch {
    schema: Schema,
    columns: Vec<ColumnVector>,
}

impl RecordBatch {
    pub fn new(schema: Schema) -> RecordBatch {
        let mut columns = Vec::new();
        schema.get_fields().iter().for_each(|field| {
            columns.push(ColumnVector::new(field.get_datatype().clone(), Vec::new()));
        });
        RecordBatch { schema, columns }
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn row_count(&self) -> usize {
        self.columns[0].size()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn insert_row(&mut self, row: Vec<String>) -> Result<(), ZakuError> {
        row.iter().enumerate().try_for_each(|(i, r)| {
            let datatype = self.schema.get_datatype_from_index(&i)?;
            let val = Value::get_value_from_string_val(&r, datatype);
            self.columns[i].add(val);
            Ok::<(), ZakuError>(())
        })
    }
}
