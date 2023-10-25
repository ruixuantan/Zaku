use super::{column_vector::ColumnVector, schema::Schema, types::Value};

pub struct RecordBatch {
    schema: Schema,
    columns: Vec<ColumnVector>,
}

impl RecordBatch {
    pub fn new(schema: Schema) -> RecordBatch {
        let mut columns = Vec::new();
        for field in schema.get_fields() {
            let datatype = schema.get_datatype(&field);
            let column = ColumnVector::new(datatype.clone(), Vec::new());
            columns.push(column);
        }
        RecordBatch { schema, columns }
    }

    pub fn get_schema(&self) -> Schema {
        self.schema.clone()
    }

    pub fn row_count(&self) -> usize {
        self.columns[0].size()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn insert_row(&mut self, row: Vec<String>) {
        for (i, r) in row.iter().enumerate() {
            let val =
                Value::get_value_from_string_val(&r, &self.schema.get_datatype_from_index(&i));
            self.columns[i].add(val);
        }
    }
}
