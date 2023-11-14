use std::sync::Arc;

use crate::{
    datatypes::{
        column_vector::Vectors,
        record_batch::RecordBatch,
        schema::{Field, Schema},
        types::{DataType, Value},
    },
    Datasink,
};

pub struct DatasinkBuilder {
    schema: Option<Schema>,
    data: Option<Vec<Arc<Vectors>>>,
}

impl DatasinkBuilder {
    pub fn new(schema: Option<Schema>, data: Option<Vec<Arc<Vectors>>>) -> DatasinkBuilder {
        DatasinkBuilder { schema, data }
    }

    fn get_datatype_from_str(str_val: &str) -> DataType {
        match str_val {
            "num" => DataType::Number,
            "text" => DataType::Text,
            "bool" => DataType::Boolean,
            _ => panic!("Unsupported datatype"),
        }
    }

    pub fn add_schema(mut self, col_names: Vec<&str>, datatypes: Vec<&str>) -> DatasinkBuilder {
        let datatypes = datatypes
            .iter()
            .map(|d| DatasinkBuilder::get_datatype_from_str(d))
            .collect::<Vec<DataType>>();
        let fields = col_names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let datatype = datatypes[i];
                Field::new(name.to_string(), datatype)
            })
            .collect();
        self.schema = Some(Schema::new(fields));
        self
    }

    // data is row-based
    pub fn add_data(mut self, data: Vec<Vec<&str>>) -> DatasinkBuilder {
        let mut cols = vec![];
        let datatypes = self
            .schema
            .as_ref()
            .expect("Schema not set")
            .fields()
            .iter()
            .map(|f| f.datatype())
            .collect::<Vec<&DataType>>();

        (0..data[0].len()).for_each(|_| cols.push(vec![]));

        data.iter().for_each(|row| {
            row.iter().enumerate().for_each(|(i, str_val)| {
                let datatype = datatypes[i];
                let val = Value::get_value_from_string_val(str_val, datatype);
                cols[i].push(val);
            })
        });

        let arc_cols = RecordBatch::make_arc_cols(cols, self.schema.as_ref().unwrap());
        self.data = Some(arc_cols);
        self
    }

    pub fn build(&self) -> Datasink {
        Datasink::new(
            self.schema.as_ref().expect("Schema not set").clone(),
            self.data.as_ref().expect("Data not set").clone(),
        )
    }
}

impl Default for DatasinkBuilder {
    fn default() -> Self {
        Self::new(None, None)
    }
}
