use std::sync::Arc;

use crate::datatypes::{
    column_vector::{ColumnVector, Vectors},
    record_batch::RecordBatch,
    schema::{Field, Schema},
    types::{DataType, Value},
};

#[derive(Debug, Clone, PartialEq)]
pub struct ContainerData {
    pub schema: Schema,
    pub data: Vec<RecordBatch>,
}

pub struct ContainerDataBuilder {
    schema: Option<Schema>,
    data: Option<Vec<Vec<Value>>>,
}

impl ContainerDataBuilder {
    pub fn new(schema: Option<Schema>, data: Option<Vec<Vec<Value>>>) -> ContainerDataBuilder {
        ContainerDataBuilder { schema, data }
    }

    fn get_datatype_from_str(str_val: &str) -> DataType {
        match str_val {
            "num" => DataType::Number,
            "text" => DataType::Text,
            "bool" => DataType::Boolean,
            "date" => DataType::Date,
            _ => panic!("Unsupported datatype"),
        }
    }

    pub fn add_schema(
        mut self,
        col_names: Vec<&str>,
        datatypes: Vec<&str>,
    ) -> ContainerDataBuilder {
        let datatypes = datatypes
            .iter()
            .map(|d| ContainerDataBuilder::get_datatype_from_str(d))
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
    pub fn add_data(mut self, data: Vec<Vec<&str>>) -> ContainerDataBuilder {
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

        self.data = Some(cols);
        self
    }

    pub fn build(&self) -> ContainerData {
        let schema = self.schema.clone().expect("Schema not set");
        let mut arc_vec = vec![];
        if let Some(cols) = self.data.clone() {
            cols.iter().enumerate().for_each(|(i, col)| {
                let vec = ColumnVector::new(
                    *self
                        .schema
                        .clone()
                        .unwrap()
                        .get_datatype_from_index(&i)
                        .unwrap(),
                    col.clone(),
                );
                arc_vec.push(Arc::new(Vectors::ColumnVector(vec)));
            })
        } else {
            schema.fields().iter().for_each(|f| {
                let vec = ColumnVector::new(*f.datatype(), vec![]);
                arc_vec.push(Arc::new(Vectors::ColumnVector(vec)));
            })
        }
        ContainerData {
            schema: schema.clone(),
            data: vec![RecordBatch::new(schema, arc_vec)],
        }
    }
}

impl Default for ContainerDataBuilder {
    fn default() -> Self {
        Self::new(None, None)
    }
}
