use std::collections::HashMap;

use super::types::DataType;

#[derive(Clone)]
pub struct Schema {
    fields: Vec<String>,
    info: HashMap<String, DataType>,
}

impl Schema {
    pub fn new(fields: Vec<String>, info: HashMap<String, DataType>) -> Schema {
        Schema { fields, info }
    }

    pub fn get_fields(&self) -> &Vec<String> {
        &self.fields
    }

    pub fn get_datatype(&self, field: &String) -> &DataType {
        &self.info[field]
    }

    pub fn get_datatype_from_index(&self, index: &usize) -> &DataType {
        &self.info[&self.fields[*index]]
    }
}
