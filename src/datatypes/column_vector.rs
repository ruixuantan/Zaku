use super::types::{DataType, Value};

pub struct ColumnVector {
    datatype: DataType,
    values: Vec<Value>,
}

impl ColumnVector {
    pub fn new(datatype: DataType, values: Vec<Value>) -> ColumnVector {
        ColumnVector { datatype, values }
    }

    pub fn get_type(&self) -> DataType {
        self.datatype.clone()
    }

    pub fn get_value(&self, index: usize) -> Value {
        if index >= self.values.len() {
            panic!("Index out of bounds");
        }
        self.values[index].clone()
    }

    pub fn size(&self) -> usize {
        self.values.len()
    }

    pub fn add(&mut self, value: Value) {
        self.values.push(value);
    }
}
