use super::types::{DataType, Value};

pub trait ColumnVector {
    fn get_type(&self) -> DataType;

    fn get_value(&self, index: usize) -> Value;

    fn size(&self) -> usize;
}

pub struct LiteralColumnVector {
    datatype: DataType,
    values: Vec<Value>,
}

impl LiteralColumnVector {
    fn new(datatype: DataType, values: Vec<Value>) -> LiteralColumnVector {
        LiteralColumnVector { datatype, values }
    }
}

impl ColumnVector for LiteralColumnVector {
    fn get_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn get_value(&self, index: usize) -> Value {
        if index >= self.values.len() {
            panic!("Index out of bounds");
        }
        self.values[index].clone()
    }

    fn size(&self) -> usize {
        self.values.len()
    }
}
