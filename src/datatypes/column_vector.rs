use super::types::{DataType, Value};

#[derive(Debug, PartialEq, Clone)]
pub enum Vector {
    ColumnVector(ColumnVector),
    LiteralVector(LiteralVector),
}

impl Vector {
    pub fn get_type(&self) -> &DataType {
        match self {
            Vector::ColumnVector(vector) => vector.get_type(),
            Vector::LiteralVector(vector) => &vector.datatype,
        }
    }

    pub fn get_value(&self, index: &usize) -> &Value {
        match self {
            Vector::ColumnVector(vector) => vector.get_value(index),
            Vector::LiteralVector(vector) => vector.get_value(index),
        }
    }

    pub fn values(&self) -> Vec<Value> {
        match self {
            Vector::ColumnVector(vector) => vector.values().clone(),
            Vector::LiteralVector(vector) => vector.values(),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Vector::ColumnVector(vector) => vector.size(),
            Vector::LiteralVector(vector) => vector.size,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnVector {
    datatype: DataType,
    values: Vec<Value>,
}

impl ColumnVector {
    pub fn new(datatype: DataType, values: Vec<Value>) -> ColumnVector {
        ColumnVector { datatype, values }
    }

    pub fn get_type(&self) -> &DataType {
        &self.datatype
    }

    pub fn values(&self) -> &Vec<Value> {
        &self.values
    }

    pub fn get_value(&self, index: &usize) -> &Value {
        if *index >= self.values.len() {
            panic!("Index out of bounds");
        }
        &self.values[*index]
    }

    pub fn size(&self) -> usize {
        self.values.len()
    }

    pub fn add(&mut self, value: Value) {
        self.values.push(value);
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct LiteralVector {
    datatype: DataType,
    value: Value,
    size: usize,
}

impl LiteralVector {
    pub fn new(datatype: DataType, value: Value, size: usize) -> LiteralVector {
        LiteralVector {
            datatype,
            value,
            size,
        }
    }

    pub fn get_value(&self, index: &usize) -> &Value {
        if *index >= self.size {
            panic!("Index out of bounds");
        }
        &self.value
    }

    pub fn values(&self) -> Vec<Value> {
        (0..self.size).map(|_| self.value.clone()).collect()
    }
}
