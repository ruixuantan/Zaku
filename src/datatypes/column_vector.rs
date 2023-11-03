use enum_dispatch::enum_dispatch;

use super::types::{DataType, Value};

#[enum_dispatch]
pub trait Vector {
    fn get_type(&self) -> &DataType;

    fn get_value(&self, index: &usize) -> &Value;

    fn iter(&self) -> Box<dyn Iterator<Item = &Value> + '_>;

    fn size(&self) -> usize;
}

#[derive(Debug, PartialEq, Clone)]
#[enum_dispatch(Vector)]
pub enum Vectors {
    ColumnVector(ColumnVector),
    LiteralVector(LiteralVector),
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
}

impl Vector for ColumnVector {
    fn get_type(&self) -> &DataType {
        &self.datatype
    }

    fn get_value(&self, index: &usize) -> &Value {
        if *index >= self.values.len() {
            panic!("Index out of bounds");
        }
        &self.values[*index]
    }

    fn size(&self) -> usize {
        self.values.len()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = &Value> + '_> {
        Box::new(ColumnVectorIterator {
            column_vector: self,
            index: 0,
        })
    }
}

pub struct ColumnVectorIterator<'a> {
    column_vector: &'a ColumnVector,
    index: usize,
}

impl<'a> Iterator for ColumnVectorIterator<'a> {
    type Item = &'a Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.column_vector.size() {
            None
        } else {
            let val = &self.column_vector.values[self.index];
            self.index += 1;
            Some(val)
        }
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

    pub fn value(&self) -> &Value {
        &self.value
    }
}

impl Vector for LiteralVector {
    fn get_value(&self, index: &usize) -> &Value {
        if *index >= self.size {
            panic!("Index out of bounds");
        }
        &self.value
    }

    fn get_type(&self) -> &DataType {
        &self.datatype
    }

    fn size(&self) -> usize {
        self.size
    }

    fn iter(&self) -> Box<dyn Iterator<Item = &Value> + '_> {
        Box::new(LiteralVectorIterator {
            literal_vector: self,
            index: 0,
        })
    }
}

pub struct LiteralVectorIterator<'a> {
    literal_vector: &'a LiteralVector,
    index: usize,
}

impl<'a> Iterator for LiteralVectorIterator<'a> {
    type Item = &'a Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.literal_vector.size {
            None
        } else {
            self.index += 1;
            Some(&self.literal_vector.value)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::datatypes::{
        column_vector::ColumnVector,
        types::{DataType, Value},
    };

    use super::{LiteralVector, Vector};

    #[test]
    fn test_column_vector_iterator() {
        let vector = ColumnVector::new(
            DataType::Integer,
            vec![Value::Integer(0), Value::Integer(1), Value::Integer(2)],
        );
        for i in 0..vector.size() + 1 {
            if i == vector.size() {
                let res = std::panic::catch_unwind(|| vector.get_value(&i));
                assert!(res.is_err());
            } else {
                assert_eq!(vector.get_value(&i), &Value::Integer(i as i32));
            }
        }
    }

    #[test]
    fn test_literal_vector_iterator() {
        let vector = LiteralVector::new(DataType::Integer, Value::Integer(2), 3);
        for i in 0..vector.size + 1 {
            if i == vector.size {
                let res = std::panic::catch_unwind(|| vector.get_value(&i));
                assert!(res.is_err());
            } else {
                assert_eq!(vector.get_value(&i), &Value::Integer(2));
            }
        }
    }
}
