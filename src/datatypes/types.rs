#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Integer(i32),
    Float(f32),
    Text(String),
    Boolean(bool),
}

impl DataType {
    pub fn get_type_from_string_val(val: &str) -> DataType {
        if val.parse::<i32>().is_ok() {
            return DataType::Integer;
        }
        if val.parse::<f32>().is_ok() {
            return DataType::Float;
        }
        if val.parse::<bool>().is_ok() {
            return DataType::Boolean;
        }
        DataType::Text
    }
}

impl Value {
    pub fn get_value_from_string_val(val: &str, datatype: &DataType) -> Value {
        match datatype {
            DataType::Integer => Value::Integer(
                val.parse::<i32>()
                    .expect(format!("Expected integer, got {val}").as_str()),
            ),
            DataType::Float => Value::Float(
                val.parse::<f32>()
                    .expect(format!("Expected float, got {val}").as_str()),
            ),
            DataType::Boolean => Value::Boolean(
                val.parse::<bool>()
                    .expect(format!("Expected boolean, got {val}").as_str()),
            ),
            DataType::Text => Value::Text(val.to_string()),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            Value::Integer(val) => val.to_string(),
            Value::Float(val) => val.to_string(),
            Value::Boolean(val) => val.to_string(),
            Value::Text(val) => val.to_string(),
        }
    }

    pub fn and(&self, other: &Value) -> Value {
        match self {
            Value::Boolean(l) => match other {
                Value::Boolean(r) => Value::Boolean(*l && *r),
                _ => panic!("Type mismatch"),
            },
            _ => panic!("Type not supported for and"),
        }
    }

    pub fn or(&self, other: &Value) -> Value {
        match self {
            Value::Boolean(l) => match other {
                Value::Boolean(r) => Value::Boolean(*l || *r),
                _ => panic!("Type mismatch"),
            },
            _ => panic!("Type not supported for or"),
        }
    }

    pub fn eq(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Boolean(*l == *r),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Float(r) => Value::Boolean(*l == *r),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(l) => match other {
                Value::Boolean(r) => Value::Boolean(*l == *r),
                _ => panic!("Type mismatch"),
            },
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l == *r),
                _ => panic!("Type mismatch"),
            },
        }
    }

    pub fn neq(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Boolean(*l != *r),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Float(r) => Value::Boolean(*l != *r),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(l) => match other {
                Value::Boolean(r) => Value::Boolean(*l != *r),
                _ => panic!("Type mismatch"),
            },
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l != *r),
                _ => panic!("Type mismatch"),
            },
        }
    }

    pub fn gt(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Boolean(*l > *r),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Float(r) => Value::Boolean(*l > *r),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type mismatch"),
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l > *r),
                _ => panic!("Type mismatch"),
            },
        }
    }

    pub fn gte(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Boolean(*l >= *r),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Float(r) => Value::Boolean(*l >= *r),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type mismatch"),
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l >= *r),
                _ => panic!("Type mismatch"),
            },
        }
    }

    pub fn lt(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Boolean(*l < *r),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Float(r) => Value::Boolean(*l < *r),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type mismatch"),
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l < *r),
                _ => panic!("Type mismatch"),
            },
        }
    }

    pub fn lte(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Boolean(*l <= *r),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Float(r) => Value::Boolean(*l <= *r),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type mismatch"),
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l <= *r),
                _ => panic!("Type mismatch"),
            },
        }
    }

    pub fn add(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Integer(*l + *r),
                Value::Float(r) => Value::Float(*l as f32 + *r),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Integer(r) => Value::Float(*l + *r as f32),
                Value::Float(r) => Value::Float(*l + *r),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type not supported for addition"),
            Value::Text(l) => match other {
                Value::Text(r) => Value::Text(format!("{}{}", l, r)),
                _ => panic!("Type mismatch"),
            },
        }
    }

    pub fn sub(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Integer(*l - *r),
                Value::Float(r) => Value::Float(*l as f32 - *r),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Integer(r) => Value::Float(*l - *r as f32),
                Value::Float(r) => Value::Float(*l - *r),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type not supported for subtraction"),
            Value::Text(_) => panic!("Type not supported for subtraction"),
        }
    }

    pub fn mul(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Integer(*l * *r),
                Value::Float(r) => Value::Float(*l as f32 * *r),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Integer(r) => Value::Float(*l * *r as f32),
                Value::Float(r) => Value::Float(*l * *r),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type not supported for multiplication"),
            Value::Text(_) => panic!("Type not supported for multiplication"),
        }
    }

    pub fn div(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Integer(*l / *r),
                Value::Float(r) => Value::Float(*l as f32 / *r),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Integer(r) => Value::Float(*l / *r as f32),
                Value::Float(r) => Value::Float(*l / *r),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type not supported for division"),
            Value::Text(_) => panic!("Type not supported for division"),
        }
    }

    pub fn modulo(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Integer(*l % *r),
                Value::Float(r) => Value::Float(*l as f32 % *r),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Integer(r) => Value::Float(*l % *r as f32),
                Value::Float(r) => Value::Float(*l % *r),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type not supported for modulo"),
            Value::Text(_) => panic!("Type not supported for modulo"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DataType;

    #[test]
    fn test_get_type_from_string_val() {
        assert_eq!(DataType::get_type_from_string_val("1"), DataType::Integer);
        assert_eq!(DataType::get_type_from_string_val("1.0"), DataType::Float);
        assert_eq!(
            DataType::get_type_from_string_val("true"),
            DataType::Boolean
        );
        assert_eq!(
            DataType::get_type_from_string_val("false"),
            DataType::Boolean
        );
        assert_eq!(DataType::get_type_from_string_val("hello"), DataType::Text);
    }

    #[test]
    fn test_get_value_from_string_val() {
        assert_eq!(
            super::Value::get_value_from_string_val("1", &DataType::Integer),
            super::Value::Integer(1)
        );
        assert_eq!(
            super::Value::get_value_from_string_val("1.0", &DataType::Float),
            super::Value::Float(1.0)
        );
        assert_eq!(
            super::Value::get_value_from_string_val("true", &DataType::Boolean),
            super::Value::Boolean(true)
        );
        assert_eq!(
            super::Value::get_value_from_string_val("false", &DataType::Boolean),
            super::Value::Boolean(false)
        );
        assert_eq!(
            super::Value::get_value_from_string_val("hello", &DataType::Text),
            super::Value::Text("hello".to_string())
        );
    }
}
