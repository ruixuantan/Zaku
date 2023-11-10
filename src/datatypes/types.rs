use std::{
    fmt::{Display, Formatter},
    hash::Hash,
};

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum DataType {
    Integer,
    Float,
    #[default]
    Text,
    Boolean,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Integer(i32),
    Float(f32),
    Text(String),
    Boolean(bool),
    Null,
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
        if val.is_empty() {
            return Value::Null;
        }
        match datatype {
            DataType::Integer => Value::Integer(
                val.parse::<i32>()
                    .unwrap_or_else(|_| panic!("Expected integer, got {val}")),
            ),
            DataType::Float => Value::Float(
                val.parse::<f32>()
                    .unwrap_or_else(|_| panic!("Expected float, got {val}")),
            ),
            DataType::Boolean => Value::Boolean(
                val.parse::<bool>()
                    .unwrap_or_else(|_| panic!("Expected boolean, got {val}")),
            ),
            DataType::Text => Value::Text(val.to_string()),
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
                Value::Float(r) => Value::Boolean(*l as f32 == *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Float(r) => Value::Boolean(*l == *r),
                Value::Integer(r) => Value::Boolean(*l == *r as f32),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(l) => match other {
                Value::Boolean(r) => Value::Boolean(*l == *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l == *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Boolean(false),
        }
    }

    pub fn neq(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Boolean(*l != *r),
                Value::Float(r) => Value::Boolean(*l as f32 != *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Float(r) => Value::Boolean(*l != *r),
                Value::Integer(r) => Value::Boolean(*l != *r as f32),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(l) => match other {
                Value::Boolean(r) => Value::Boolean(*l != *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l != *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Boolean(false),
        }
    }

    pub fn gt(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Boolean(*l > *r),
                Value::Float(r) => Value::Boolean(*l as f32 > *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Float(r) => Value::Boolean(*l > *r),
                Value::Integer(r) => Value::Boolean(*l > *r as f32),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type mismatch"),
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l > *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Boolean(false),
        }
    }

    pub fn gte(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Boolean(*l >= *r),
                Value::Float(r) => Value::Boolean(*l as f32 >= *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Float(r) => Value::Boolean(*l >= *r),
                Value::Integer(r) => Value::Boolean(*l >= *r as f32),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type mismatch"),
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l >= *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Boolean(false),
        }
    }

    pub fn lt(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Boolean(*l < *r),
                Value::Float(r) => Value::Boolean(*r > *l as f32),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Float(r) => Value::Boolean(*l < *r),
                Value::Integer(r) => Value::Boolean(*l < *r as f32),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type mismatch"),
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l < *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Boolean(false),
        }
    }

    pub fn lte(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Boolean(*l <= *r),
                Value::Float(r) => Value::Boolean(*l as f32 <= *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Float(r) => Value::Boolean(*l <= *r),
                Value::Integer(r) => Value::Boolean(*l <= *r as f32),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type mismatch"),
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l <= *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Boolean(false),
        }
    }

    pub fn add(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Integer(*l + *r),
                Value::Float(r) => Value::Float(*l as f32 + *r),
                Value::Null => Value::Null,
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Integer(r) => Value::Float(*l + *r as f32),
                Value::Float(r) => Value::Float(*l + *r),
                Value::Null => Value::Null,
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Null,
            _ => panic!("Type not supported for addition"),
        }
    }

    pub fn sub(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Integer(*l - *r),
                Value::Float(r) => Value::Float(*l as f32 - *r),
                Value::Null => Value::Null,
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Integer(r) => Value::Float(*l - *r as f32),
                Value::Float(r) => Value::Float(*l - *r),
                Value::Null => Value::Null,
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Null,
            _ => panic!("Type not supported for subtraction"),
        }
    }

    pub fn mul(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Integer(*l * *r),
                Value::Float(r) => Value::Float(*l as f32 * *r),
                Value::Null => Value::Null,

                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Integer(r) => Value::Float(*l * *r as f32),
                Value::Float(r) => Value::Float(*l * *r),
                Value::Null => Value::Null,

                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Null,
            _ => panic!("Type not supported for multiplication"),
        }
    }

    pub fn div(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Integer(*l / *r),
                Value::Float(r) => Value::Float(*l as f32 / *r),
                Value::Null => Value::Null,
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Integer(r) => Value::Float(*l / *r as f32),
                Value::Float(r) => Value::Float(*l / *r),
                Value::Null => Value::Null,
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Null,
            _ => panic!("Type not supported for division"),
        }
    }

    pub fn modulo(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Integer(*l % *r),
                Value::Float(r) => Value::Float(*l as f32 % *r),
                Value::Null => Value::Null,
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Integer(r) => Value::Float(*l % *r as f32),
                Value::Float(r) => Value::Float(*l % *r),
                Value::Null => Value::Null,
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Null,
            _ => panic!("Type not supported for modulo"),
        }
    }

    pub fn maximum(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Integer(*l.max(r)),
                Value::Null => self.clone(),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Float(r) => Value::Float(l.max(*r)),
                Value::Null => self.clone(),
                _ => panic!("Type mismatch"),
            },
            Value::Null => match other {
                Value::Null | Value::Float(_) | Value::Integer(_) => other.clone(),
                _ => panic!("Type not supported for max"),
            },
            _ => panic!("Type not supported for max"),
        }
    }

    pub fn minimum(&self, other: &Value) -> Value {
        match self {
            Value::Integer(l) => match other {
                Value::Integer(r) => Value::Integer(*l.min(r)),
                Value::Null => self.clone(),
                _ => panic!("Type mismatch"),
            },
            Value::Float(l) => match other {
                Value::Float(r) => Value::Float(l.min(*r)),
                Value::Null => self.clone(),
                _ => panic!("Type mismatch"),
            },
            Value::Null => match other {
                Value::Null | Value::Float(_) | Value::Integer(_) => other.clone(),
                _ => panic!("Type not supported for max"),
            },
            _ => panic!("Type not supported for min"),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Integer(val) => write!(f, "{}", val),
            Value::Float(val) => write!(f, "{}", val),
            Value::Boolean(val) => write!(f, "{}", val),
            Value::Text(val) => write!(f, "{}", val),
            Value::Null => write!(f, ""),
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Integer(val) => val.hash(state),
            Value::Float(val) => val.to_bits().hash(state),
            Value::Boolean(val) => val.hash(state),
            Value::Text(val) => val.hash(state),
            Value::Null => 0.hash(state),
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
