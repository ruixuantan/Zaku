use std::{
    fmt::{Display, Formatter},
    hash::Hash,
};

use bigdecimal::BigDecimal;
use chrono::NaiveDate;
use std::str::FromStr;

use crate::ZakuError;

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum DataType {
    #[default]
    Text,
    Boolean,
    Number,
    Date,
}

impl DataType {
    pub fn get_type_from_string_val(val: &str) -> DataType {
        if parse_iso_date_from_str(val).is_ok() {
            return DataType::Date;
        }
        if BigDecimal::from_str(val).is_ok() {
            return DataType::Number;
        }
        if val.parse::<bool>().is_ok() {
            return DataType::Boolean;
        }
        DataType::Text
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Text => write!(f, "text"),
            DataType::Boolean => write!(f, "boolean"),
            DataType::Number => write!(f, "number"),
            DataType::Date => write!(f, "date"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub enum Value {
    Number(BigDecimal),
    Text(String),
    Boolean(bool),
    Date(NaiveDate),
    Null,
}

pub fn parse_iso_date_from_str(s: &str) -> Result<NaiveDate, ZakuError> {
    Ok(NaiveDate::parse_from_str(s, "%Y-%m-%d")?)
}

impl Value {
    pub fn number(val: &str) -> Value {
        Value::Number(BigDecimal::from_str(val).expect("Val should be a numeric value"))
    }

    pub fn date(val: &str) -> Value {
        Value::Date(parse_iso_date_from_str(val).expect("Val should be a date value"))
    }

    pub fn get_value_from_string_val(val: &str, datatype: &DataType) -> Value {
        if val.is_empty() {
            return Value::Null;
        }
        match datatype {
            DataType::Number => Value::Number(
                BigDecimal::from_str(val.replace(',', "").as_str())
                    .unwrap_or_else(|_| panic!("Expected float, got {val}")),
            ),
            DataType::Date => Value::Date(
                parse_iso_date_from_str(val)
                    .unwrap_or_else(|_| panic!("Expected date, got '{val}'")),
            ),
            DataType::Boolean => Value::Boolean(
                val.parse::<bool>()
                    .unwrap_or_else(|_| panic!("Expected boolean, got '{val}'")),
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
            Value::Number(l) => match other {
                Value::Number(r) => Value::Boolean(*l == *r),
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
            Value::Date(l) => match other {
                Value::Date(r) => Value::Boolean(*l == *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Boolean(false),
        }
    }

    pub fn neq(&self, other: &Value) -> Value {
        match self {
            Value::Number(l) => match other {
                Value::Number(r) => Value::Boolean(*l != *r),
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
            Value::Date(l) => match other {
                Value::Date(r) => Value::Boolean(*l != *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Boolean(false),
        }
    }

    pub fn gt(&self, other: &Value) -> Value {
        match self {
            Value::Number(l) => match other {
                Value::Number(r) => Value::Boolean(*l > *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type mismatch"),
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l > *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Date(l) => match other {
                Value::Date(r) => Value::Boolean(*l > *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Boolean(false),
        }
    }

    pub fn gte(&self, other: &Value) -> Value {
        match self {
            Value::Number(l) => match other {
                Value::Number(r) => Value::Boolean(*l >= *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type mismatch"),
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l >= *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Date(l) => match other {
                Value::Date(r) => Value::Boolean(*l >= *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Boolean(false),
        }
    }

    pub fn lt(&self, other: &Value) -> Value {
        match self {
            Value::Number(l) => match other {
                Value::Number(r) => Value::Boolean(*l < *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type mismatch"),
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l < *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Date(l) => match other {
                Value::Date(r) => Value::Boolean(*l < *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Boolean(false),
        }
    }

    pub fn lte(&self, other: &Value) -> Value {
        match self {
            Value::Number(l) => match other {
                Value::Number(r) => Value::Boolean(*l <= *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Boolean(_) => panic!("Type mismatch"),
            Value::Text(l) => match other {
                Value::Text(r) => Value::Boolean(*l <= *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Date(l) => match other {
                Value::Date(r) => Value::Boolean(*l <= *r),
                Value::Null => Value::Boolean(false),
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Boolean(false),
        }
    }

    pub fn add(&self, other: &Value) -> Value {
        match self {
            Value::Number(l) => match other {
                Value::Number(r) => Value::Number(l + r),
                Value::Null => Value::Null,
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Null,
            _ => panic!("Type not supported for addition"),
        }
    }

    pub fn sub(&self, other: &Value) -> Value {
        match self {
            Value::Number(l) => match other {
                Value::Number(r) => Value::Number(l - r),
                Value::Null => Value::Null,
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Null,
            _ => panic!("Type not supported for subtraction"),
        }
    }

    pub fn mul(&self, other: &Value) -> Value {
        match self {
            Value::Number(l) => match other {
                Value::Number(r) => Value::Number(l * r),
                Value::Null => Value::Null,
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Null,
            _ => panic!("Type not supported for multiplication"),
        }
    }

    pub fn div(&self, other: &Value) -> Value {
        match self {
            Value::Number(l) => match other {
                Value::Number(r) => Value::Number(l / r),
                Value::Null => Value::Null,
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Null,
            _ => panic!("Type not supported for division"),
        }
    }

    pub fn modulo(&self, other: &Value) -> Value {
        match self {
            Value::Number(l) => match other {
                Value::Number(r) => Value::Number(l % r),
                Value::Null => Value::Null,
                _ => panic!("Type mismatch"),
            },
            Value::Null => Value::Null,
            _ => panic!("Type not supported for modulo"),
        }
    }

    pub fn maximum(&self, other: &Value) -> Value {
        match self {
            Value::Number(l) => match other {
                Value::Number(r) => Value::Number(l.max(r).clone()),
                Value::Null => self.clone(),
                _ => panic!("Type mismatch"),
            },
            Value::Null => match other {
                Value::Null | Value::Number(_) => other.clone(),
                _ => panic!("Type not supported for max"),
            },
            Value::Date(l) => match other {
                Value::Date(r) => Value::Date(*l.max(r)),
                Value::Null => self.clone(),
                _ => panic!("Type mismatch"),
            },
            _ => panic!("Type not supported for max"),
        }
    }

    pub fn minimum(&self, other: &Value) -> Value {
        match self {
            Value::Number(l) => match other {
                Value::Number(r) => Value::Number(l.min(r).clone()),
                Value::Null => self.clone(),
                _ => panic!("Type mismatch"),
            },
            Value::Null => match other {
                Value::Null | Value::Number(_) => other.clone(),
                _ => panic!("Type not supported for max"),
            },
            Value::Date(l) => match other {
                Value::Date(r) => Value::Date(*l.min(r)),
                Value::Null => self.clone(),
                _ => panic!("Type mismatch"),
            },
            _ => panic!("Type not supported for min"),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Number(val) => write!(f, "{}", val),
            Value::Boolean(val) => write!(f, "{}", val),
            Value::Text(val) => write!(f, "{}", val),
            Value::Date(val) => write!(f, "{}", val),
            Value::Null => write!(f, ""),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bigdecimal::BigDecimal;

    use super::DataType;

    #[test]
    fn test_get_type_from_string_val() {
        assert_eq!(DataType::get_type_from_string_val("1"), DataType::Number);
        assert_eq!(DataType::get_type_from_string_val("1.0"), DataType::Number);
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
            super::Value::get_value_from_string_val("1", &DataType::Number),
            super::Value::Number(BigDecimal::from_str("1").unwrap())
        );
        assert_eq!(
            super::Value::get_value_from_string_val("1.0", &DataType::Number),
            super::Value::Number(BigDecimal::from_str("1").unwrap())
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
