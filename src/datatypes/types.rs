#[derive(Clone, Debug, PartialEq)]
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
