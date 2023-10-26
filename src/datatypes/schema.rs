use super::types::DataType;
use crate::error::ZakuError;
use std::collections::HashMap;

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

    pub fn get_datatype_from_index(&self, index: &usize) -> Result<&DataType, ZakuError> {
        if index >= &self.fields.len() {
            return Err(ZakuError::new("Index out of bounds".to_string()));
        }
        Ok(&self.info[&self.fields[*index]])
    }
}

#[cfg(test)]
mod test {
    use super::Schema;
    use crate::datatypes::types::DataType;
    use std::collections::HashMap;

    #[test]
    fn test_get_datatype_from_index() {
        let fields = vec![
            "id".to_string(),
            "name".to_string(),
            "age".to_string(),
            "weight".to_string(),
        ];
        let mut info = HashMap::new();
        info.insert("id".to_string(), DataType::Integer);
        info.insert("name".to_string(), DataType::Text);
        info.insert("age".to_string(), DataType::Integer);
        info.insert("weight".to_string(), DataType::Float);
        let schema = Schema::new(fields, info);
        assert_eq!(
            schema.get_datatype_from_index(&0).unwrap(),
            &DataType::Integer
        );
        assert_eq!(schema.get_datatype_from_index(&1).unwrap(), &DataType::Text);
        assert_eq!(
            schema.get_datatype_from_index(&2).unwrap(),
            &DataType::Integer
        );
        assert_eq!(
            schema.get_datatype_from_index(&3).unwrap(),
            &DataType::Float
        );
    }
}
