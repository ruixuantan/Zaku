use super::types::DataType;
use crate::error::ZakuError;

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    name: String,
    datatype: DataType,
}

impl Field {
    pub fn new(name: String, datatype: DataType) -> Field {
        Field { name, datatype }
    }

    pub fn datatype(&self) -> &DataType {
        &self.datatype
    }

    pub fn set_datatype(&mut self, datatype: DataType) {
        self.datatype = datatype;
    }
}

#[derive(Clone)]
pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Schema {
        Schema { fields }
    }

    pub fn get_field(&self, field: &String) -> Result<&Field, ZakuError> {
        self.fields
            .iter()
            .find(|f| &f.name == field)
            .ok_or(ZakuError::new("Field not found".to_string()))
    }

    pub fn get_index(&self, field: &String) -> Result<usize, ZakuError> {
        self.fields
            .iter()
            .position(|f| &f.name == field)
            .ok_or(ZakuError::new("Field not found".to_string()))
    }

    pub fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    pub fn get_datatype(&self, field: &String) -> Result<&DataType, ZakuError> {
        self.fields
            .iter()
            .find(|f| &f.name == field)
            .map(|f| &f.datatype)
            .ok_or(ZakuError::new("Field not found".to_string()))
    }

    pub fn get_datatype_from_index(&self, index: &usize) -> Result<&DataType, ZakuError> {
        if index >= &self.fields.len() {
            return Err(ZakuError::new("Index out of bounds".to_string()));
        }
        Ok(&self.fields[*index].datatype)
    }

    pub fn select(&self, fields: &Vec<String>) -> Schema {
        let mut selected_fields = Vec::new();
        fields.iter().for_each(|f| {
            // Ignore fields that don't exist
            if let Ok(field) = self.get_field(f) {
                selected_fields.push(field.clone());
            }
        });
        Schema::new(selected_fields)
    }
}

#[cfg(test)]
mod test {
    use super::Field;
    use super::Schema;
    use crate::datatypes::types::DataType;

    #[test]
    fn test_get_datatype_from_index() {
        let fields = vec![
            Field::new("id".to_string(), DataType::Integer),
            Field::new("name".to_string(), DataType::Text),
            Field::new("age".to_string(), DataType::Integer),
            Field::new("weight".to_string(), DataType::Float),
        ];
        let schema = Schema::new(fields);
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
