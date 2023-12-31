use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

use super::{
    column_vector::{ColumnVector, Vectors},
    record_batch::RecordBatch,
    types::{DataType, Value},
};
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

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn datatype(&self) -> &DataType {
        &self.datatype
    }

    pub fn set_datatype(&mut self, datatype: DataType) {
        self.datatype = datatype;
    }
}

#[derive(Debug, Clone, PartialEq)]
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
            .ok_or(ZakuError::new(
                format!("Field '{}' not found", field).as_str(),
            ))
    }

    pub fn get_field_by_index(&self, index: &usize) -> Result<&Field, ZakuError> {
        if index >= &self.fields.len() {
            return Err(ZakuError::new(
                format!("Index {} out of bounds", index).as_str(),
            ));
        }
        Ok(&self.fields[*index])
    }

    pub fn get_index(&self, field: &String) -> Result<usize, ZakuError> {
        self.fields
            .iter()
            .position(|f| &f.name == field)
            .ok_or(ZakuError::new(
                format!("Field '{}' not found", field).as_str(),
            ))
    }

    pub fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    pub fn get_datatype(&self, field: &String) -> Result<&DataType, ZakuError> {
        self.fields
            .iter()
            .find(|f| &f.name == field)
            .map(|f| &f.datatype)
            .ok_or(ZakuError::new(
                format!("Field '{}' not found", field).as_str(),
            ))
    }

    pub fn get_datatype_from_index(&self, index: &usize) -> Result<&DataType, ZakuError> {
        if index >= &self.fields.len() {
            return Err(ZakuError::new("Index out of bounds"));
        }
        Ok(&self.fields[*index].datatype)
    }

    pub fn select(&self, fields: &[String]) -> Schema {
        let selected_fields = fields
            .iter()
            .map(|f| self.get_field(f))
            .filter(|f| f.is_ok()) // Ignore fields that don't exist
            .map(|f| f.expect("Field should be returned as an Ok result").clone())
            .collect();
        Schema::new(selected_fields)
    }

    pub fn as_header(&self) -> Vec<String> {
        self.fields.iter().map(|f| f.name().clone()).collect()
    }

    pub fn to_record_batch(&self) -> RecordBatch {
        let new_schema = Schema::new(vec![
            Field::new("Column".to_string(), DataType::Text),
            Field::new("Datatype".to_string(), DataType::Text),
        ]);
        let mut cols = vec![];
        let column = Arc::new(Vectors::ColumnVector(ColumnVector::new(
            DataType::Text,
            self.fields
                .iter()
                .map(|f| Value::Text(f.name().clone()))
                .collect(),
        )));
        cols.push(column);
        let datatypes = Arc::new(Vectors::ColumnVector(ColumnVector::new(
            DataType::Text,
            self.fields
                .iter()
                .map(|f| Value::Text(f.datatype().to_string()))
                .collect(),
        )));
        cols.push(datatypes);
        RecordBatch::new(new_schema, cols)
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut schema = vec![];
        for field in &self.fields {
            schema.push(format!("{}({})", field.name(), field.datatype()));
        }
        write!(f, "{}", schema.join(" | "))
    }
}

#[cfg(test)]
mod test {
    use super::Field;
    use super::Schema;
    use crate::datatypes::types::DataType;

    fn get_schema() -> Schema {
        let fields = vec![
            Field::new("id".to_string(), DataType::Number),
            Field::new("name".to_string(), DataType::Text),
            Field::new("age".to_string(), DataType::Number),
            Field::new("weight".to_string(), DataType::Number),
        ];
        Schema::new(fields)
    }

    #[test]
    fn test_get_datatype_from_index() {
        let schema = get_schema();
        assert_eq!(
            schema.get_datatype_from_index(&0).unwrap(),
            &DataType::Number
        );
        assert_eq!(schema.get_datatype_from_index(&1).unwrap(), &DataType::Text);
        assert_eq!(
            schema.get_datatype_from_index(&2).unwrap(),
            &DataType::Number
        );
        assert_eq!(
            schema.get_datatype_from_index(&3).unwrap(),
            &DataType::Number
        );
    }

    #[test]
    fn test_select() {
        let schema = get_schema();
        let selected_schema = schema.select(&["id".to_string(), "name".to_string()]);
        let ex_fields = vec![
            Field::new("id".to_string(), DataType::Number),
            Field::new("name".to_string(), DataType::Text),
        ];
        let ex_schema = Schema::new(ex_fields);
        assert_eq!(selected_schema, ex_schema);
    }
}
