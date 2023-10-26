use std::{collections::HashMap, error::Error};

use crate::datatypes::{record_batch::RecordBatch, schema::Schema, types::DataType};

pub struct Datasource {
    path: String,
    schema: Schema,
    record_batch: RecordBatch,
}

impl Datasource {
    pub fn new(path: String, schema: Schema, record_batch: RecordBatch) -> Datasource {
        Datasource {
            path,
            schema,
            record_batch,
        }
    }

    pub fn from_csv(path: &str) -> Result<Datasource, Box<dyn Error>> {
        let schema = Datasource::get_csv_schema(path)?;
        let record_batch = Datasource::load_csv_data(path, schema.clone())?;
        Ok(Datasource::new(path.to_string(), schema, record_batch))
    }

    fn get_csv_schema(path: &str) -> Result<Schema, Box<dyn Error>> {
        let mut rdr = csv::Reader::from_path(path)?;
        let mut fields = Vec::new();
        let mut info = HashMap::new();

        rdr.headers()?
            .iter()
            .for_each(|h| fields.push(h.to_string()));
        rdr.records().take(1).for_each(|r| {
            r.unwrap().iter().enumerate().for_each(|(i, field)| {
                let datatype = DataType::get_type_from_string_val(field);
                info.insert(fields[i].to_string(), datatype);
            });
        });
        Ok(Schema::new(fields, info))
    }

    fn load_csv_data(path: &str, schema: Schema) -> Result<RecordBatch, Box<dyn Error>> {
        let mut rdr = csv::Reader::from_path(path)?;
        let mut record_batch = RecordBatch::new(schema);
        for result in rdr.records() {
            let record = result?;
            record_batch.insert_row(record.iter().map(|r| r.to_string()).collect())?;
        }
        Ok(record_batch)
    }
}

#[cfg(test)]
mod test {
    use super::Datasource;

    fn csv_test_file() -> String {
        "resources/test.csv".to_string()
    }

    #[test]
    fn test_get_csv_schema() {
        let schema = Datasource::get_csv_schema(&csv_test_file()).unwrap();
        assert_eq!(
            schema.get_fields(),
            &vec!["id", "product_name", "is_available", "price", "quantity"]
        );
        assert_eq!(
            schema.get_datatype(&"id".to_string()),
            &super::DataType::Integer
        );
        assert_eq!(
            schema.get_datatype(&"product_name".to_string()),
            &super::DataType::Text
        );
        assert_eq!(
            schema.get_datatype(&"is_available".to_string()),
            &super::DataType::Boolean
        );
        assert_eq!(
            schema.get_datatype(&"price".to_string()),
            &super::DataType::Float
        );
        assert_eq!(
            schema.get_datatype(&"quantity".to_string()),
            &super::DataType::Integer
        );
    }

    #[test]
    fn test_load_csv_data() {
        let record_batch = Datasource::load_csv_data(
            &csv_test_file(),
            Datasource::get_csv_schema(&csv_test_file()).unwrap(),
        )
        .unwrap();
        assert_eq!(record_batch.row_count(), 5);
        assert_eq!(record_batch.column_count(), 5);
    }
}
