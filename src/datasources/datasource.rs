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
        let mut first = true;
        for result in rdr.records() {
            let record = result?;
            if first {
                for field in record.iter() {
                    fields.push(field.to_string());
                }
                first = false;
            } else {
                for (i, field) in record.iter().enumerate() {
                    let datatype = DataType::get_type_from_string_val(field);
                    info.insert(fields[i].to_string(), datatype);
                }
                break;
            }
        }
        Ok(Schema::new(fields, info))
    }

    fn load_csv_data(path: &str, schema: Schema) -> Result<RecordBatch, Box<dyn Error>> {
        let mut rdr = csv::Reader::from_path(path)?;
        let mut record_batch = RecordBatch::new(schema);
        for result in rdr.records() {
            let record = result?;
            record_batch.insert_row(record.iter().map(|r| r.to_string()).collect());
        }
        Ok(record_batch)
    }
}
