use std::sync::Arc;

use crate::{
    datatypes::{
        column_vector::ColumnVector,
        record_batch::RecordBatch,
        schema::{Field, Schema},
        types::{DataType, Value},
    },
    error::ZakuError,
};

#[derive(Clone)]
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

    pub fn from_csv(path: &str) -> Result<Datasource, ZakuError> {
        let schema = Datasource::get_csv_schema(path)?;
        let record_batch = Datasource::load_csv_data(path, schema.clone())?;
        Ok(Datasource::new(path.to_string(), schema, record_batch))
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn path(&self) -> &String {
        &self.path
    }

    pub fn record_batch(&self) -> &RecordBatch {
        &self.record_batch
    }

    fn get_csv_schema(path: &str) -> Result<Schema, ZakuError> {
        let mut rdr = csv::Reader::from_path(path).map_err(|e| ZakuError::new(e.to_string()))?;
        let mut fields = Vec::new();

        rdr.headers()
            .map_err(|e| ZakuError::new(e.to_string()))?
            .iter()
            .for_each(|h| fields.push(Field::new(h.to_string(), DataType::Boolean)));

        rdr.records().take(1).try_for_each(|r| {
            r.map_err(|e| ZakuError::new(e.to_string()))?
                .iter()
                .enumerate()
                .for_each(|(i, field)| {
                    let datatype = DataType::get_type_from_string_val(field);
                    fields[i].set_datatype(datatype);
                });
            Ok::<(), ZakuError>(())
        })?;
        Ok(Schema::new(fields))
    }

    fn load_csv_data(path: &str, schema: Schema) -> Result<RecordBatch, ZakuError> {
        let mut rdr = csv::Reader::from_path(path).map_err(|e| ZakuError::new(e.to_string()))?;
        let mut cols = Vec::new();
        schema.fields().iter().for_each(|_| cols.push(Vec::new()));
        rdr.records()
            .map(|r| r.map_err(|e| ZakuError::new(e.to_string())))
            .try_for_each(|r| {
                let record = r?;
                record.iter().enumerate().try_for_each(|(i, str_val)| {
                    let datatype = schema.get_datatype_from_index(&i)?;
                    let val = Value::get_value_from_string_val(str_val, datatype);
                    cols[i].push(val);
                    Ok::<(), ZakuError>(())
                })?;
                Ok::<(), ZakuError>(())
            })?;
        let arc_cols = cols
            .into_iter()
            .enumerate()
            .map(|(i, c)| {
                Arc::new(ColumnVector::new(
                    schema
                        .get_datatype_from_index(&i)
                        .expect("Index out of bounds")
                        .clone(),
                    c,
                ))
            })
            .collect();
        Ok(RecordBatch::new(schema, arc_cols))
    }
}

#[cfg(test)]
mod test {
    use crate::datatypes::{schema::Field, types::DataType};

    use super::Datasource;

    fn csv_test_file() -> String {
        "resources/test.csv".to_string()
    }

    #[test]
    fn test_get_csv_schema() {
        let schema = Datasource::get_csv_schema(&csv_test_file()).unwrap();
        assert_eq!(
            schema.fields(),
            &vec![
                Field::new("id".to_string(), DataType::Integer),
                Field::new("product_name".to_string(), DataType::Text),
                Field::new("is_available".to_string(), DataType::Boolean),
                Field::new("price".to_string(), DataType::Float),
                Field::new("quantity".to_string(), DataType::Integer)
            ]
        );
    }

    #[test]
    fn test_load_csv_data() {
        // TODO: Test each record, check if the values are correct
        let record_batch = Datasource::load_csv_data(
            &csv_test_file(),
            Datasource::get_csv_schema(&csv_test_file()).unwrap(),
        )
        .unwrap();
        assert_eq!(record_batch.row_count(), 5);
        assert_eq!(record_batch.column_count(), 5);
    }
}
