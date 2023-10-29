use std::sync::Arc;

use crate::{
    datatypes::{
        column_vector::{ColumnVector, Vector},
        record_batch::RecordBatch,
        schema::{Field, Schema},
        types::{DataType, Value},
    },
    error::ZakuError,
};

#[derive(Debug, Clone)]
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
                Arc::new(Vector::ColumnVector(ColumnVector::new(
                    schema
                        .get_datatype_from_index(&i)
                        .expect("Index out of bounds")
                        .clone(),
                    c,
                )))
            })
            .collect();
        Ok(RecordBatch::new(schema, arc_cols))
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, vec};

    use crate::datatypes::{
        column_vector::{ColumnVector, Vector},
        schema::Field,
        types::{DataType, Value},
    };

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

        let cols = record_batch.columns();
        let ex_cols = vec![
            Arc::new(Vector::ColumnVector(ColumnVector::new(
                DataType::Integer,
                vec![1, 2, 3, 4, 5]
                    .iter()
                    .map(|i| Value::Integer(*i))
                    .collect(),
            ))),
            Arc::new(Vector::ColumnVector(ColumnVector::new(
                DataType::Text,
                vec![
                    "toothbrush",
                    "toothpaste",
                    "shampoo",
                    "soap",
                    "shaving cream",
                ]
                .iter()
                .map(|s| Value::Text(s.to_string()))
                .collect(),
            ))),
            Arc::new(Vector::ColumnVector(ColumnVector::new(
                DataType::Boolean,
                vec![true, true, true, false, true]
                    .iter()
                    .map(|b| Value::Boolean(*b))
                    .collect(),
            ))),
            Arc::new(Vector::ColumnVector(ColumnVector::new(
                DataType::Float,
                vec![5.00, 10.00, 15.50, 2.00, 20.00]
                    .iter()
                    .map(|f| Value::Float(*f))
                    .collect(),
            ))),
            Arc::new(Vector::ColumnVector(ColumnVector::new(
                DataType::Integer,
                vec![100, 50, 25, 0, 10]
                    .iter()
                    .map(|i| Value::Integer(*i))
                    .collect(),
            ))),
        ];
        assert_eq!(cols, &ex_cols);
    }
}
