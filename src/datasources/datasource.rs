use crate::{
    datatypes::{
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
    data: Vec<RecordBatch>,
}

impl Datasource {
    pub fn new(path: String, schema: Schema, data: Vec<RecordBatch>) -> Datasource {
        Datasource { path, schema, data }
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

    pub fn get_data(&self) -> &Vec<RecordBatch> {
        &self.data
    }

    pub fn scan(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        Box::new(DatasourceIterator::new(&self.data.iter()))
    }

    fn get_csv_schema(path: &str) -> Result<Schema, ZakuError> {
        let mut rdr = csv::Reader::from_path(path)?;

        let mut fields: Vec<Field> = rdr
            .headers()?
            .iter()
            .map(|h| Field::new(h.to_string(), DataType::default()))
            .collect();

        rdr.records().take(1).try_for_each(|r| {
            r?.iter().enumerate().for_each(|(i, field)| {
                let datatype = DataType::get_type_from_string_val(field);
                fields[i].set_datatype(datatype);
            });
            Ok::<(), ZakuError>(())
        })?;
        Ok(Schema::new(fields))
    }

    fn load_csv_data(path: &str, schema: Schema) -> Result<Vec<RecordBatch>, ZakuError> {
        let mut rdr = csv::Reader::from_path(path)?;
        let schema_len = schema.fields().len();
        let mut cols: Vec<Vec<Value>> = (0..schema_len).map(|_| Vec::new()).collect();

        for record in rdr.records() {
            let r = record?;
            for i in 0..schema.fields().len() {
                let datatype = schema.get_datatype_from_index(&i)?;
                let val = Value::get_value_from_string_val(&r[i], datatype);
                cols[i].push(val);
            }
        }
        Ok(RecordBatch::to_record_batch(cols, &schema))
    }
}

pub struct DatasourceIterator<'a> {
    data: std::slice::Iter<'a, RecordBatch>,
}

impl<'a> DatasourceIterator<'a> {
    pub fn new(data: &std::slice::Iter<'a, RecordBatch>) -> DatasourceIterator<'a> {
        DatasourceIterator { data: data.clone() }
    }
}

impl<'a> Iterator for DatasourceIterator<'a> {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        self.data.next().cloned()
    }
}

#[cfg(test)]
mod test {
    use std::{path::Path, sync::Arc, vec};

    use crate::datatypes::{
        column_vector::{ColumnVector, Vectors},
        schema::Field,
        types::{DataType, Value},
    };

    use super::Datasource;

    fn csv_test_file() -> String {
        Path::new("resources")
            .join("test.csv")
            .to_str()
            .expect("test.csv file should exist")
            .to_string()
    }

    #[test]
    fn test_get_csv_schema() {
        let schema = Datasource::get_csv_schema(&csv_test_file()).unwrap();
        assert_eq!(
            schema.fields(),
            &vec![
                Field::new("id".to_string(), DataType::Number),
                Field::new("product_name".to_string(), DataType::Text),
                Field::new("is_available".to_string(), DataType::Boolean),
                Field::new("price".to_string(), DataType::Number),
                Field::new("quantity".to_string(), DataType::Number),
                Field::new("updated_on".to_string(), DataType::Date),
            ]
        );
    }

    #[test]
    fn test_load_csv_data() {
        let record_batch = &Datasource::load_csv_data(
            &csv_test_file(),
            Datasource::get_csv_schema(&csv_test_file()).unwrap(),
        )
        .unwrap()[0];
        assert_eq!(record_batch.row_count(), 5);
        assert_eq!(record_batch.column_count(), 6);

        let cols = record_batch.columns();
        let ex_cols = vec![
            Arc::new(Vectors::ColumnVector(ColumnVector::new(
                DataType::Number,
                ["1", "2", "3", "4", "5"]
                    .iter()
                    .map(|i| Value::number(i))
                    .collect(),
            ))),
            Arc::new(Vectors::ColumnVector(ColumnVector::new(
                DataType::Text,
                [
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
            Arc::new(Vectors::ColumnVector(ColumnVector::new(
                DataType::Boolean,
                [true, true, true, false, true]
                    .iter()
                    .map(|b| Value::Boolean(*b))
                    .collect(),
            ))),
            Arc::new(Vectors::ColumnVector(ColumnVector::new(
                DataType::Number,
                [5.00, 10.00, 15.50, 2.00, 20.00]
                    .iter()
                    .map(|f| Value::number(f.to_string().as_str()))
                    .collect(),
            ))),
            Arc::new(Vectors::ColumnVector(ColumnVector::new(
                DataType::Number,
                [100, 50, 25, 0, 10]
                    .iter()
                    .map(|i| Value::number(i.to_string().as_str()))
                    .collect(),
            ))),
            Arc::new(Vectors::ColumnVector(ColumnVector::new(
                DataType::Date,
                [
                    "2023-06-06",
                    "2023-01-01",
                    "2023-04-04",
                    "2023-02-02",
                    "2023-03-03",
                ]
                .iter()
                .map(|i| Value::date(i))
                .collect(),
            ))),
        ];
        assert_eq!(cols, &ex_cols);
    }
}
