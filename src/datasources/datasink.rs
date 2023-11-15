use csv::Writer;

use crate::{
    datatypes::{column_vector::Vector, record_batch::RecordBatch, schema::Schema, types::Value},
    error::ZakuError,
};

use super::prettifier::prettify;

#[derive(Debug, Clone, PartialEq)]
pub struct Datasink {
    schema: Schema,
    data: Vec<Vec<Value>>,
}

impl Datasink {
    pub fn new(schema: Schema, data: Vec<Vec<Value>>) -> Datasink {
        Datasink { schema, data }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn data(&self) -> &Vec<Vec<Value>> {
        &self.data
    }

    pub fn row_count(&self) -> usize {
        self.data[0].len()
    }

    pub fn column_count(&self) -> usize {
        self.data.len()
    }

    pub fn from_record_batches(record_batches: Vec<RecordBatch>) -> Datasink {
        let schema = record_batches[0].schema().clone();
        let mut data: Vec<Vec<Value>> = schema.fields().iter().map(|_| vec![]).collect();
        record_batches.into_iter().for_each(|rb| {
            rb.columns().iter().enumerate().for_each(|(i, col)| {
                data[i].extend(col.iter().cloned());
            })
        });
        Datasink::new(schema, data)
    }

    pub fn get(&self, index: &usize) -> Result<Vec<Value>, ZakuError> {
        if index >= &self.column_count() {
            return Err(ZakuError::new("Index out of bounds"));
        }
        Ok(self.data[*index].clone())
    }

    pub fn iter(&self) -> DatasinkIterator {
        DatasinkIterator { ds: self, index: 0 }
    }

    pub fn pretty_print(&self) -> String {
        prettify(self)
    }

    pub fn to_csv(&self, path: &String) -> Result<(), ZakuError> {
        let mut file = Writer::from_path(path)?;
        file.write_record(self.schema.as_header())?;

        (0..self.row_count()).for_each(|i| {
            let row = self
                .iter()
                .map(|col| col[i].to_string())
                .collect::<Vec<String>>();
            file.write_record(row).unwrap();
        });
        file.flush()?;

        Ok(())
    }
}

pub struct DatasinkIterator<'a> {
    ds: &'a Datasink,
    index: usize,
}

impl<'a> Iterator for DatasinkIterator<'a> {
    type Item = &'a Vec<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.ds.column_count() {
            None
        } else {
            let col = &self.ds.data[self.index];
            self.index += 1;
            Some(col)
        }
    }
}
