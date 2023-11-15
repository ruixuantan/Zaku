use csv::Writer;

use crate::{
    datatypes::{column_vector::Vector, record_batch::RecordBatch, schema::Schema},
    error::ZakuError,
};

#[derive(Debug, Clone, PartialEq)]
pub struct Datasink {
    schema: Schema,
    data: Vec<RecordBatch>,
}

impl Datasink {
    pub fn new(data: Vec<RecordBatch>) -> Datasink {
        Datasink {
            schema: data[0].schema().clone(),
            data,
        }
    }

    pub fn num_batches(&self) -> usize {
        self.data.len()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn data(&self) -> &Vec<RecordBatch> {
        &self.data
    }

    pub fn iter(&self) -> DatasinkIterator {
        DatasinkIterator { ds: self, index: 0 }
    }

    pub fn to_csv(&self, path: &String) -> Result<(), ZakuError> {
        let mut file = Writer::from_path(path)?;
        file.write_record(self.schema.as_header())?;

        for rb in &self.data {
            (0..rb.row_count()).for_each(|i| {
                let row = rb
                    .iter()
                    .map(|col| col.get_value(&i).to_string())
                    .collect::<Vec<String>>();
                file.write_record(row).unwrap();
            });
            file.flush()?;
        }

        Ok(())
    }
}

pub struct DatasinkIterator<'a> {
    ds: &'a Datasink,
    index: usize,
}

impl<'a> Iterator for DatasinkIterator<'a> {
    type Item = &'a RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.ds.data.len() {
            None
        } else {
            let rb = &self.ds.data[self.index];
            self.index += 1;
            Some(rb)
        }
    }
}
