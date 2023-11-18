use csv::Writer;

use crate::{
    datatypes::{column_vector::Vector, record_batch::RecordBatch, schema::Schema},
    error::ZakuError,
    physical_plans::physical_plan::PhysicalPlans,
};
use futures_async_stream::{for_await, try_stream};

pub struct Datasink {
    schema: Schema,
    input: PhysicalPlans,
}

impl Datasink {
    pub fn new(schema: Schema, input: PhysicalPlans) -> Datasink {
        Datasink { schema, input }
    }

    pub async fn materialize(&self) -> Result<Vec<RecordBatch>, ZakuError> {
        let mut data = vec![];
        #[for_await]
        for rb in self.input.execute() {
            data.push(rb?);
        }
        Ok(data)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    #[try_stream(ok = RecordBatch, error = ZakuError)]
    pub async fn iter(&self) {
        #[for_await]
        for rb in self.input.execute() {
            yield rb?
        }
    }

    pub async fn to_csv(&self, path: &String) -> Result<(), ZakuError> {
        let mut file = Writer::from_path(path)?;
        file.write_record(self.schema.as_header())?;

        #[for_await]
        for res in self.input.execute() {
            let rb = res?;
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
