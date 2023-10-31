use std::sync::Arc;

use crate::{
    datasources::datasource::Datasource,
    datatypes::{
        column_vector::{ColumnVector, Vector},
        record_batch::RecordBatch,
        schema::Schema,
        types::Value,
    },
};

use super::physical_expr::PhysicalExpr;

#[derive(Clone)]
pub enum PhysicalPlan {
    Scan(ScanExec),
    Projection(ProjectionExec),
    Filter(FilterExec),
}

impl PhysicalPlan {
    pub fn schema(&self) -> Schema {
        match self {
            PhysicalPlan::Scan(scan) => scan.schema(),
            PhysicalPlan::Projection(projection) => projection.schema(),
            PhysicalPlan::Filter(filter) => filter.schema(),
        }
    }

    pub fn execute(&self) -> RecordBatch {
        match self {
            PhysicalPlan::Scan(scan) => scan.execute(),
            PhysicalPlan::Projection(projection) => projection.execute(),
            PhysicalPlan::Filter(filter) => filter.execute(),
        }
    }

    pub fn children(&self) -> Vec<PhysicalPlan> {
        match self {
            PhysicalPlan::Scan(scan) => scan.children(),
            PhysicalPlan::Projection(projection) => projection.children(),
            PhysicalPlan::Filter(filter) => filter.children(),
        }
    }
}

#[derive(Clone)]
pub struct ScanExec {
    datasource: Datasource,
    projection: Vec<String>,
}

impl ScanExec {
    pub fn new(datasource: Datasource, projection: Vec<String>) -> ScanExec {
        ScanExec {
            datasource,
            projection,
        }
    }

    fn schema(&self) -> Schema {
        self.datasource.schema().select(&self.projection)
    }

    fn execute(&self) -> RecordBatch {
        self.datasource.record_batch().clone()
    }

    fn children(&self) -> Vec<PhysicalPlan> {
        Vec::new()
    }
}

#[derive(Clone)]
pub struct ProjectionExec {
    schema: Schema,
    physical_plan: Box<PhysicalPlan>,
    expr: Vec<PhysicalExpr>,
}

impl ProjectionExec {
    pub fn new(
        schema: Schema,
        physical_plan: PhysicalPlan,
        expr: Vec<PhysicalExpr>,
    ) -> ProjectionExec {
        ProjectionExec {
            schema,
            physical_plan: Box::new(physical_plan),
            expr,
        }
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn execute(&self) -> RecordBatch {
        let record_batch = self.physical_plan.execute();
        let columns = self
            .expr
            .iter()
            .map(|e| e.evaluate(&record_batch))
            .collect();
        RecordBatch::new(self.schema.clone(), columns)
    }

    fn children(&self) -> Vec<PhysicalPlan> {
        vec![*self.physical_plan.clone()]
    }
}

#[derive(Clone)]
pub struct FilterExec {
    schema: Schema,
    physical_plan: Box<PhysicalPlan>,
    expr: PhysicalExpr,
}

impl FilterExec {
    pub fn new(schema: Schema, physical_plan: PhysicalPlan, expr: PhysicalExpr) -> FilterExec {
        FilterExec {
            schema,
            physical_plan: Box::new(physical_plan),
            expr,
        }
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn execute(&self) -> RecordBatch {
        let record_batch = self.physical_plan.execute();
        let eval_col = self.expr.evaluate(&record_batch);

        let cols = record_batch
            .iter()
            .map(|c| {
                Arc::new(Vector::ColumnVector(ColumnVector::new(
                    c.get_type().clone(),
                    c.iter()
                        .enumerate()
                        .filter(|(i, _)| eval_col.get_value(i) == &Value::Boolean(true))
                        .map(|(_, v)| v.clone())
                        .collect(),
                )))
            })
            .collect();

        RecordBatch::new(self.schema.clone(), cols)
    }

    fn children(&self) -> Vec<PhysicalPlan> {
        vec![*self.physical_plan.clone()]
    }
}
