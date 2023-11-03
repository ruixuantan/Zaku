use std::sync::Arc;

use enum_dispatch::enum_dispatch;

use crate::{
    datasources::datasource::Datasource,
    datatypes::{
        column_vector::{ColumnVector, Vector, Vectors},
        record_batch::RecordBatch,
        schema::Schema,
        types::Value,
    },
};

use super::physical_expr::{PhysicalExpr, PhysicalExprs};

#[enum_dispatch]
pub trait PhysicalPlan {
    fn schema(&self) -> Schema;

    fn execute(&self) -> RecordBatch;

    fn children(&self) -> Vec<PhysicalPlans>;
}

#[derive(Clone)]
#[enum_dispatch(PhysicalPlan)]
pub enum PhysicalPlans {
    Scan(ScanExec),
    Projection(ProjectionExec),
    Filter(FilterExec),
    Limit(LimitExec),
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
}

impl PhysicalPlan for ScanExec {
    fn schema(&self) -> Schema {
        self.datasource.schema().select(&self.projection)
    }

    fn execute(&self) -> RecordBatch {
        self.datasource.record_batch().clone()
    }

    fn children(&self) -> Vec<PhysicalPlans> {
        Vec::new()
    }
}

#[derive(Clone)]
pub struct ProjectionExec {
    schema: Schema,
    physical_plan: Box<PhysicalPlans>,
    expr: Vec<PhysicalExprs>,
}

impl ProjectionExec {
    pub fn new(
        schema: Schema,
        physical_plan: PhysicalPlans,
        expr: Vec<PhysicalExprs>,
    ) -> ProjectionExec {
        ProjectionExec {
            schema,
            physical_plan: Box::new(physical_plan),
            expr,
        }
    }
}

impl PhysicalPlan for ProjectionExec {
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

    fn children(&self) -> Vec<PhysicalPlans> {
        vec![*self.physical_plan.clone()]
    }
}

#[derive(Clone)]
pub struct FilterExec {
    schema: Schema,
    physical_plan: Box<PhysicalPlans>,
    expr: PhysicalExprs,
}

impl FilterExec {
    pub fn new(schema: Schema, physical_plan: PhysicalPlans, expr: PhysicalExprs) -> FilterExec {
        FilterExec {
            schema,
            physical_plan: Box::new(physical_plan),
            expr,
        }
    }
}

impl PhysicalPlan for FilterExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn execute(&self) -> RecordBatch {
        let record_batch = self.physical_plan.execute();
        let eval_col = self.expr.evaluate(&record_batch);

        let cols = record_batch
            .iter()
            .map(|c| {
                Arc::new(Vectors::ColumnVector(ColumnVector::new(
                    *c.get_type(),
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

    fn children(&self) -> Vec<PhysicalPlans> {
        vec![*self.physical_plan.clone()]
    }
}

#[derive(Clone)]
pub struct LimitExec {
    schema: Schema,
    physical_plan: Box<PhysicalPlans>,
    limit: usize,
}

impl LimitExec {
    pub fn new(schema: Schema, physical_plan: PhysicalPlans, limit: usize) -> LimitExec {
        LimitExec {
            schema,
            physical_plan: Box::new(physical_plan),
            limit,
        }
    }
}

impl PhysicalPlan for LimitExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn execute(&self) -> RecordBatch {
        let record_batch = self.physical_plan.execute();
        let cols = record_batch
            .iter()
            .map(|c| {
                Arc::new(Vectors::ColumnVector(ColumnVector::new(
                    *c.get_type(),
                    c.iter().take(self.limit).cloned().collect(),
                )))
            })
            .collect();

        RecordBatch::new(self.schema.clone(), cols)
    }

    fn children(&self) -> Vec<PhysicalPlans> {
        vec![*self.physical_plan.clone()]
    }
}
