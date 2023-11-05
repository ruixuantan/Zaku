use enum_dispatch::enum_dispatch;
use std::sync::Arc;

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

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_>;

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

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        self.datasource.scan()
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

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        Box::new(self.physical_plan.execute().map(|rb| {
            let columns = self.expr.iter().map(|e| e.evaluate(&rb)).collect();
            RecordBatch::new(self.schema.clone(), columns)
        }))
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

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        let res = self.physical_plan.execute().map(|rb| {
            let eval_col = self.expr.evaluate(&rb);

            let cols = rb
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
        });

        Box::new(res)
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

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        let mut counter = self.limit;
        let res = self.physical_plan.execute().map(move |rb| {
            let take = if counter > rb.row_count() {
                counter -= rb.row_count();
                rb.row_count()
            } else {
                let temp = counter;
                counter = 0;
                temp
            };
            let cols = rb
                .iter()
                .map(|c| {
                    Arc::new(Vectors::ColumnVector(ColumnVector::new(
                        *c.get_type(),
                        c.iter().take(take).cloned().collect(),
                    )))
                })
                .collect();

            RecordBatch::new(self.schema.clone(), cols)
        });

        Box::new(res)
    }

    fn children(&self) -> Vec<PhysicalPlans> {
        vec![*self.physical_plan.clone()]
    }
}
