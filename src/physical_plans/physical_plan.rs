use std::sync::Arc;

use crate::{
    datasources::datasource::Datasource,
    datatypes::{record_batch::RecordBatch, schema::Schema},
};

use super::physical_expr::PhysicalExpr;

pub trait PhysicalPlan {
    fn schema(&self) -> Schema;

    fn execute(&self) -> RecordBatch;

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>>;

    fn to_string(&self) -> String;
}

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

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        Vec::new()
    }

    fn to_string(&self) -> String {
        if self.projection.is_empty() {
            return format!("ScanExec: {} | None", self.datasource.path());
        } else {
            return format!(
                "ScanExec: {} | {}",
                self.datasource.path(),
                self.projection.join(", ")
            );
        }
    }
}

pub struct ProjectionExec {
    schema: Schema,
    physical_plan: Arc<dyn PhysicalPlan>,
    expr: Vec<Arc<dyn PhysicalExpr>>,
}

impl ProjectionExec {
    pub fn new(
        schema: Schema,
        physical_plan: Arc<dyn PhysicalPlan>,
        expr: Vec<Arc<dyn PhysicalExpr>>,
    ) -> ProjectionExec {
        ProjectionExec {
            schema,
            physical_plan,
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

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        vec![self.physical_plan.clone()]
    }

    fn to_string(&self) -> String {
        let expr_str = self
            .expr
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<String>>()
            .join(", ");
        format!("ProjectionExec: {}", expr_str)
    }
}
