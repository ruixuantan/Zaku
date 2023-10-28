use crate::{
    datasources::datasource::Datasource,
    datatypes::{record_batch::RecordBatch, schema::Schema},
};

use super::physical_expr::PhysicalExpr;

#[derive(Clone)]
pub enum PhysicalPlan {
    Scan(ScanExec),
    Projection(ProjectionExec),
}

impl PhysicalPlan {
    pub fn schema(&self) -> Schema {
        match self {
            PhysicalPlan::Scan(scan) => scan.schema(),
            PhysicalPlan::Projection(projection) => projection.schema(),
        }
    }

    pub fn execute(&self) -> RecordBatch {
        match self {
            PhysicalPlan::Scan(scan) => scan.execute(),
            PhysicalPlan::Projection(projection) => projection.execute(),
        }
    }

    pub fn children(&self) -> Vec<PhysicalPlan> {
        match self {
            PhysicalPlan::Scan(scan) => scan.children(),
            PhysicalPlan::Projection(projection) => projection.children(),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            PhysicalPlan::Scan(scan) => scan.to_string(),
            PhysicalPlan::Projection(projection) => projection.to_string(),
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
