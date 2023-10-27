use std::sync::Arc;

use crate::{
    datasources::datasource::Datasource,
    datatypes::schema::Schema,
    physical_plans::physical_plan::{PhysicalPlan, ProjectionExec, ScanExec},
};

use super::logical_expr::LogicalExpr;

pub trait LogicalPlan {
    fn schema(&self) -> Schema;

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>>;

    fn to_string(&self) -> String;

    fn to_physical_plan(&self) -> Arc<dyn PhysicalPlan>;
}

pub struct Scan {
    pub datasource: Datasource,
    pub path: String,
    pub projection: Vec<String>,
}

impl Scan {
    pub fn new(datasource: Datasource, path: String, projection: Vec<String>) -> Scan {
        Scan {
            datasource,
            path,
            projection,
        }
    }
}

impl LogicalPlan for Scan {
    fn schema(&self) -> Schema {
        let mut schema = self.datasource.schema().clone();
        if !self.projection.is_empty() {
            schema = schema.select(&self.projection);
        }
        schema
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        Vec::new()
    }

    fn to_string(&self) -> String {
        if self.projection.is_empty() {
            return format!("Scan: {} | None", self.path);
        } else {
            return format!("Scan: {} | {}", self.path, self.projection.join(", "));
        }
    }

    fn to_physical_plan(&self) -> Arc<dyn PhysicalPlan> {
        Arc::new(ScanExec::new(
            self.datasource.clone(),
            self.projection.clone(),
        ))
    }
}

pub struct Projection {
    schema: Schema,
    logical_plan: Arc<dyn LogicalPlan>,
    expr: Vec<Arc<dyn LogicalExpr>>,
}

impl Projection {
    pub fn new(logical_plan: Arc<dyn LogicalPlan>, expr: Vec<Arc<dyn LogicalExpr>>) -> Projection {
        let schema = Schema::new(expr.iter().map(|e| e.to_field(&logical_plan)).collect());
        Projection {
            schema,
            logical_plan,
            expr,
        }
    }
}

impl LogicalPlan for Projection {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![self.logical_plan.clone()]
    }

    fn to_string(&self) -> String {
        format!(
            "Projection: {}",
            self.expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }

    fn to_physical_plan(&self) -> Arc<dyn PhysicalPlan> {
        let physical_plan = self.logical_plan.to_physical_plan();
        let projection_schema = Schema::new(
            self.expr
                .iter()
                .map(|e| e.to_field(&self.logical_plan))
                .collect(),
        );
        let physical_expr = self
            .expr
            .iter()
            .map(|e| e.to_physical_expr(&self.logical_plan))
            .collect();
        Arc::new(ProjectionExec::new(
            projection_schema,
            physical_plan,
            physical_expr,
        ))
    }
}
