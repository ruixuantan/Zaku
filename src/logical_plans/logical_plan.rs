use std::sync::Arc;

use crate::{datasources::datasource::Datasource, datatypes::schema::Schema};

use super::logical_expr::LogicalExpr;

pub trait LogicalPlan {
    fn schema(&self) -> Schema;

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>>;

    fn to_string(&self) -> String;
}

pub struct Scan {
    schema: Schema,
    path: String,
    projection: Vec<String>,
}

impl Scan {
    pub fn new(datasource: Datasource, path: String, projection: Vec<String>) -> Scan {
        let mut schema = datasource.get_schema().clone();
        if !projection.is_empty() {
            schema = schema.select(&projection);
        }
        Scan {
            schema,
            path,
            projection,
        }
    }
}

impl LogicalPlan for Scan {
    fn schema(&self) -> Schema {
        self.schema.clone()
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
}
