use crate::{
    datasources::datasource::Datasource,
    datatypes::schema::{Field, Schema},
    error::ZakuError,
    physical_plans::{
        physical_expr::PhysicalExpr,
        physical_plan::{PhysicalPlan, ProjectionExec, ScanExec},
    },
};

use super::logical_expr::LogicalExpr;

#[derive(Clone)]
pub enum LogicalPlan {
    Scan(Scan),
    Projection(Projection),
}

impl LogicalPlan {
    pub fn schema(&self) -> Schema {
        match self {
            LogicalPlan::Scan(scan) => scan.schema(),
            LogicalPlan::Projection(projection) => projection.schema(),
        }
    }

    pub fn children(&self) -> Vec<LogicalPlan> {
        match self {
            LogicalPlan::Scan(scan) => scan.children(),
            LogicalPlan::Projection(projection) => projection.children(),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            LogicalPlan::Scan(scan) => scan.to_string(),
            LogicalPlan::Projection(projection) => projection.to_string(),
        }
    }

    pub fn to_physical_plan(&self) -> Result<PhysicalPlan, ZakuError> {
        match self {
            LogicalPlan::Scan(scan) => scan.to_physical_plan(),
            LogicalPlan::Projection(projection) => projection.to_physical_plan(),
        }
    }
}

#[derive(Clone)]
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

    pub fn schema(&self) -> Schema {
        let mut schema = self.datasource.schema().clone();
        if !self.projection.is_empty() {
            schema = schema.select(&self.projection);
        }
        schema
    }

    pub fn children(&self) -> Vec<LogicalPlan> {
        Vec::new()
    }

    pub fn to_string(&self) -> String {
        if self.projection.is_empty() {
            return format!("Scan: {} | None", self.path);
        } else {
            return format!("Scan: {} | {}", self.path, self.projection.join(", "));
        }
    }

    pub fn to_physical_plan(&self) -> Result<PhysicalPlan, ZakuError> {
        Ok(PhysicalPlan::Scan(ScanExec::new(
            self.datasource.clone(),
            self.projection.clone(),
        )))
    }
}

#[derive(Clone)]
pub struct Projection {
    schema: Schema,
    logical_plan: Box<LogicalPlan>,
    expr: Vec<LogicalExpr>,
}

impl Projection {
    pub fn new(logical_plan: LogicalPlan, expr: Vec<LogicalExpr>) -> Result<Projection, ZakuError> {
        let schema: Result<Vec<Field>, _> =
            expr.iter().map(|e| e.to_field(&logical_plan)).collect();
        Ok(Projection {
            schema: Schema::new(schema?),
            logical_plan: Box::new(logical_plan),
            expr,
        })
    }

    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }

    pub fn children(&self) -> Vec<LogicalPlan> {
        vec![*self.logical_plan.clone()]
    }

    pub fn to_string(&self) -> String {
        format!(
            "Projection: {}",
            self.expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }

    pub fn to_physical_plan(&self) -> Result<PhysicalPlan, ZakuError> {
        let physical_plan = self.logical_plan.to_physical_plan()?;
        let projection_fields: Result<Vec<Field>, _> = self
            .expr
            .iter()
            .map(|e| e.to_field(&self.logical_plan))
            .collect();
        let projection_schema = Schema::new(projection_fields?);
        let physical_expr: Result<Vec<PhysicalExpr>, _> = self
            .expr
            .iter()
            .map(|e| e.to_physical_expr(&self.logical_plan))
            .collect();
        Ok(PhysicalPlan::Projection(ProjectionExec::new(
            projection_schema,
            physical_plan,
            physical_expr?,
        )))
    }
}
