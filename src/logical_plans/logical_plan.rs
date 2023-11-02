use std::fmt::Display;

use crate::{
    datasources::datasource::Datasource,
    datatypes::schema::{Field, Schema},
    error::ZakuError,
    physical_plans::{
        physical_expr::PhysicalExpr,
        physical_plan::{FilterExec, PhysicalPlan, ProjectionExec, ScanExec},
    },
};

use super::logical_expr::LogicalExpr;

pub trait LogicalPlanTrait {
    fn schema(&self) -> Schema;
    fn children(&self) -> Vec<LogicalPlan>;
    fn to_string(&self) -> String;
    fn to_physical_plan(&self) -> Result<PhysicalPlan, ZakuError>;
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan(Scan),
    Projection(Projection),
    Filter(Filter),
}

impl LogicalPlan {
    fn to_string(&self) -> String {
        match self {
            LogicalPlan::Scan(scan) => scan.to_string(),
            LogicalPlan::Projection(projection) => projection.to_string(),
            LogicalPlan::Filter(filter) => filter.to_string(),
        }
    }

    fn format(plan: &LogicalPlan, indent: usize) -> String {
        let mut s = String::new();
        (0..indent).for_each(|_| s.push_str("\t"));
        s.push_str(plan.to_string().as_str());
        s.push_str("\n");
        plan.children().iter().for_each(|p| {
            s.push_str(LogicalPlan::format(p, indent + 1).as_str());
        });
        s
    }
}

impl LogicalPlanTrait for LogicalPlan {
    fn schema(&self) -> Schema {
        match self {
            LogicalPlan::Scan(scan) => scan.schema(),
            LogicalPlan::Projection(projection) => projection.schema(),
            LogicalPlan::Filter(filter) => filter.schema(),
        }
    }

    fn children(&self) -> Vec<LogicalPlan> {
        match self {
            LogicalPlan::Scan(scan) => scan.children(),
            LogicalPlan::Projection(projection) => projection.children(),
            LogicalPlan::Filter(filter) => filter.children(),
        }
    }

    fn to_string(&self) -> String {
        self.to_string()
    }

    fn to_physical_plan(&self) -> Result<PhysicalPlan, ZakuError> {
        match self {
            LogicalPlan::Scan(scan) => scan.to_physical_plan(),
            LogicalPlan::Projection(projection) => projection.to_physical_plan(),
            LogicalPlan::Filter(filter) => filter.to_physical_plan(),
        }
    }
}

impl Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", LogicalPlan::format(self, 0))
    }
}

#[derive(Debug, Clone)]
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

impl LogicalPlanTrait for Scan {
    fn schema(&self) -> Schema {
        let mut schema = self.datasource.schema().clone();
        if !self.projection.is_empty() {
            schema = schema.select(&self.projection);
        }
        schema
    }

    fn children(&self) -> Vec<LogicalPlan> {
        Vec::new()
    }

    fn to_string(&self) -> String {
        if self.projection.is_empty() {
            return format!("Scan: {} | None", self.path);
        } else {
            return format!("Scan: {} | {}", self.path, self.projection.join(", "));
        }
    }

    fn to_physical_plan(&self) -> Result<PhysicalPlan, ZakuError> {
        Ok(PhysicalPlan::Scan(ScanExec::new(
            self.datasource.clone(),
            self.projection.clone(),
        )))
    }
}

#[derive(Debug, Clone)]
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
}

impl LogicalPlanTrait for Projection {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn children(&self) -> Vec<LogicalPlan> {
        vec![*self.logical_plan.clone()]
    }

    fn to_string(&self) -> String {
        format!(
            "Projection: {}",
            self.expr
                .iter()
                .map(|e| format!("#{}", e.to_string()))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }

    fn to_physical_plan(&self) -> Result<PhysicalPlan, ZakuError> {
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

#[derive(Debug, Clone)]
pub struct Filter {
    logical_plan: Box<LogicalPlan>,
    expr: LogicalExpr,
}

impl Filter {
    pub fn new(logical_plan: LogicalPlan, expr: LogicalExpr) -> Result<Filter, ZakuError> {
        Ok(Filter {
            logical_plan: Box::new(logical_plan),
            expr,
        })
    }
}

impl LogicalPlanTrait for Filter {
    fn schema(&self) -> Schema {
        self.logical_plan.schema()
    }

    fn children(&self) -> Vec<LogicalPlan> {
        vec![*self.logical_plan.clone()]
    }

    fn to_string(&self) -> String {
        format!("Filter: {}", self.expr.to_string())
    }

    fn to_physical_plan(&self) -> Result<PhysicalPlan, ZakuError> {
        let physical_plan = self.logical_plan.to_physical_plan()?;
        let physical_expr = self.expr.to_physical_expr(&self.logical_plan)?;
        Ok(PhysicalPlan::Filter(FilterExec::new(
            self.schema(),
            physical_plan,
            physical_expr,
        )))
    }
}
