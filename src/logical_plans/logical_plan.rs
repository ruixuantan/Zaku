use std::fmt::Display;

use enum_dispatch::enum_dispatch;

use crate::{
    datasources::datasource::Datasource,
    datatypes::schema::{Field, Schema},
    error::ZakuError,
    physical_plans::{
        accumulator::AggregateExpressions,
        physical_expr::PhysicalExprs,
        physical_plan::{
            FilterExec, HashAggregateExec, LimitExec, PhysicalPlans, ProjectionExec, ScanExec,
        },
    },
};

use super::{
    aggregate_expr::AggregateExprs,
    logical_expr::{LogicalExpr, LogicalExprs},
};

#[enum_dispatch]
pub trait LogicalPlan {
    fn schema(&self) -> Schema;
    fn children(&self) -> Vec<LogicalPlans>;
    fn to_string(&self) -> String;
    fn to_physical_plan(&self) -> Result<PhysicalPlans, ZakuError>;
}

#[derive(Debug, Clone)]
#[enum_dispatch(LogicalPlan)]
pub enum LogicalPlans {
    Scan(Scan),
    Projection(Projection),
    Filter(Filter),
    Limit(Limit),
    Aggregate(Aggregate),
}

impl LogicalPlans {
    fn format(plan: &LogicalPlans, indent: usize) -> String {
        let mut s = String::new();
        (0..indent).for_each(|_| s.push('\t'));
        s.push_str(LogicalPlan::to_string(plan).as_str());
        s.push('\n');
        plan.children().iter().for_each(|p| {
            s.push_str(LogicalPlans::format(p, indent + 1).as_str());
        });
        s
    }
}

impl Display for LogicalPlans {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", LogicalPlans::format(self, 0))
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

impl LogicalPlan for Scan {
    fn schema(&self) -> Schema {
        let mut schema = self.datasource.schema().clone();
        if !self.projection.is_empty() {
            schema = schema.select(&self.projection);
        }
        schema
    }

    fn children(&self) -> Vec<LogicalPlans> {
        Vec::new()
    }

    fn to_string(&self) -> String {
        if self.projection.is_empty() {
            format!("Scan: {} | None", self.path)
        } else {
            format!("Scan: {} | {}", self.path, self.projection.join(", "))
        }
    }

    fn to_physical_plan(&self) -> Result<PhysicalPlans, ZakuError> {
        Ok(PhysicalPlans::Scan(ScanExec::new(
            self.datasource.clone(),
            self.projection.clone(),
        )))
    }
}

#[derive(Debug, Clone)]
pub struct Projection {
    schema: Schema,
    logical_plan: Box<LogicalPlans>,
    expr: Vec<LogicalExprs>,
}

impl Projection {
    pub fn new(
        logical_plan: LogicalPlans,
        expr: Vec<LogicalExprs>,
    ) -> Result<Projection, ZakuError> {
        let schema: Result<Vec<Field>, _> =
            expr.iter().map(|e| e.to_field(&logical_plan)).collect();

        Ok(Projection {
            schema: Schema::new(schema?),
            logical_plan: Box::new(logical_plan),
            expr,
        })
    }
}

impl LogicalPlan for Projection {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn children(&self) -> Vec<LogicalPlans> {
        vec![*self.logical_plan.clone()]
    }

    fn to_string(&self) -> String {
        format!(
            "Projection: {}",
            self.expr
                .iter()
                .map(|e| format!("{}", e))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }

    fn to_physical_plan(&self) -> Result<PhysicalPlans, ZakuError> {
        let physical_plan = self.logical_plan.to_physical_plan()?;
        let projection_fields: Result<Vec<Field>, _> = self
            .expr
            .iter()
            .map(|e| e.to_field(&self.logical_plan))
            .collect();
        let projection_schema = Schema::new(projection_fields?);
        let physical_expr: Result<Vec<PhysicalExprs>, _> = self
            .expr
            .iter()
            .map(|e| e.to_physical_expr(&self.logical_plan))
            .collect();
        Ok(PhysicalPlans::Projection(ProjectionExec::new(
            projection_schema,
            physical_plan,
            physical_expr?,
        )))
    }
}

#[derive(Debug, Clone)]
pub struct Filter {
    logical_plan: Box<LogicalPlans>,
    expr: LogicalExprs,
}

impl Filter {
    pub fn new(logical_plan: LogicalPlans, expr: LogicalExprs) -> Result<Filter, ZakuError> {
        Ok(Filter {
            logical_plan: Box::new(logical_plan),
            expr,
        })
    }
}

impl LogicalPlan for Filter {
    fn schema(&self) -> Schema {
        self.logical_plan.schema()
    }

    fn children(&self) -> Vec<LogicalPlans> {
        vec![*self.logical_plan.clone()]
    }

    fn to_string(&self) -> String {
        format!("Filter: {}", self.expr)
    }

    fn to_physical_plan(&self) -> Result<PhysicalPlans, ZakuError> {
        let physical_plan = self.logical_plan.to_physical_plan()?;
        let physical_expr = self.expr.to_physical_expr(&self.logical_plan)?;
        Ok(PhysicalPlans::Filter(FilterExec::new(
            self.schema(),
            physical_plan,
            physical_expr,
        )))
    }
}

#[derive(Debug, Clone)]
pub struct Limit {
    logical_plan: Box<LogicalPlans>,
    limit: usize,
}

impl Limit {
    pub fn new(logical_plan: LogicalPlans, limit: usize) -> Result<Limit, ZakuError> {
        Ok(Limit {
            logical_plan: Box::new(logical_plan),
            limit,
        })
    }
}

impl LogicalPlan for Limit {
    fn schema(&self) -> Schema {
        self.logical_plan.schema()
    }

    fn children(&self) -> Vec<LogicalPlans> {
        vec![*self.logical_plan.clone()]
    }

    fn to_string(&self) -> String {
        format!("Limit: {}", self.limit)
    }

    fn to_physical_plan(&self) -> Result<PhysicalPlans, ZakuError> {
        let physical_plan = self.logical_plan.to_physical_plan()?;
        Ok(PhysicalPlans::Limit(LimitExec::new(
            self.schema(),
            physical_plan,
            self.limit,
        )))
    }
}

#[derive(Debug, Clone)]
pub struct Aggregate {
    schema: Schema,
    logical_plan: Box<LogicalPlans>,
    group_expr: Vec<LogicalExprs>,
    aggregate_expr: Vec<AggregateExprs>,
}

impl Aggregate {
    pub fn new(
        logical_plan: LogicalPlans,
        group_expr: Vec<LogicalExprs>,
        aggregate_expr: Vec<AggregateExprs>,
    ) -> Result<Aggregate, ZakuError> {
        let mut group_fields = group_expr
            .iter()
            .map(|e| e.to_field(&logical_plan))
            .collect::<Result<Vec<Field>, _>>()?;
        let mut aggregate_fields = aggregate_expr
            .iter()
            .map(|e| e.to_field(&logical_plan))
            .collect::<Result<Vec<Field>, _>>()?;
        group_fields.append(&mut aggregate_fields);
        Ok(Aggregate {
            schema: Schema::new(group_fields),
            logical_plan: Box::new(logical_plan),
            group_expr,
            aggregate_expr,
        })
    }
}

impl LogicalPlan for Aggregate {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn children(&self) -> Vec<LogicalPlans> {
        vec![*self.logical_plan.clone()]
    }

    fn to_string(&self) -> String {
        format!(
            "Aggregate: group by={}, aggregate={}",
            self.group_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(", "),
            self.aggregate_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }

    fn to_physical_plan(&self) -> Result<PhysicalPlans, ZakuError> {
        let physical_plan = self.logical_plan.to_physical_plan()?;
        let physical_group_expr = self
            .group_expr
            .iter()
            .map(|e| e.to_physical_expr(&self.logical_plan))
            .collect::<Result<Vec<PhysicalExprs>, _>>()?;
        let physical_aggregate_expr = self
            .aggregate_expr
            .iter()
            .map(|e| e.to_physical_aggregate(&self.logical_plan))
            .collect::<Result<Vec<AggregateExpressions>, _>>()?;
        Ok(PhysicalPlans::HashAggregate(HashAggregateExec::new(
            physical_plan,
            physical_group_expr,
            physical_aggregate_expr,
            self.schema(),
        )))
    }
}
