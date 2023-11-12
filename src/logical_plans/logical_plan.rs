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
        (0..indent).for_each(|_| s.push_str("  "));
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
    input: Box<LogicalPlans>,
    expr: Vec<LogicalExprs>,
}

impl Projection {
    pub fn new(input: LogicalPlans, expr: Vec<LogicalExprs>) -> Result<Projection, ZakuError> {
        let schema: Result<Vec<Field>, _> = expr.iter().map(|e| e.to_field(&input)).collect();

        Ok(Projection {
            schema: Schema::new(schema?),
            input: Box::new(input),
            expr,
        })
    }
}

impl LogicalPlan for Projection {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn children(&self) -> Vec<LogicalPlans> {
        vec![*self.input.clone()]
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
        let physical_plan = self.input.to_physical_plan()?;
        let projection_fields: Result<Vec<Field>, _> =
            self.expr.iter().map(|e| e.to_field(&self.input)).collect();
        let projection_schema = Schema::new(projection_fields?);
        let physical_expr: Result<Vec<PhysicalExprs>, _> = self
            .expr
            .iter()
            .map(|e| e.to_physical_expr(&self.input))
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
    input: Box<LogicalPlans>,
    expr: LogicalExprs,
}

impl Filter {
    pub fn new(input: LogicalPlans, expr: LogicalExprs) -> Result<Filter, ZakuError> {
        Ok(Filter {
            input: Box::new(input),
            expr,
        })
    }
}

impl LogicalPlan for Filter {
    fn schema(&self) -> Schema {
        self.input.schema()
    }

    fn children(&self) -> Vec<LogicalPlans> {
        vec![*self.input.clone()]
    }

    fn to_string(&self) -> String {
        format!("Filter: {}", self.expr)
    }

    fn to_physical_plan(&self) -> Result<PhysicalPlans, ZakuError> {
        let physical_plan = self.input.to_physical_plan()?;
        let physical_expr = self.expr.to_physical_expr(&self.input)?;
        Ok(PhysicalPlans::Filter(FilterExec::new(
            self.schema(),
            physical_plan,
            physical_expr,
        )))
    }
}

#[derive(Debug, Clone)]
pub struct Limit {
    input: Box<LogicalPlans>,
    limit: usize,
}

impl Limit {
    pub fn new(input: LogicalPlans, limit: usize) -> Result<Limit, ZakuError> {
        Ok(Limit {
            input: Box::new(input),
            limit,
        })
    }
}

impl LogicalPlan for Limit {
    fn schema(&self) -> Schema {
        self.input.schema()
    }

    fn children(&self) -> Vec<LogicalPlans> {
        vec![*self.input.clone()]
    }

    fn to_string(&self) -> String {
        format!("Limit: {}", self.limit)
    }

    fn to_physical_plan(&self) -> Result<PhysicalPlans, ZakuError> {
        let physical_plan = self.input.to_physical_plan()?;
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
    input: Box<LogicalPlans>,
    group_expr: Vec<LogicalExprs>,
    aggregate_expr: Vec<AggregateExprs>,
}

impl Aggregate {
    pub fn new(
        input: LogicalPlans,
        group_expr: Vec<LogicalExprs>,
        aggregate_expr: Vec<AggregateExprs>,
    ) -> Result<Aggregate, ZakuError> {
        let mut group_fields = group_expr
            .iter()
            .map(|e| e.to_field(&input))
            .collect::<Result<Vec<Field>, _>>()?;
        let mut aggregate_fields = aggregate_expr
            .iter()
            .map(|e| e.to_field(&input))
            .collect::<Result<Vec<Field>, _>>()?;
        group_fields.append(&mut aggregate_fields);
        Ok(Aggregate {
            schema: Schema::new(group_fields),
            input: Box::new(input),
            group_expr,
            aggregate_expr,
        })
    }

    fn group_expr_str(&self) -> String {
        if self.group_expr.is_empty() {
            "None".to_string()
        } else {
            self.group_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        }
    }

    fn aggr_expr_str(&self) -> String {
        if self.aggregate_expr.is_empty() {
            "None".to_string()
        } else {
            self.aggregate_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        }
    }
}

impl LogicalPlan for Aggregate {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn children(&self) -> Vec<LogicalPlans> {
        vec![*self.input.clone()]
    }

    fn to_string(&self) -> String {
        format!(
            "Aggregate: group by={}, aggregate={}",
            self.group_expr_str(),
            self.aggr_expr_str()
        )
    }

    fn to_physical_plan(&self) -> Result<PhysicalPlans, ZakuError> {
        let physical_plan = self.input.to_physical_plan()?;
        let physical_group_expr = self
            .group_expr
            .iter()
            .map(|e| e.to_physical_expr(&self.input))
            .collect::<Result<Vec<PhysicalExprs>, _>>()?;
        let physical_aggregate_expr = self
            .aggregate_expr
            .iter()
            .map(|e| e.to_physical_aggregate(&self.input))
            .collect::<Result<Vec<AggregateExpressions>, _>>()?;
        Ok(PhysicalPlans::HashAggregate(HashAggregateExec::new(
            physical_plan,
            physical_group_expr,
            physical_aggregate_expr,
            self.schema(),
        )))
    }
}
