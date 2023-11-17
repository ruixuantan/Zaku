use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

use crate::{datasources::datasource::Datasource, datatypes::schema::Schema, error::ZakuError};

use super::{
    aggregate_expr::AggregateExprs,
    logical_expr::LogicalExprs,
    logical_plan::{Aggregate, Filter, Limit, LogicalPlan, LogicalPlans, Projection, Scan, Sort},
};

#[derive(Debug, Clone)]
pub struct Dataframe {
    plan: Arc<LogicalPlans>,
}

impl Dataframe {
    pub fn new(plan: Arc<LogicalPlans>) -> Dataframe {
        Dataframe { plan }
    }

    pub fn schema(&self) -> Schema {
        self.plan.schema()
    }

    pub fn logical_plan(&self) -> &LogicalPlans {
        &self.plan
    }

    pub fn from_csv(filename: &str, delimiter: Option<u8>) -> Result<Dataframe, ZakuError> {
        let datasource = Datasource::from_csv(filename, delimiter)?;
        Ok(Dataframe::new(Arc::new(LogicalPlans::Scan(Scan::new(
            datasource,
            filename.to_string(),
            Vec::new(),
        )))))
    }

    pub fn projection(&self, expr: Vec<LogicalExprs>) -> Result<Dataframe, ZakuError> {
        Ok(Dataframe::new(Arc::new(LogicalPlans::Projection(
            Projection::new(self.plan.clone(), expr)?,
        ))))
    }

    pub fn filter(&self, expr: LogicalExprs) -> Result<Dataframe, ZakuError> {
        Ok(Dataframe::new(Arc::new(LogicalPlans::Filter(Filter::new(
            self.plan.clone(),
            expr,
        )?))))
    }

    pub fn limit(&self, limit: usize) -> Result<Dataframe, ZakuError> {
        Ok(Dataframe::new(Arc::new(LogicalPlans::Limit(Limit::new(
            self.plan.clone(),
            limit,
        )?))))
    }

    pub fn sort(&self, sort_by: Vec<LogicalExprs>, asc: Vec<bool>) -> Result<Dataframe, ZakuError> {
        Ok(Dataframe::new(Arc::new(LogicalPlans::Sort(Sort::new(
            self.plan.clone(),
            sort_by,
            asc,
        )?))))
    }

    pub fn aggregate(
        &self,
        group_by: Vec<LogicalExprs>,
        aggregates: Vec<AggregateExprs>,
    ) -> Result<Dataframe, ZakuError> {
        Ok(Dataframe::new(Arc::new(LogicalPlans::Aggregate(
            Aggregate::new(self.plan.clone(), group_by, aggregates)?,
        ))))
    }
}

impl Display for Dataframe {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.plan)
    }
}
