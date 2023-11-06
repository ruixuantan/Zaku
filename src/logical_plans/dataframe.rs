use crate::{datasources::datasource::Datasource, datatypes::schema::Schema, error::ZakuError};

use super::{
    logical_expr::LogicalExprs,
    logical_plan::{Filter, Limit, LogicalPlan, LogicalPlans, Projection, Scan},
};

#[derive(Debug, Clone)]
pub struct Dataframe {
    plan: LogicalPlans,
}

impl Dataframe {
    pub fn new(plan: LogicalPlans) -> Dataframe {
        Dataframe { plan }
    }

    pub fn schema(&self) -> Schema {
        self.plan.schema()
    }

    pub fn logical_plan(&self) -> &LogicalPlans {
        &self.plan
    }

    pub fn from_csv(filename: &str) -> Result<Dataframe, ZakuError> {
        let datasource = Datasource::from_csv(filename)?;
        Ok(Dataframe::new(LogicalPlans::Scan(Scan::new(
            datasource,
            filename.to_string(),
            Vec::new(),
        ))))
    }

    pub fn projection(&self, expr: Vec<LogicalExprs>) -> Result<Dataframe, ZakuError> {
        Ok(Dataframe::new(LogicalPlans::Projection(Projection::new(
            self.plan.clone(),
            expr,
        )?)))
    }

    pub fn filter(&self, expr: LogicalExprs) -> Result<Dataframe, ZakuError> {
        Ok(Dataframe::new(LogicalPlans::Filter(Filter::new(
            self.plan.clone(),
            expr,
        )?)))
    }

    pub fn limit(&self, limit: usize) -> Result<Dataframe, ZakuError> {
        Ok(Dataframe::new(LogicalPlans::Limit(Limit::new(
            self.plan.clone(),
            limit,
        )?)))
    }
}
