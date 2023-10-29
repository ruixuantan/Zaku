use crate::{datasources::datasource::Datasource, datatypes::schema::Schema, error::ZakuError};

use super::{
    logical_expr::LogicalExpr,
    logical_plan::{LogicalPlan, Projection, Scan},
};

#[derive(Debug, Clone)]
pub struct Dataframe {
    plan: LogicalPlan,
}

impl Dataframe {
    pub fn new(plan: LogicalPlan) -> Dataframe {
        Dataframe { plan }
    }

    pub fn schema(&self) -> Schema {
        self.plan.schema()
    }

    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.plan
    }

    pub fn from_csv(filename: &String) -> Result<Dataframe, ZakuError> {
        let datasource = Datasource::from_csv(filename)?;
        Ok(Dataframe::new(LogicalPlan::Scan(Scan::new(
            datasource,
            filename.to_string(),
            Vec::new(),
        ))))
    }

    pub fn projection(&self, expr: Vec<LogicalExpr>) -> Result<Dataframe, ZakuError> {
        Ok(Dataframe::new(LogicalPlan::Projection(Projection::new(
            self.plan.clone(),
            expr,
        )?)))
    }
}
