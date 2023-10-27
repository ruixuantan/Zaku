use std::sync::Arc;

use crate::{datasources::datasource::Datasource, datatypes::schema::Schema, error::ZakuError};

use super::{
    logical_expr::LogicalExpr,
    logical_plan::{LogicalPlan, Projection, Scan},
};

pub struct Dataframe {
    plan: Arc<dyn LogicalPlan>,
}

impl Dataframe {
    pub fn new(plan: Arc<dyn LogicalPlan>) -> Dataframe {
        Dataframe { plan }
    }

    pub fn schema(&self) -> Schema {
        self.plan.schema()
    }

    pub fn logical_plan(&self) -> &Arc<dyn LogicalPlan> {
        &self.plan
    }

    pub fn from_csv(filename: &str) -> Result<Dataframe, ZakuError> {
        let datasource = Datasource::from_csv(filename)?;
        Ok(Dataframe::new(Arc::new(Scan::new(
            datasource,
            filename.to_string(),
            Vec::new(),
        ))))
    }

    pub fn projection(&self, expr: Vec<Arc<dyn LogicalExpr>>) -> Dataframe {
        Dataframe::new(Arc::new(Projection::new(self.plan.clone(), expr)))
    }
}
