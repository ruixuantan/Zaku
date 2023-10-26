use crate::datatypes::schema::Schema;

pub trait LogicalPlan {
    fn schema(&self) -> Schema;

    fn children(&self) -> Vec<Box<dyn LogicalPlan>>;
}
