use std::sync::Arc;

use crate::datatypes::{schema::Field, types::DataType};

use super::logical_plan::LogicalPlan;

pub trait LogicalExpr {
    fn to_field(&self, input: &Arc<dyn LogicalPlan>) -> Field;

    fn to_string(&self) -> String;
}

pub struct Column {
    name: String,
}

impl Column {
    pub fn new(name: String) -> Column {
        Column { name }
    }
}

impl LogicalExpr for Column {
    fn to_field(&self, input: &Arc<dyn LogicalPlan>) -> Field {
        input
            .schema()
            .get_field(&self.name)
            .expect(format!("Field {} not found", self.name).as_str())
    }

    fn to_string(&self) -> String {
        self.name.clone()
    }
}

pub struct LiteralText {
    value: String,
}

impl LiteralText {
    pub fn new(value: String) -> LiteralText {
        LiteralText { value }
    }
}

impl LogicalExpr for LiteralText {
    fn to_field(&self, _input: &Arc<dyn LogicalPlan>) -> Field {
        Field::new(self.value.clone(), DataType::Text)
    }

    fn to_string(&self) -> String {
        self.value.clone()
    }
}

pub struct LiteralInteger {
    value: i32,
}

impl LiteralInteger {
    pub fn new(value: i32) -> LiteralInteger {
        LiteralInteger { value }
    }
}

impl LogicalExpr for LiteralInteger {
    fn to_field(&self, _input: &Arc<dyn LogicalPlan>) -> Field {
        Field::new(self.value.to_string(), DataType::Integer)
    }

    fn to_string(&self) -> String {
        self.value.to_string()
    }
}

pub struct LiteralFloat {
    value: f32,
}

impl LiteralFloat {
    pub fn new(value: f32) -> LiteralFloat {
        LiteralFloat { value }
    }
}

impl LogicalExpr for LiteralFloat {
    fn to_field(&self, _input: &Arc<dyn LogicalPlan>) -> Field {
        Field::new(self.value.to_string(), DataType::Float)
    }

    fn to_string(&self) -> String {
        self.value.to_string()
    }
}

pub struct LiteralBoolean {
    value: bool,
}

impl LiteralBoolean {
    pub fn new(value: bool) -> LiteralBoolean {
        LiteralBoolean { value }
    }
}

impl LogicalExpr for LiteralBoolean {
    fn to_field(&self, _input: &Arc<dyn LogicalPlan>) -> Field {
        Field::new(self.value.to_string(), DataType::Boolean)
    }

    fn to_string(&self) -> String {
        self.value.to_string()
    }
}
