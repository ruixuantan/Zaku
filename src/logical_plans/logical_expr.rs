use crate::{
    datatypes::schema::Field, error::ZakuError, physical_plans::physical_expr::PhysicalExpr,
};

use super::logical_plan::LogicalPlan;

#[derive(Clone)]
pub enum LogicalExpr {
    Column(String),
}

impl LogicalExpr {
    pub fn to_field(&self, input: &LogicalPlan) -> Result<Field, ZakuError> {
        match self {
            LogicalExpr::Column(name) => column_to_field(input, name),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            LogicalExpr::Column(name) => name.clone(),
        }
    }

    pub fn to_physical_expr(&self, input: &LogicalPlan) -> Result<PhysicalExpr, ZakuError> {
        match self {
            LogicalExpr::Column(name) => column_to_physical_expr(input, name),
        }
    }
}

fn column_to_field(input: &LogicalPlan, name: &String) -> Result<Field, ZakuError> {
    Ok(input.schema().get_field(name)?.clone())
}

fn column_to_physical_expr(input: &LogicalPlan, name: &String) -> Result<PhysicalExpr, ZakuError> {
    let index = input.schema().get_index(name)?;
    Ok(PhysicalExpr::ColumnExpr(index))
}

// pub struct LiteralText {
//     value: String,
// }

// impl LiteralText {
//     pub fn new(value: String) -> LiteralText {
//         LiteralText { value }
//     }
// }

// impl LogicalExpr for LiteralText {
//     fn to_field(&self, _input: &Arc<dyn LogicalPlan>) -> Field {
//         Field::new(self.value.clone(), DataType::Text)
//     }

//     fn to_string(&self) -> String {
//         self.value.clone()
//     }
// }

// pub struct LiteralInteger {
//     value: i32,
// }

// impl LiteralInteger {
//     pub fn new(value: i32) -> LiteralInteger {
//         LiteralInteger { value }
//     }
// }

// impl LogicalExpr for LiteralInteger {
//     fn to_field(&self, _input: &Arc<dyn LogicalPlan>) -> Field {
//         Field::new(self.value.to_string(), DataType::Integer)
//     }

//     fn to_string(&self) -> String {
//         self.value.to_string()
//     }
// }

// pub struct LiteralFloat {
//     value: f32,
// }

// impl LiteralFloat {
//     pub fn new(value: f32) -> LiteralFloat {
//         LiteralFloat { value }
//     }
// }

// impl LogicalExpr for LiteralFloat {
//     fn to_field(&self, _input: &Arc<dyn LogicalPlan>) -> Field {
//         Field::new(self.value.to_string(), DataType::Float)
//     }

//     fn to_string(&self) -> String {
//         self.value.to_string()
//     }
// }

// pub struct LiteralBoolean {
//     value: bool,
// }

// impl LiteralBoolean {
//     pub fn new(value: bool) -> LiteralBoolean {
//         LiteralBoolean { value }
//     }
// }

// impl LogicalExpr for LiteralBoolean {
//     fn to_field(&self, _input: &Arc<dyn LogicalPlan>) -> Field {
//         Field::new(self.value.to_string(), DataType::Boolean)
//     }

//     fn to_string(&self) -> String {
//         self.value.to_string()
//     }
// }
