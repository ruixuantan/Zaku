use crate::{
    datatypes::{schema::Field, types::DataType},
    error::ZakuError,
    physical_plans::{self, binary_expr::BooleanOp, physical_expr::PhysicalExpr},
};

use super::{logical_expr::LogicalExpr, logical_plan::LogicalPlan};

#[derive(Debug, Clone)]
pub enum BinaryExpr {
    And(BooleanExpr),
    Or(BooleanExpr),
    Eq(BooleanExpr),
    Neq(BooleanExpr),
    Gt(BooleanExpr),
    Gte(BooleanExpr),
    Lt(BooleanExpr),
    Lte(BooleanExpr),
    Add(MathExpr),
    Sub(MathExpr),
    Mul(MathExpr),
    Div(MathExpr),
    Mod(MathExpr),
}

impl BinaryExpr {
    pub fn to_field(&self, input: &LogicalPlan) -> Result<Field, ZakuError> {
        match self {
            BinaryExpr::And(expr) => expr.to_field(),
            BinaryExpr::Or(expr) => expr.to_field(),
            BinaryExpr::Eq(expr) => expr.to_field(),
            BinaryExpr::Neq(expr) => expr.to_field(),
            BinaryExpr::Gt(expr) => expr.to_field(),
            BinaryExpr::Gte(expr) => expr.to_field(),
            BinaryExpr::Lt(expr) => expr.to_field(),
            BinaryExpr::Lte(expr) => expr.to_field(),
            BinaryExpr::Add(expr) => expr.to_field(input),
            BinaryExpr::Sub(expr) => expr.to_field(input),
            BinaryExpr::Mul(expr) => expr.to_field(input),
            BinaryExpr::Div(expr) => expr.to_field(input),
            BinaryExpr::Mod(expr) => expr.to_field(input),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            BinaryExpr::And(expr) => expr.to_string(),
            BinaryExpr::Or(expr) => expr.to_string(),
            BinaryExpr::Eq(expr) => expr.to_string(),
            BinaryExpr::Neq(expr) => expr.to_string(),
            BinaryExpr::Gt(expr) => expr.to_string(),
            BinaryExpr::Gte(expr) => expr.to_string(),
            BinaryExpr::Lt(expr) => expr.to_string(),
            BinaryExpr::Lte(expr) => expr.to_string(),
            BinaryExpr::Add(expr) => expr.to_string(),
            BinaryExpr::Sub(expr) => expr.to_string(),
            BinaryExpr::Mul(expr) => expr.to_string(),
            BinaryExpr::Div(expr) => expr.to_string(),
            BinaryExpr::Mod(expr) => expr.to_string(),
        }
    }

    pub fn to_physical_expr(&self, input: &LogicalPlan) -> Result<PhysicalExpr, ZakuError> {
        match self {
            BinaryExpr::And(expr) => expr.to_physical_expr(input),
            BinaryExpr::Or(expr) => expr.to_physical_expr(input),
            BinaryExpr::Eq(expr) => expr.to_physical_expr(input),
            BinaryExpr::Neq(expr) => expr.to_physical_expr(input),
            BinaryExpr::Gt(expr) => expr.to_physical_expr(input),
            BinaryExpr::Gte(expr) => expr.to_physical_expr(input),
            BinaryExpr::Lt(expr) => expr.to_physical_expr(input),
            BinaryExpr::Lte(expr) => expr.to_physical_expr(input),
            BinaryExpr::Add(expr) => expr.to_physical_expr(input),
            BinaryExpr::Sub(expr) => expr.to_physical_expr(input),
            BinaryExpr::Mul(expr) => expr.to_physical_expr(input),
            BinaryExpr::Div(expr) => expr.to_physical_expr(input),
            BinaryExpr::Mod(expr) => expr.to_physical_expr(input),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BooleanExpr {
    name: String,
    l: Box<LogicalExpr>,
    op: String,
    r: Box<LogicalExpr>,
}

impl BooleanExpr {
    pub fn new(name: String, l: LogicalExpr, op: String, r: LogicalExpr) -> BooleanExpr {
        BooleanExpr {
            name,
            l: Box::new(l),
            op,
            r: Box::new(r),
        }
    }

    pub fn to_field(&self) -> Result<Field, ZakuError> {
        Ok(Field::new(self.name.clone(), DataType::Boolean))
    }

    pub fn to_string(&self) -> String {
        format!("{} {} {}", self.l.to_string(), self.op, self.r.to_string())
    }

    pub fn to_physical_expr(&self, input: &LogicalPlan) -> Result<PhysicalExpr, ZakuError> {
        get_datatype(&self.l, &self.r, input)?;
        let l = self.l.to_physical_expr(input)?;
        let r = self.r.to_physical_expr(input)?;
        let op = BooleanOp::from_str(&self.op)?;
        Ok(PhysicalExpr::BooleanExpr(
            physical_plans::binary_expr::BooleanExpr::new(Box::new(l), op, Box::new(r)),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct MathExpr {
    name: String,
    l: Box<LogicalExpr>,
    op: String,
    r: Box<LogicalExpr>,
}

impl MathExpr {
    pub fn new(name: String, l: LogicalExpr, op: String, r: LogicalExpr) -> MathExpr {
        MathExpr {
            name,
            l: Box::new(l),
            op,
            r: Box::new(r),
        }
    }

    pub fn to_field(&self, input: &LogicalPlan) -> Result<Field, ZakuError> {
        let datatype = get_datatype(&self.l, &self.r, input)?;
        Ok(Field::new(self.name.clone(), datatype))
    }

    pub fn to_string(&self) -> String {
        format!("{} {} {}", self.l.to_string(), self.op, self.r.to_string())
    }

    pub fn to_physical_expr(&self, input: &LogicalPlan) -> Result<PhysicalExpr, ZakuError> {
        let l = self.l.to_physical_expr(input)?;
        let r = self.r.to_physical_expr(input)?;

        let _ = get_datatype(&self.l, &self.r, input)?;
        let op = physical_plans::binary_expr::MathOp::from_str(&self.op)?;
        Ok(PhysicalExpr::MathExpr(
            physical_plans::binary_expr::MathExpr::new(Box::new(l), op, Box::new(r)),
        ))
    }
}

fn get_datatype(
    l: &Box<LogicalExpr>,
    r: &Box<LogicalExpr>,
    input: &LogicalPlan,
) -> Result<DataType, ZakuError> {
    let l_field = l.as_ref().to_field(input)?;
    let r_field = r.as_ref().to_field(input)?;
    let l_datatype = l_field.datatype();
    let r_datatype = r_field.datatype();
    let err = Err(ZakuError::new("Datatypes do not match".to_string()));

    match l_datatype {
        DataType::Integer => match r_datatype {
            DataType::Integer => Ok(DataType::Integer),
            DataType::Float => Ok(DataType::Float),
            _ => err,
        },
        DataType::Float => match r_datatype {
            DataType::Integer => Ok(DataType::Float),
            DataType::Float => Ok(DataType::Float),
            _ => err,
        },
        DataType::Text => match r_datatype {
            DataType::Text => Ok(DataType::Text),
            _ => err,
        },
        DataType::Boolean => match r_datatype {
            DataType::Boolean => Ok(DataType::Boolean),
            _ => err,
        },
    }
}
