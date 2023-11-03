use sqlparser::ast::BinaryOperator;

use crate::{
    datatypes::{schema::Field, types::DataType},
    error::ZakuError,
    physical_plans::{self, physical_expr::PhysicalExprs},
    sql::operators::{BinaryOp, BooleanOp, MathOp},
};

use super::{
    logical_expr::{LogicalExpr, LogicalExprs},
    logical_plan::LogicalPlans,
};

pub trait BinaryExpr {
    fn to_string(&self) -> String;
}

#[derive(Debug, Clone)]
pub enum BinaryExprs {
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

impl BinaryExprs {
    pub fn new(l: LogicalExprs, op: &BinaryOperator, r: LogicalExprs) -> Result<Self, ZakuError> {
        match op {
            BinaryOperator::And => Ok(BinaryExprs::And(BooleanExpr::new(l, BooleanOp::And, r))),
            BinaryOperator::Or => Ok(BinaryExprs::Or(BooleanExpr::new(l, BooleanOp::Or, r))),
            BinaryOperator::Eq => Ok(BinaryExprs::Eq(BooleanExpr::new(l, BooleanOp::Eq, r))),
            BinaryOperator::NotEq => Ok(BinaryExprs::Neq(BooleanExpr::new(l, BooleanOp::Neq, r))),
            BinaryOperator::Gt => Ok(BinaryExprs::Gt(BooleanExpr::new(l, BooleanOp::Gt, r))),
            BinaryOperator::GtEq => Ok(BinaryExprs::Gte(BooleanExpr::new(l, BooleanOp::Gte, r))),
            BinaryOperator::Lt => Ok(BinaryExprs::Lt(BooleanExpr::new(l, BooleanOp::Lt, r))),
            BinaryOperator::LtEq => Ok(BinaryExprs::Lte(BooleanExpr::new(l, BooleanOp::Lte, r))),
            BinaryOperator::Plus => Ok(BinaryExprs::Add(MathExpr::new(l, MathOp::Add, r))),
            BinaryOperator::Minus => Ok(BinaryExprs::Sub(MathExpr::new(l, MathOp::Sub, r))),
            BinaryOperator::Multiply => Ok(BinaryExprs::Mul(MathExpr::new(l, MathOp::Mul, r))),
            BinaryOperator::Divide => Ok(BinaryExprs::Div(MathExpr::new(l, MathOp::Div, r))),
            BinaryOperator::Modulo => Ok(BinaryExprs::Mod(MathExpr::new(l, MathOp::Mod, r))),
            _ => Err(ZakuError::new("Invalid operator")),
        }
    }
}

impl BinaryExpr for BinaryExprs {
    fn to_string(&self) -> String {
        match self {
            BinaryExprs::And(expr) => expr.to_string(),
            BinaryExprs::Or(expr) => expr.to_string(),
            BinaryExprs::Eq(expr) => expr.to_string(),
            BinaryExprs::Neq(expr) => expr.to_string(),
            BinaryExprs::Gt(expr) => expr.to_string(),
            BinaryExprs::Gte(expr) => expr.to_string(),
            BinaryExprs::Lt(expr) => expr.to_string(),
            BinaryExprs::Lte(expr) => expr.to_string(),
            BinaryExprs::Add(expr) => expr.to_string(),
            BinaryExprs::Sub(expr) => expr.to_string(),
            BinaryExprs::Mul(expr) => expr.to_string(),
            BinaryExprs::Div(expr) => expr.to_string(),
            BinaryExprs::Mod(expr) => expr.to_string(),
        }
    }
}

impl LogicalExpr for BinaryExprs {
    fn to_field(&self, input: &LogicalPlans) -> Result<Field, ZakuError> {
        match self {
            BinaryExprs::And(expr) => expr.to_field(input),
            BinaryExprs::Or(expr) => expr.to_field(input),
            BinaryExprs::Eq(expr) => expr.to_field(input),
            BinaryExprs::Neq(expr) => expr.to_field(input),
            BinaryExprs::Gt(expr) => expr.to_field(input),
            BinaryExprs::Gte(expr) => expr.to_field(input),
            BinaryExprs::Lt(expr) => expr.to_field(input),
            BinaryExprs::Lte(expr) => expr.to_field(input),
            BinaryExprs::Add(expr) => expr.to_field(input),
            BinaryExprs::Sub(expr) => expr.to_field(input),
            BinaryExprs::Mul(expr) => expr.to_field(input),
            BinaryExprs::Div(expr) => expr.to_field(input),
            BinaryExprs::Mod(expr) => expr.to_field(input),
        }
    }

    fn to_physical_expr(&self, input: &LogicalPlans) -> Result<PhysicalExprs, ZakuError> {
        match self {
            BinaryExprs::And(expr) => expr.to_physical_expr(input),
            BinaryExprs::Or(expr) => expr.to_physical_expr(input),
            BinaryExprs::Eq(expr) => expr.to_physical_expr(input),
            BinaryExprs::Neq(expr) => expr.to_physical_expr(input),
            BinaryExprs::Gt(expr) => expr.to_physical_expr(input),
            BinaryExprs::Gte(expr) => expr.to_physical_expr(input),
            BinaryExprs::Lt(expr) => expr.to_physical_expr(input),
            BinaryExprs::Lte(expr) => expr.to_physical_expr(input),
            BinaryExprs::Add(expr) => expr.to_physical_expr(input),
            BinaryExprs::Sub(expr) => expr.to_physical_expr(input),
            BinaryExprs::Mul(expr) => expr.to_physical_expr(input),
            BinaryExprs::Div(expr) => expr.to_physical_expr(input),
            BinaryExprs::Mod(expr) => expr.to_physical_expr(input),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BooleanExpr {
    l: Box<LogicalExprs>,
    op: BooleanOp,
    r: Box<LogicalExprs>,
}

impl BooleanExpr {
    fn new(l: LogicalExprs, op: BooleanOp, r: LogicalExprs) -> BooleanExpr {
        BooleanExpr {
            l: Box::new(l),
            op,
            r: Box::new(r),
        }
    }
}

impl BinaryExpr for BooleanExpr {
    fn to_string(&self) -> String {
        format!("{} {} {}", self.l, self.op.to_string(), self.r)
    }
}

impl LogicalExpr for BooleanExpr {
    fn to_field(&self, _input: &LogicalPlans) -> Result<Field, ZakuError> {
        Ok(Field::new(self.op.name(), DataType::Boolean))
    }

    fn to_physical_expr(&self, input: &LogicalPlans) -> Result<PhysicalExprs, ZakuError> {
        get_datatype(&self.l, &self.r, input)?;
        let l = self.l.to_physical_expr(input)?;
        let r = self.r.to_physical_expr(input)?;
        Ok(PhysicalExprs::BooleanExpr(
            physical_plans::binary_expr::BooleanExpr::new(Box::new(l), self.op, Box::new(r)),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct MathExpr {
    l: Box<LogicalExprs>,
    op: MathOp,
    r: Box<LogicalExprs>,
}

impl MathExpr {
    fn new(l: LogicalExprs, op: MathOp, r: LogicalExprs) -> MathExpr {
        MathExpr {
            l: Box::new(l),
            op,
            r: Box::new(r),
        }
    }
}

impl BinaryExpr for MathExpr {
    fn to_string(&self) -> String {
        format!("{} {} {}", self.l, self.op.to_string(), self.r)
    }
}

impl LogicalExpr for MathExpr {
    fn to_field(&self, input: &LogicalPlans) -> Result<Field, ZakuError> {
        let datatype = get_datatype(&self.l, &self.r, input)?;
        Ok(Field::new(self.op.name(), datatype))
    }

    fn to_physical_expr(&self, input: &LogicalPlans) -> Result<PhysicalExprs, ZakuError> {
        let l = self.l.to_physical_expr(input)?;
        let r = self.r.to_physical_expr(input)?;

        Ok(PhysicalExprs::MathExpr(
            physical_plans::binary_expr::MathExpr::new(Box::new(l), self.op, Box::new(r)),
        ))
    }
}

fn get_datatype(
    l: &LogicalExprs,
    r: &LogicalExprs,
    input: &LogicalPlans,
) -> Result<DataType, ZakuError> {
    let l_field = l.to_field(input)?;
    let r_field = r.to_field(input)?;
    let l_datatype = l_field.datatype();
    let r_datatype = r_field.datatype();
    let err = Err(ZakuError::new("Datatypes do not match"));

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
