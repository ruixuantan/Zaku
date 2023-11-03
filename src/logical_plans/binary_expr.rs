use sqlparser::ast::BinaryOperator;

use crate::{
    datatypes::{schema::Field, types::DataType},
    error::ZakuError,
    physical_plans::{self, physical_expr::PhysicalExpr},
    sql::operators::{BinaryOp, BooleanOp, MathOp},
};

use super::{logical_expr::LogicalExpr, logical_plan::LogicalPlan};

pub trait BinaryExprTrait {
    fn to_field(&self, input: &LogicalPlan) -> Result<Field, ZakuError>;

    fn to_string(&self) -> String;

    fn to_physical_expr(&self, input: &LogicalPlan) -> Result<PhysicalExpr, ZakuError>;
}

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
    pub fn new(l: LogicalExpr, op: &BinaryOperator, r: LogicalExpr) -> Result<Self, ZakuError> {
        match op {
            BinaryOperator::And => Ok(BinaryExpr::And(BooleanExpr::new(l, BooleanOp::And, r))),
            BinaryOperator::Or => Ok(BinaryExpr::Or(BooleanExpr::new(l, BooleanOp::Or, r))),
            BinaryOperator::Eq => Ok(BinaryExpr::Eq(BooleanExpr::new(l, BooleanOp::Eq, r))),
            BinaryOperator::NotEq => Ok(BinaryExpr::Neq(BooleanExpr::new(l, BooleanOp::Neq, r))),
            BinaryOperator::Gt => Ok(BinaryExpr::Gt(BooleanExpr::new(l, BooleanOp::Gt, r))),
            BinaryOperator::GtEq => Ok(BinaryExpr::Gte(BooleanExpr::new(l, BooleanOp::Gte, r))),
            BinaryOperator::Lt => Ok(BinaryExpr::Lt(BooleanExpr::new(l, BooleanOp::Lt, r))),
            BinaryOperator::LtEq => Ok(BinaryExpr::Lte(BooleanExpr::new(l, BooleanOp::Lte, r))),
            BinaryOperator::Plus => Ok(BinaryExpr::Add(MathExpr::new(l, MathOp::Add, r))),
            BinaryOperator::Minus => Ok(BinaryExpr::Sub(MathExpr::new(l, MathOp::Sub, r))),
            BinaryOperator::Multiply => Ok(BinaryExpr::Mul(MathExpr::new(l, MathOp::Mul, r))),
            BinaryOperator::Divide => Ok(BinaryExpr::Div(MathExpr::new(l, MathOp::Div, r))),
            BinaryOperator::Modulo => Ok(BinaryExpr::Mod(MathExpr::new(l, MathOp::Mod, r))),
            _ => Err(ZakuError::new("Invalid operator")),
        }
    }
}

impl BinaryExprTrait for BinaryExpr {
    fn to_field(&self, input: &LogicalPlan) -> Result<Field, ZakuError> {
        match self {
            BinaryExpr::And(expr) => expr.to_field(input),
            BinaryExpr::Or(expr) => expr.to_field(input),
            BinaryExpr::Eq(expr) => expr.to_field(input),
            BinaryExpr::Neq(expr) => expr.to_field(input),
            BinaryExpr::Gt(expr) => expr.to_field(input),
            BinaryExpr::Gte(expr) => expr.to_field(input),
            BinaryExpr::Lt(expr) => expr.to_field(input),
            BinaryExpr::Lte(expr) => expr.to_field(input),
            BinaryExpr::Add(expr) => expr.to_field(input),
            BinaryExpr::Sub(expr) => expr.to_field(input),
            BinaryExpr::Mul(expr) => expr.to_field(input),
            BinaryExpr::Div(expr) => expr.to_field(input),
            BinaryExpr::Mod(expr) => expr.to_field(input),
        }
    }

    fn to_string(&self) -> String {
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

    fn to_physical_expr(&self, input: &LogicalPlan) -> Result<PhysicalExpr, ZakuError> {
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
    l: Box<LogicalExpr>,
    op: BooleanOp,
    r: Box<LogicalExpr>,
}

impl BooleanExpr {
    fn new(l: LogicalExpr, op: BooleanOp, r: LogicalExpr) -> BooleanExpr {
        BooleanExpr {
            l: Box::new(l),
            op,
            r: Box::new(r),
        }
    }
}

impl BinaryExprTrait for BooleanExpr {
    fn to_field(&self, _input: &LogicalPlan) -> Result<Field, ZakuError> {
        Ok(Field::new(self.op.name(), DataType::Boolean))
    }

    fn to_string(&self) -> String {
        format!("{} {} {}", self.l, self.op.to_string(), self.r)
    }

    fn to_physical_expr(&self, input: &LogicalPlan) -> Result<PhysicalExpr, ZakuError> {
        get_datatype(&self.l, &self.r, input)?;
        let l = self.l.to_physical_expr(input)?;
        let r = self.r.to_physical_expr(input)?;
        Ok(PhysicalExpr::BooleanExpr(
            physical_plans::binary_expr::BooleanExpr::new(Box::new(l), self.op, Box::new(r)),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct MathExpr {
    l: Box<LogicalExpr>,
    op: MathOp,
    r: Box<LogicalExpr>,
}

impl MathExpr {
    fn new(l: LogicalExpr, op: MathOp, r: LogicalExpr) -> MathExpr {
        MathExpr {
            l: Box::new(l),
            op,
            r: Box::new(r),
        }
    }
}

impl BinaryExprTrait for MathExpr {
    fn to_field(&self, input: &LogicalPlan) -> Result<Field, ZakuError> {
        let datatype = get_datatype(&self.l, &self.r, input)?;
        Ok(Field::new(self.op.name(), datatype))
    }

    fn to_string(&self) -> String {
        format!("{} {} {}", self.l, self.op.to_string(), self.r)
    }

    fn to_physical_expr(&self, input: &LogicalPlan) -> Result<PhysicalExpr, ZakuError> {
        let l = self.l.to_physical_expr(input)?;
        let r = self.r.to_physical_expr(input)?;

        Ok(PhysicalExpr::MathExpr(
            physical_plans::binary_expr::MathExpr::new(Box::new(l), self.op, Box::new(r)),
        ))
    }
}

fn get_datatype(
    l: &LogicalExpr,
    r: &LogicalExpr,
    input: &LogicalPlan,
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
