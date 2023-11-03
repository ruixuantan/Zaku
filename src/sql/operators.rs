use sqlparser::ast::BinaryOperator;

use crate::error::ZakuError;

pub trait BinaryOp {
    fn from_sql_parser(op: &BinaryOperator) -> Result<Self, ZakuError>
    where
        Self: Sized;

    fn name(&self) -> String;

    fn to_string(&self) -> String;
}

#[derive(Debug, Clone, Copy)]
pub enum BooleanOp {
    And,
    Or,
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl BinaryOp for BooleanOp {
    fn from_sql_parser(op: &BinaryOperator) -> Result<BooleanOp, ZakuError> {
        match op {
            BinaryOperator::And => Ok(BooleanOp::And),
            BinaryOperator::Or => Ok(BooleanOp::Or),
            BinaryOperator::Eq => Ok(BooleanOp::Eq),
            BinaryOperator::NotEq => Ok(BooleanOp::Neq),
            BinaryOperator::Gt => Ok(BooleanOp::Gt),
            BinaryOperator::GtEq => Ok(BooleanOp::Gte),
            BinaryOperator::Lt => Ok(BooleanOp::Lt),
            BinaryOperator::LtEq => Ok(BooleanOp::Lte),
            _ => Err(ZakuError::new("Invalid boolean operator")),
        }
    }

    fn name(&self) -> String {
        match self {
            BooleanOp::And => "and".to_string(),
            BooleanOp::Or => "or".to_string(),
            BooleanOp::Eq => "eq".to_string(),
            BooleanOp::Neq => "neq".to_string(),
            BooleanOp::Gt => "gt".to_string(),
            BooleanOp::Gte => "gte".to_string(),
            BooleanOp::Lt => "lt".to_string(),
            BooleanOp::Lte => "lte".to_string(),
        }
    }

    fn to_string(&self) -> String {
        match self {
            BooleanOp::And => "AND".to_string(),
            BooleanOp::Or => "OR".to_string(),
            BooleanOp::Eq => "=".to_string(),
            BooleanOp::Neq => "<>".to_string(),
            BooleanOp::Gt => ">".to_string(),
            BooleanOp::Gte => ">=".to_string(),
            BooleanOp::Lt => "<".to_string(),
            BooleanOp::Lte => "<=".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum MathOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl BinaryOp for MathOp {
    fn from_sql_parser(op: &BinaryOperator) -> Result<MathOp, ZakuError> {
        match op {
            BinaryOperator::Plus => Ok(MathOp::Add),
            BinaryOperator::Minus => Ok(MathOp::Sub),
            BinaryOperator::Multiply => Ok(MathOp::Mul),
            BinaryOperator::Divide => Ok(MathOp::Div),
            BinaryOperator::Modulo => Ok(MathOp::Mod),
            _ => Err(ZakuError::new("Invalid math operator")),
        }
    }

    fn name(&self) -> String {
        match self {
            MathOp::Add => "add".to_string(),
            MathOp::Sub => "sub".to_string(),
            MathOp::Mul => "mul".to_string(),
            MathOp::Div => "div".to_string(),
            MathOp::Mod => "mod".to_string(),
        }
    }

    fn to_string(&self) -> String {
        match self {
            MathOp::Add => "+".to_string(),
            MathOp::Sub => "-".to_string(),
            MathOp::Mul => "*".to_string(),
            MathOp::Div => "/".to_string(),
            MathOp::Mod => "%".to_string(),
        }
    }
}
