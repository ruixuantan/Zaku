use sqlparser::ast::{OrderByExpr, Select};

use crate::logical_plans::dataframe::Dataframe;

pub enum Stmt {
    Select(Dataframe),
    Explain(Dataframe),
    CopyTo(Dataframe, String),
}

pub struct SelectStmt {
    pub body: Box<Select>,
    pub limit: Option<usize>,
    pub order_by: Vec<OrderByExpr>,
}

impl SelectStmt {
    pub fn new(body: Box<Select>, limit: Option<usize>, order_by: Vec<OrderByExpr>) -> Self {
        Self {
            body,
            limit,
            order_by,
        }
    }
}
