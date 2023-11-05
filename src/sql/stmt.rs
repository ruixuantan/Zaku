use sqlparser::ast::Select;

use crate::logical_plans::dataframe::Dataframe;

pub enum Stmt {
    Select(Dataframe),
    Explain(Dataframe),
    CopyTo(Dataframe, String),
}

pub struct SelectStmt {
    pub body: Box<Select>,
    pub limit: Option<usize>,
}

impl SelectStmt {
    pub fn new(body: Box<Select>, limit: Option<usize>) -> Self {
        Self { body, limit }
    }
}
