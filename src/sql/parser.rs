use sqlparser::{ast::Expr, ast::Select, ast::SelectItem, ast::Statement};

use crate::{
    error::ZakuError,
    logical_plans::{
        binary_expr::BinaryExprs,
        dataframe::Dataframe,
        logical_expr::{Column, LogicalExprs},
    },
};

struct SqlStmt {
    body: Box<Select>,
    limit: Option<usize>,
}

impl SqlStmt {
    fn new(body: Box<Select>, limit: Option<usize>) -> Self {
        Self { body, limit }
    }
}

fn parse_select(sql: &str) -> Result<SqlStmt, ZakuError> {
    let dialect = sqlparser::dialect::GenericDialect {};
    let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql)?;
    match &ast[0] {
        Statement::Query(query) => {
            let limit = match &query.limit {
                Some(sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(num, _))) => {
                    Ok(Some(
                        num.parse::<usize>()
                            .unwrap_or_else(|_| panic!("{num} should be a number")),
                    ))
                }
                Some(_) => Err(ZakuError::new("Limit should be a number")),
                _ => Ok(None),
            };

            let body = match &*query.body {
                sqlparser::ast::SetExpr::Select(s) => Ok(s.clone()),
                _ => Err(ZakuError::new("Not a select query")),
            };

            Ok(SqlStmt::new(body?, limit?))
        }
        _ => Err(ZakuError::new("Not a query")),
    }
}

fn parse_projection(select: &Select) -> Result<Vec<LogicalExprs>, ZakuError> {
    let logical_expr = select
        .projection
        .iter()
        .filter(|item| {
            matches!(
                item,
                SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { expr: _, alias: _ }
            )
        })
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => parse_expr(expr),
            SelectItem::ExprWithAlias { expr, alias } => {
                parse_expr(expr).map(|e| e.set_alias(alias.value.clone()))
            }
            _ => panic!("Non unnamed expressions should have been filtered"),
        })
        .collect::<Result<Vec<LogicalExprs>, ZakuError>>()?;
    Ok(logical_expr)
}

fn parse_expr(expr: &Expr) -> Result<LogicalExprs, ZakuError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let l = parse_expr(left)?;
            let r = parse_expr(right)?;
            Ok(LogicalExprs::BinaryExpr(BinaryExprs::new(l, op, r)?, None))
        }
        Expr::Identifier(ident) => Ok(LogicalExprs::Column(Column::new(ident.value.clone()), None)),
        Expr::Value(value) => match value {
            sqlparser::ast::Value::Boolean(b) => Ok(LogicalExprs::LiteralBoolean(*b, None)),
            sqlparser::ast::Value::Number(n, _) => Ok(LogicalExprs::LiteralFloat(
                n.parse::<f32>().expect("Value should be a float"),
                None,
            )),
            sqlparser::ast::Value::SingleQuotedString(s) => {
                Ok(LogicalExprs::LiteralText(s.clone(), None))
            }
            _ => Err(ZakuError::new("Unsupported value")),
        },
        Expr::Nested(expr) => parse_expr(expr),
        _ => Err(ZakuError::new("Unsupported expression")),
    }
}

fn create_df(select: &SqlStmt, dataframe: Dataframe) -> Result<Dataframe, ZakuError> {
    let mut df = dataframe;
    let selection = select.body.selection.as_ref().map(parse_expr);
    if let Some(selection) = selection {
        df = df.filter(selection?)?;
    }

    let projections = parse_projection(&select.body)?;
    if !projections.is_empty() {
        df = df.projection(projections)?;
    }

    if let Some(limit) = select.limit {
        df = df.limit(limit)?;
    }

    Ok(df)
}

pub fn parse(sql: &str, df: Dataframe) -> Result<Dataframe, ZakuError> {
    let select = parse_select(sql)?;
    let df = create_df(&select, df)?;
    Ok(df)
}
