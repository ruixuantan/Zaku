use sqlparser::{ast::Expr, ast::Select, ast::SelectItem, ast::Statement};

use crate::{
    error::ZakuError,
    logical_plans::{
        binary_expr::BinaryExpr,
        dataframe::Dataframe,
        logical_expr::{Column, LogicalExpr},
    },
};

fn parse_select(sql: &str) -> Result<Box<Select>, ZakuError> {
    let dialect = sqlparser::dialect::GenericDialect {};

    let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql)?;
    match &ast[0] {
        Statement::Query(query) => match &*query.body {
            sqlparser::ast::SetExpr::Select(s) => Ok(s.clone()),
            _ => Err(ZakuError::new("Not a select query".to_string())),
        },
        _ => Err(ZakuError::new("Not a query".to_string())),
    }
}

fn parse_projection(select: &Select) -> Result<Vec<LogicalExpr>, ZakuError> {
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
        .collect::<Result<Vec<LogicalExpr>, ZakuError>>()?;
    Ok(logical_expr)
}

fn parse_expr(expr: &Expr) -> Result<LogicalExpr, ZakuError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let l = parse_expr(left)?;
            let r = parse_expr(right)?;
            Ok(LogicalExpr::BinaryExpr(BinaryExpr::new(l, op, r)?, None))
        }
        Expr::Identifier(ident) => Ok(LogicalExpr::Column(Column::new(ident.value.clone()), None)),
        Expr::Value(value) => match value {
            sqlparser::ast::Value::Boolean(b) => Ok(LogicalExpr::LiteralBoolean(*b, None)),
            sqlparser::ast::Value::Number(n, _) => Ok(LogicalExpr::LiteralFloat(
                n.parse::<f32>().expect("Value should be a float"),
                None,
            )),
            sqlparser::ast::Value::SingleQuotedString(s) => {
                Ok(LogicalExpr::LiteralText(s.clone(), None))
            }
            _ => Err(ZakuError::new("Unsupported value".to_string())),
        },
        Expr::Nested(expr) => parse_expr(expr),
        _ => Err(ZakuError::new("Unsupported expression".to_string())),
    }
}

fn create_df(select: &Select, dataframe: Dataframe) -> Result<Dataframe, ZakuError> {
    let mut df = dataframe;
    let selection = select.selection.as_ref().map(parse_expr);
    if let Some(selection) = selection {
        df = df.filter(selection?)?;
    }

    let projections = parse_projection(select)?;
    if !projections.is_empty() {
        df = df.projection(projections)?;
    }

    Ok(df)
}

pub fn parse(sql: &str, df: Dataframe) -> Result<Dataframe, ZakuError> {
    let select = parse_select(sql)?;
    let df = create_df(&select, df)?;
    Ok(df)
}
