use std::ops::Deref;

use sqlparser::{
    ast::Expr,
    ast::Select,
    ast::{
        CopySource, CopyTarget, Function, FunctionArg, FunctionArgExpr, GroupByExpr, ObjectName,
        Statement,
    },
    ast::{Query, SelectItem},
};

use crate::{
    error::ZakuError,
    logical_plans::{
        aggregate_expr::AggregateExprs,
        binary_expr::BinaryExprs,
        dataframe::Dataframe,
        logical_expr::{AliasExpr, Column, LogicalExprs},
    },
};

use super::stmt::{SelectStmt, Stmt};

fn parse_select(query: &Query) -> Result<SelectStmt, ZakuError> {
    let limit = match &query.limit {
        Some(sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(num, _))) => Ok(Some(
            num.parse::<usize>()
                .unwrap_or_else(|_| panic!("{num} should be a number")),
        )),
        Some(_) => Err(ZakuError::new("Limit should be a number")),
        _ => Ok(None),
    };

    let body = match &*query.body {
        sqlparser::ast::SetExpr::Select(s) => Ok(s.clone()),
        _ => Err(ZakuError::new("Not a select query")),
    };

    Ok(SelectStmt::new(body?, limit?))
}

fn parse_projection(
    select: &Select,
) -> Result<(Vec<LogicalExprs>, Vec<AggregateExprs>), ZakuError> {
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
            SelectItem::ExprWithAlias { expr, alias } => parse_expr(expr)
                .map(|e| LogicalExprs::AliasExpr(AliasExpr::new(e, alias.value.clone()))),
            _ => panic!("Non unnamed expressions should have been filtered"),
        })
        .collect::<Result<Vec<LogicalExprs>, ZakuError>>()?;

    let projections = logical_expr
        .iter()
        .filter(|expr| !expr.is_aggregate())
        .cloned()
        .collect();
    let aggregations = logical_expr
        .iter()
        .filter(|expr| expr.is_aggregate())
        .map(|expr| expr.as_aggregate())
        .collect();

    Ok((projections, aggregations))
}

fn parse_group_by(expr: &GroupByExpr) -> Result<Vec<LogicalExprs>, ZakuError> {
    match expr {
        GroupByExpr::Expressions(exprs) => exprs.iter().map(parse_expr).collect(),
        _ => Err(ZakuError::new("Unsupported group by expression")),
    }
}

fn parse_aggregate_function(func: &Function) -> Result<LogicalExprs, ZakuError> {
    let ObjectName(idents) = &func.name;

    let args = func
        .args
        .iter()
        .map(|f| match f {
            FunctionArg::Unnamed(expr) => match expr {
                FunctionArgExpr::Expr(e) => parse_expr(e),
                _ => Err(ZakuError::new("Only column names are supported")),
            },
            FunctionArg::Named { name: _, arg: _ } => {
                Err(ZakuError::new("Named function arguments are not supported"))
            }
        })
        .collect::<Result<Vec<LogicalExprs>, ZakuError>>()?;
    Ok(LogicalExprs::AggregateExpr(AggregateExprs::from_str(
        &idents[0].value,
        args[0].clone(),
    )?))
}

fn parse_expr(expr: &Expr) -> Result<LogicalExprs, ZakuError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let l = parse_expr(left)?;
            let r = parse_expr(right)?;
            Ok(LogicalExprs::BinaryExpr(BinaryExprs::new(l, op, r)?))
        }
        Expr::Identifier(ident) => Ok(LogicalExprs::Column(Column::new(ident.value.clone()))),
        Expr::Value(value) => match value {
            sqlparser::ast::Value::Boolean(b) => Ok(LogicalExprs::LiteralBoolean(*b)),
            sqlparser::ast::Value::Number(n, _) => Ok(LogicalExprs::LiteralFloat(
                n.parse::<f32>().expect("Value should be a float"),
            )),
            sqlparser::ast::Value::SingleQuotedString(s) => {
                Ok(LogicalExprs::LiteralText(s.clone()))
            }
            _ => Err(ZakuError::new("Unsupported value")),
        },
        Expr::Nested(expr) => parse_expr(expr),
        Expr::Function(func) => parse_aggregate_function(func),
        _ => Err(ZakuError::new("Unsupported expression")),
    }
}

fn create_df(select: &SelectStmt, dataframe: Dataframe) -> Result<Dataframe, ZakuError> {
    let mut df = dataframe;
    let selection = select.body.selection.as_ref().map(parse_expr);
    if let Some(selection) = selection {
        df = df.filter(selection?)?;
    }

    let (projections, aggregates) = parse_projection(&select.body)?;

    let group_by_exprs = parse_group_by(&select.body.group_by)?;
    if !group_by_exprs.is_empty() || !aggregates.is_empty() {
        df = df.aggregate(group_by_exprs, aggregates)?;
    }

    if !projections.is_empty() {
        df = df.projection(projections)?;
    }

    if let Some(limit) = select.limit {
        df = df.limit(limit)?;
    }

    Ok(df)
}

fn parse_copy(
    df: Dataframe,
    to: &bool,
    source: &CopySource,
    target: &CopyTarget,
) -> Result<Stmt, ZakuError> {
    if to == &false {
        return Err(ZakuError::new("COPY FROM is not supported"));
    }

    let filename = match target {
        CopyTarget::File { filename } => Ok(filename),
        _ => Err(ZakuError::new("COPY is only supported to csv files")),
    };

    let df = match source {
        CopySource::Query(query) => {
            let select_stmt = parse_select(query)?;
            create_df(&select_stmt, df)
        }
        _ => Err(ZakuError::new("COPY is only supported from SELECT queries")),
    };

    Ok(Stmt::CopyTo(df?, filename?.to_string()))
}

pub fn parse(sql: &str, df: Dataframe) -> Result<Stmt, ZakuError> {
    let dialect = sqlparser::dialect::GenericDialect {};
    let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql)?;

    match &ast[0] {
        Statement::Explain {
            describe_alias: _,
            analyze: _,
            verbose: _,
            statement,
            format: _,
        } => match statement.deref() {
            Statement::Query(query) => {
                let select_stmt = parse_select(query)?;
                let df = create_df(&select_stmt, df)?;
                Ok(Stmt::Explain(df))
            }
            _ => Err(ZakuError::new("Only SELECT queris are supported")),
        },
        Statement::Copy {
            source,
            to,
            target,
            options: _,
            legacy_options: _,
            values: _,
        } => parse_copy(df, to, source, target),
        Statement::Query(query) => {
            let select_stmt = parse_select(query)?;
            let df = create_df(&select_stmt, df)?;
            Ok(Stmt::Select(df))
        }
        _ => Err(ZakuError::new("Only SELECT and EXPLAIN are supported")),
    }
}
