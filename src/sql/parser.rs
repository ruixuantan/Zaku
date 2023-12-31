use std::{ops::Deref, str::FromStr};

use crate::{
    datatypes::types::parse_iso_date_from_str,
    error::ZakuError,
    logical_plans::{
        aggregate_expr::AggregateExprs,
        binary_expr::BinaryExprs,
        dataframe::Dataframe,
        logical_expr::{AliasExpr, Column, LogicalExprs},
    },
};
use bigdecimal::BigDecimal;
use sqlparser::{
    ast::Expr,
    ast::Select,
    ast::{
        CopySource, CopyTarget, Function, FunctionArg, FunctionArgExpr, GroupByExpr, ObjectName,
        OrderByExpr, Statement,
    },
    ast::{Query, SelectItem},
};

use super::stmt::{SelectStmt, Stmt};

fn parse_select(query: &Query) -> Result<SelectStmt, ZakuError> {
    let limit = match &query.limit {
        Some(sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(num, _))) => Ok(Some(
            num.parse::<usize>()
                .unwrap_or_else(|_| panic!("{num} should be a number")),
        )),
        Some(_) => Err(ZakuError::new("Limit should be a positive number")),
        _ => Ok(None),
    };

    let body = match &*query.body {
        sqlparser::ast::SetExpr::Select(s) => Ok(s.clone()),
        _ => Err(ZakuError::new("Not a select query")),
    };

    let order_by = query.order_by.clone();

    Ok(SelectStmt::new(body?, limit?, order_by))
}

fn parse_projection(select: &Select) -> Result<Vec<LogicalExprs>, ZakuError> {
    let projections = select
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

    Ok(projections)
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
                _ => Err(ZakuError::new(
                    "Only column names in aggregate functions are supported",
                )),
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
            sqlparser::ast::Value::Number(n, _) => {
                Ok(LogicalExprs::LiteralNumber(BigDecimal::from_str(n)?))
            }
            sqlparser::ast::Value::SingleQuotedString(s) => match parse_iso_date_from_str(s) {
                Ok(date) => Ok(LogicalExprs::LiteralDate(date)),
                Err(_) => Ok(LogicalExprs::LiteralText(s.clone())),
            },
            _ => Err(ZakuError::new("Unsupported value")),
        },
        Expr::Nested(expr) => parse_expr(expr),
        Expr::Function(func) => parse_aggregate_function(func),
        _ => Err(ZakuError::new("Unsupported expression")),
    }
}

fn parse_order_by(exprs: &[OrderByExpr]) -> Result<(Vec<LogicalExprs>, Vec<bool>), ZakuError> {
    let mut order_by_exprs = vec![];
    let mut asc = vec![];
    let _ = exprs.iter().try_for_each(|expr| {
        let logical_expr = parse_expr(&expr.expr)?;
        order_by_exprs.push(logical_expr);
        asc.push(expr.asc.unwrap_or(true));
        Ok::<(), ZakuError>(())
    });
    Ok((order_by_exprs, asc))
}

fn retrieve_aggregate_col_idx(
    group_by_size: usize,
    expr: &LogicalExprs,
    aggregates: &Vec<AggregateExprs>,
) -> LogicalExprs {
    match expr {
        LogicalExprs::AggregateExpr(expr) => {
            let idx = aggregates
                .iter()
                .position(|e| e == expr)
                .expect("Aggregate expr should be found within aggregates");
            LogicalExprs::ColumnIndex(idx + group_by_size)
        }
        LogicalExprs::AliasExpr(alias) => {
            let aggr = retrieve_aggregate_col_idx(group_by_size, alias.expr(), aggregates);
            LogicalExprs::AliasExpr(AliasExpr::new(aggr, alias.alias().clone()))
        }
        LogicalExprs::BinaryExpr(binary_expr) => {
            let l = retrieve_aggregate_col_idx(group_by_size, binary_expr.get_l(), aggregates);
            let r = retrieve_aggregate_col_idx(group_by_size, binary_expr.get_r(), aggregates);
            LogicalExprs::BinaryExpr(BinaryExprs::new(l, &binary_expr.get_op(), r).unwrap())
        }

        _ => expr.clone(),
    }
}

// Convert aggregate functions to column indexes for the projections
// After a group by aggregation, the schema starts first with the group by columns
// followed by the aggregate columns
// As such, we need to offset the aggregate column indexes by the number of group by columns
fn get_aggregate_indexes(
    group_by_size: usize,
    projections: Vec<LogicalExprs>,
    aggregates: &Vec<AggregateExprs>,
) -> Result<Vec<LogicalExprs>, ZakuError> {
    if projections.is_empty() {
        return Err(ZakuError::new(
            "Group by queries must have at least one projection",
        ));
    }
    Ok(projections
        .iter()
        .map(|expr| retrieve_aggregate_col_idx(group_by_size, expr, aggregates))
        .collect())
}

fn create_df(select: &SelectStmt, dataframe: Dataframe) -> Result<Dataframe, ZakuError> {
    let mut df = dataframe;

    // parse where clause
    if let Some(selection) = select.body.selection.as_ref().map(parse_expr) {
        if !selection?.as_aggregate().is_empty() {
            return Err(ZakuError::new(
                "WHERE clause cannot contain aggregate functions",
            ));
        }
    }
    let selection = select.body.selection.as_ref().map(parse_expr);
    if let Some(selection) = selection {
        df = df.filter(selection?)?;
    }

    // handle GROUP BY + HAVING and aggregates
    let projections = parse_projection(&select.body)?;
    let mut aggregates: Vec<AggregateExprs> = projections
        .iter()
        .flat_map(|expr| expr.as_aggregate())
        .collect();
    if let Some(have) = select.body.having.as_ref().map(parse_expr) {
        let have_aggregates: Vec<AggregateExprs> = have?
            .as_aggregate()
            .iter()
            .filter(|expr| !aggregates.contains(expr))
            .cloned()
            .collect();
        aggregates.extend(have_aggregates);
    }

    let group_by_exprs = parse_group_by(&select.body.group_by)?;

    // no group by clause and no aggregate functions in SELECT
    if group_by_exprs.is_empty() && aggregates.is_empty() {
        if !select.order_by.is_empty() {
            let (order_by_exprs, asc) = parse_order_by(&select.order_by)?;
            df = df.sort(order_by_exprs, asc)?;
        }

        if !projections.is_empty() {
            df = df.projection(projections)?;
        }

        if let Some(limit) = select.limit {
            df = df.limit(limit)?;
        }
        return Ok(df);
    }

    let group_by_size = group_by_exprs.len();
    let aggr_projections = get_aggregate_indexes(group_by_size, projections, &aggregates)?;
    df = df.aggregate(group_by_exprs, aggregates.clone())?;

    let having = select.body.having.as_ref().map(parse_expr);
    if let Some(have) = having {
        let aggr_havings = retrieve_aggregate_col_idx(group_by_size, &have?, &aggregates);
        df = df.filter(aggr_havings)?;
    }

    if !select.order_by.is_empty() {
        let (order_by_exprs, asc) = parse_order_by(&select.order_by)?;
        df = df.sort(order_by_exprs, asc)?;
    }

    df = df.projection(aggr_projections)?;

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
            _ => Err(ZakuError::new("Only SELECT queries are supported")),
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
