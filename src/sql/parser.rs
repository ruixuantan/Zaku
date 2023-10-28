use std::sync::Arc;

use sqlparser::{ast::Expr, ast::Select, ast::SelectItem, ast::Statement};

use crate::{
    error::ZakuError,
    logical_plans::{dataframe::Dataframe, logical_expr::Column},
};

fn parse_select(sql: &str) -> Result<Box<Select>, ZakuError> {
    let dialect = sqlparser::dialect::GenericDialect {};

    let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql)
        .map_err(|e| ZakuError::new(e.to_string()))?;
    let select_stmt = match &ast[0] {
        Statement::Query(query) => match &*query.body {
            sqlparser::ast::SetExpr::Select(s) => s.clone(),
            _ => return Err(ZakuError::new("Not a select query".to_string())),
        },
        _ => return Err(ZakuError::new("Not a query".to_string())),
    };

    Ok(select_stmt)
}

fn create_df(select: &Box<Select>, df: Dataframe) -> Dataframe {
    let mut projections = vec![];
    select.projection.iter().for_each(|item| match item {
        SelectItem::UnnamedExpr(expr) => match expr {
            Expr::Identifier(ident) => {
                projections.push(Arc::new(Column::new(ident.value.clone())));
            }
            _ => (),
        },
        SelectItem::Wildcard(_) => (),
        _ => (),
    });

    // if projections.len() > 0 {
    //     df.projection(projections);
    // }

    df
}

pub fn parse(sql: &str, df: Dataframe) -> Result<Dataframe, ZakuError> {
    let select = parse_select(sql)?;
    let df = create_df(&select, df);
    Ok(df)
}
