use sqlparser::{
    ast::Expr,
    ast::Select,
    ast::Statement,
    ast::{BinaryOperator, SelectItem},
};

use crate::{
    error::ZakuError,
    logical_plans::{
        binary_expr::{BinaryExpr, BooleanExpr, MathExpr},
        dataframe::Dataframe,
        logical_expr::{Column, LogicalExpr},
    },
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

fn parse_projection(
    select: &Box<Select>,
) -> Result<(Vec<LogicalExpr>, Vec<Option<String>>), ZakuError> {
    let mut aliases = vec![];
    let logical_expr = select
        .projection
        .iter()
        .filter(|item| match item {
            SelectItem::UnnamedExpr(_) => true,
            SelectItem::ExprWithAlias { expr: _, alias: _ } => true,
            _ => false,
        })
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => {
                aliases.push(None);
                parse_expr(expr)
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                aliases.push(Some(alias.value.clone()));
                parse_expr(expr)
            }
            _ => panic!("Non unnamed expressions should have been filtered"),
        })
        .collect::<Result<Vec<LogicalExpr>, ZakuError>>()?;
    Ok((logical_expr, aliases))
}

fn parse_expr(expr: &Expr) -> Result<LogicalExpr, ZakuError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let l = parse_expr(left)?;
            let r = parse_expr(right)?;
            match op {
                BinaryOperator::Plus => Ok(LogicalExpr::BinaryExpr(BinaryExpr::Add(
                    MathExpr::new("add".to_string(), l, "+".to_string(), r),
                ))),
                BinaryOperator::Minus => Ok(LogicalExpr::BinaryExpr(BinaryExpr::Sub(
                    MathExpr::new("sub".to_string(), l, "-".to_string(), r),
                ))),
                BinaryOperator::Multiply => Ok(LogicalExpr::BinaryExpr(BinaryExpr::Mul(
                    MathExpr::new("mul".to_string(), l, "*".to_string(), r),
                ))),
                BinaryOperator::Divide => Ok(LogicalExpr::BinaryExpr(BinaryExpr::Div(
                    MathExpr::new("div".to_string(), l, "/".to_string(), r),
                ))),
                BinaryOperator::Modulo => Ok(LogicalExpr::BinaryExpr(BinaryExpr::Mod(
                    MathExpr::new("mod".to_string(), l, "%".to_string(), r),
                ))),
                BinaryOperator::And => Ok(LogicalExpr::BinaryExpr(BinaryExpr::And(
                    BooleanExpr::new("and".to_string(), l, "AND".to_string(), r),
                ))),
                BinaryOperator::Or => Ok(LogicalExpr::BinaryExpr(BinaryExpr::Or(
                    BooleanExpr::new("or".to_string(), l, "OR".to_string(), r),
                ))),
                BinaryOperator::Gt => Ok(LogicalExpr::BinaryExpr(BinaryExpr::Gt(
                    BooleanExpr::new("gt".to_string(), l, ">".to_string(), r),
                ))),
                BinaryOperator::GtEq => Ok(LogicalExpr::BinaryExpr(BinaryExpr::Gte(
                    BooleanExpr::new("gte".to_string(), l, ">=".to_string(), r),
                ))),
                BinaryOperator::Lt => Ok(LogicalExpr::BinaryExpr(BinaryExpr::Lt(
                    BooleanExpr::new("lt".to_string(), l, "<".to_string(), r),
                ))),
                BinaryOperator::LtEq => Ok(LogicalExpr::BinaryExpr(BinaryExpr::Lte(
                    BooleanExpr::new("lte".to_string(), l, "<=".to_string(), r),
                ))),
                BinaryOperator::Eq => Ok(LogicalExpr::BinaryExpr(BinaryExpr::Eq(
                    BooleanExpr::new("eq".to_string(), l, "=".to_string(), r),
                ))),
                BinaryOperator::NotEq => Ok(LogicalExpr::BinaryExpr(BinaryExpr::Neq(
                    BooleanExpr::new("neq".to_string(), l, "<>".to_string(), r),
                ))),
                _ => Err(ZakuError::new("Unsupported operator".to_string())),
            }
        }
        Expr::Identifier(ident) => Ok(LogicalExpr::Column(Column::new(ident.value.clone()))),
        Expr::Value(value) => match value {
            sqlparser::ast::Value::Boolean(b) => Ok(LogicalExpr::LiteralBoolean(*b)),
            sqlparser::ast::Value::Number(n, _) => Ok(LogicalExpr::LiteralFloat(
                n.parse::<f32>().expect("Value should be a float"),
            )),
            sqlparser::ast::Value::SingleQuotedString(s) => Ok(LogicalExpr::LiteralText(s.clone())),
            _ => Err(ZakuError::new("Unsupported value".to_string())),
        },
        Expr::Nested(expr) => parse_expr(expr),
        _ => Err(ZakuError::new("Unsupported expression".to_string())),
    }
}

fn create_df(select: &Box<Select>, dataframe: Dataframe) -> Result<Dataframe, ZakuError> {
    let mut df = dataframe;
    let selection = select.selection.as_ref().map(|expr| parse_expr(expr));
    if let Some(selection) = selection {
        df = df.filter(selection?)?;
    }

    let (projections, aliases) = parse_projection(select)?;
    if projections.len() > 0 {
        df = df.projection(projections, aliases)?;
    }

    Ok(df)
}

pub fn parse(sql: &str, df: Dataframe) -> Result<Dataframe, ZakuError> {
    let select = parse_select(sql)?;
    let df = create_df(&select, df)?;
    Ok(df)
}
