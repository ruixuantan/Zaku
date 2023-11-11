#![feature(stmt_expr_attributes)]
#![feature(proc_macro_hygiene)]
#![feature(coroutines)]

mod datasources;
mod datatypes;
mod error;
mod execute;
mod logical_plans;
mod physical_plans;
mod sql;

pub use datasources::datasink::Datasink;
pub use error::ZakuError;
pub use execute::execute;
pub use logical_plans::dataframe::Dataframe;
