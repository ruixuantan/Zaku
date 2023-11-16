use std::fmt::Display;

use enum_dispatch::enum_dispatch;

use crate::{datatypes::types::Value, ZakuError};

use super::physical_expr::PhysicalExprs;

#[derive(Clone)]
pub enum AggregateExpressions {
    Sum(PhysicalExprs),
    Count(PhysicalExprs),
    Min(PhysicalExprs),
    Max(PhysicalExprs),
    Avg(PhysicalExprs),
}

impl AggregateExpressions {
    pub fn input_expr(&self) -> PhysicalExprs {
        let e = match self {
            AggregateExpressions::Sum(expr) => expr,
            AggregateExpressions::Count(expr) => expr,
            AggregateExpressions::Min(expr) => expr,
            AggregateExpressions::Max(expr) => expr,
            AggregateExpressions::Avg(expr) => expr,
        };
        e.clone()
    }

    pub fn create_accumulator(&self) -> Accumulators {
        match self {
            AggregateExpressions::Sum(_) => Accumulators::Sum(Sum::new()),
            AggregateExpressions::Count(_) => Accumulators::Count(Count::new()),
            AggregateExpressions::Min(_) => Accumulators::Min(Min::new()),
            AggregateExpressions::Max(_) => Accumulators::Max(Max::new()),
            AggregateExpressions::Avg(_) => Accumulators::Avg(Avg::new()),
        }
    }
}

impl Display for AggregateExpressions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateExpressions::Sum(e) => write!(f, "sum({})", e),
            AggregateExpressions::Count(e) => write!(f, "count({})", e),
            AggregateExpressions::Min(e) => write!(f, "min({})", e),
            AggregateExpressions::Max(e) => write!(f, "max({})", e),
            AggregateExpressions::Avg(e) => write!(f, "avg({})", e),
        }
    }
}

#[enum_dispatch]
pub trait Accumulator {
    fn accumulate(&mut self, value: &Value) -> Result<(), ZakuError>;

    fn get_value(&self) -> Value;
}

#[enum_dispatch(Accumulator)]
pub enum Accumulators {
    Sum(Sum),
    Count(Count),
    Min(Min),
    Max(Max),
    Avg(Avg),
}

pub struct Sum {
    value: Option<Value>,
}

impl Sum {
    pub fn new() -> Sum {
        Sum { value: None }
    }
}

impl Default for Sum {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for Sum {
    fn accumulate(&mut self, value: &Value) -> Result<(), ZakuError> {
        let err = Err(ZakuError::new("SUM only supports numeric values"));
        match &self.value {
            Some(v) => {
                let new_value = match value {
                    Value::Number(_) => Some(v.add(value)),
                    Value::Null => Some(v.add(&Value::number("0"))),
                    _ => return err,
                };
                self.value = new_value;
            }
            None => {
                match value {
                    Value::Number(_) => self.value = Some(value.clone()),
                    Value::Null => self.value = Some(Value::number("0")),
                    _ => return err,
                };
            }
        }
        Ok(())
    }

    fn get_value(&self) -> Value {
        match &self.value {
            Some(v) => v.clone(),
            None => Value::Null,
        }
    }
}

pub struct Count {
    value: usize,
}

impl Count {
    pub fn new() -> Count {
        Count { value: 0 }
    }
}

impl Default for Count {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for Count {
    fn accumulate(&mut self, _value: &Value) -> Result<(), ZakuError> {
        self.value += 1;
        Ok(())
    }

    fn get_value(&self) -> Value {
        Value::number(self.value.to_string().as_str())
    }
}

pub struct Min {
    value: Option<Value>,
}

impl Min {
    pub fn new() -> Min {
        Min { value: None }
    }
}

impl Default for Min {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for Min {
    fn accumulate(&mut self, value: &Value) -> Result<(), ZakuError> {
        match &self.value {
            Some(v) => match v {
                Value::Number(_) => self.value = Some(v.minimum(value)),
                Value::Date(_) => self.value = Some(v.minimum(value)),
                Value::Null => self.value = Some(value.clone()),
                _ => return Err(ZakuError::new("MIN only supports numeric and date values")),
            },
            None => {
                self.value = Some(value.clone());
            }
        }
        Ok(())
    }

    fn get_value(&self) -> Value {
        match &self.value {
            Some(v) => v.clone(),
            None => Value::Null,
        }
    }
}

pub struct Max {
    value: Option<Value>,
}

impl Max {
    pub fn new() -> Max {
        Max { value: None }
    }
}

impl Default for Max {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for Max {
    fn accumulate(&mut self, value: &Value) -> Result<(), ZakuError> {
        match &self.value {
            Some(v) => match v {
                Value::Number(_) => self.value = Some(v.maximum(value)),
                Value::Date(_) => self.value = Some(v.minimum(value)),
                Value::Null => self.value = Some(value.clone()),
                _ => return Err(ZakuError::new("MAX only supports numeric values")),
            },
            None => {
                self.value = Some(value.clone());
            }
        }
        Ok(())
    }

    fn get_value(&self) -> Value {
        match &self.value {
            Some(v) => v.clone(),
            None => Value::Null,
        }
    }
}

pub struct Avg {
    sum: Option<Value>,
    count: i32,
}

impl Avg {
    pub fn new() -> Avg {
        Avg {
            sum: None,
            count: 0,
        }
    }
}

impl Default for Avg {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for Avg {
    fn accumulate(&mut self, value: &Value) -> Result<(), ZakuError> {
        let err = Err(ZakuError::new("AVG only supports numeric values"));
        match &self.sum {
            Some(v) => {
                let new_value = match value {
                    Value::Number(_) => Some(v.add(value)),
                    Value::Null => Some(v.add(&Value::number("0"))),
                    _ => return err,
                };
                self.sum = new_value;
            }
            None => {
                match value {
                    Value::Number(_) => self.sum = Some(value.clone()),
                    Value::Null => self.sum = Some(Value::number("0")),
                    _ => return err,
                };
            }
        }
        self.count += 1;
        Ok(())
    }

    fn get_value(&self) -> Value {
        match &self.sum {
            Some(v) => v.div(&Value::number(self.count.to_string().as_str())),
            None => Value::Null,
        }
    }
}
