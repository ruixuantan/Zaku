use enum_dispatch::enum_dispatch;
use futures_async_stream::try_stream;
use std::{collections::HashMap, fmt::Display, sync::Arc};

use crate::{
    datasources::datasource::Datasource,
    datatypes::{
        column_vector::{ColumnVector, Vector, Vectors},
        record_batch::RecordBatch,
        schema::Schema,
        types::Value,
    },
    physical_plans::accumulator::{Accumulator, Accumulators},
    ZakuError,
};

use super::{
    accumulator::AggregateExpressions,
    physical_expr::{PhysicalExpr, PhysicalExprs},
};

#[enum_dispatch]
pub trait PhysicalPlan {
    fn schema(&self) -> Schema;

    fn children(&self) -> Vec<PhysicalPlans>;

    fn to_string(&self) -> String;
}

#[derive(Clone)]
#[enum_dispatch(PhysicalPlan)]
pub enum PhysicalPlans {
    Scan(ScanExec),
    Projection(ProjectionExec),
    Filter(FilterExec),
    Limit(LimitExec),
    HashAggregate(HashAggregateExec),
    Sort(SortExec),
}

impl PhysicalPlans {
    #[try_stream(boxed, ok = RecordBatch, error = ZakuError)]
    pub async fn execute(&self) {
        let stream = match self {
            PhysicalPlans::Scan(exec) => exec.execute(),
            PhysicalPlans::Projection(exec) => exec.execute(),
            PhysicalPlans::Filter(exec) => exec.execute(),
            PhysicalPlans::Limit(exec) => exec.execute(),
            PhysicalPlans::HashAggregate(exec) => exec.execute(),
            PhysicalPlans::Sort(exec) => exec.execute(),
        };
        #[for_await]
        for res in stream {
            yield res?
        }
    }

    fn format(plan: &PhysicalPlans, indent: usize) -> String {
        let mut s = String::new();
        (0..indent).for_each(|_| s.push_str("  "));
        s.push_str(PhysicalPlan::to_string(plan).as_str());
        s.push('\n');
        plan.children().iter().for_each(|p| {
            s.push_str(PhysicalPlans::format(p, indent + 1).as_str());
        });
        s
    }
}

impl Display for PhysicalPlans {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", PhysicalPlans::format(self, 0))
    }
}

#[derive(Clone)]
pub struct ScanExec {
    datasource: Datasource,
    projection: Vec<String>,
}

impl ScanExec {
    pub fn new(datasource: Datasource, projection: Vec<String>) -> ScanExec {
        ScanExec {
            datasource,
            projection,
        }
    }

    #[try_stream(boxed, ok = RecordBatch, error = ZakuError)]
    pub async fn execute(&self) {
        for rb in self.datasource.get_data() {
            yield rb.clone()
        }
    }
}

impl PhysicalPlan for ScanExec {
    fn schema(&self) -> Schema {
        self.datasource.schema().select(&self.projection)
    }

    fn children(&self) -> Vec<PhysicalPlans> {
        Vec::new()
    }

    fn to_string(&self) -> String {
        if self.projection.is_empty() {
            format!("Scan: {} | None", self.datasource.path())
        } else {
            format!(
                "Scan: {} | {}",
                self.datasource.path(),
                self.projection.join(", ")
            )
        }
    }
}

#[derive(Clone)]
pub struct ProjectionExec {
    schema: Schema,
    input: Box<PhysicalPlans>,
    expr: Vec<PhysicalExprs>,
}

impl ProjectionExec {
    pub fn new(schema: Schema, input: PhysicalPlans, expr: Vec<PhysicalExprs>) -> ProjectionExec {
        ProjectionExec {
            schema,
            input: Box::new(input),
            expr,
        }
    }

    #[try_stream(boxed, ok = RecordBatch, error = ZakuError)]
    pub async fn execute(&self) {
        #[for_await]
        for rb in self.input.execute() {
            let rb = rb?;
            let columns = self.expr.iter().map(|e| e.evaluate(&rb)).collect();
            yield RecordBatch::new(self.schema.clone(), columns)
        }
    }
}

impl PhysicalPlan for ProjectionExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn children(&self) -> Vec<PhysicalPlans> {
        vec![*self.input.clone()]
    }

    fn to_string(&self) -> String {
        format!(
            "Projection: {}",
            self.expr
                .iter()
                .map(|e| format!("{}", e))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

#[derive(Clone)]
pub struct FilterExec {
    schema: Schema,
    input: Box<PhysicalPlans>,
    expr: PhysicalExprs,
}

impl FilterExec {
    pub fn new(schema: Schema, input: PhysicalPlans, expr: PhysicalExprs) -> FilterExec {
        FilterExec {
            schema,
            input: Box::new(input),
            expr,
        }
    }

    #[try_stream(boxed, ok = RecordBatch, error = ZakuError)]
    pub async fn execute(&self) {
        #[for_await]
        for res in self.input.execute() {
            let rb = res?;
            let eval_col = self.expr.evaluate(&rb);
            let cols = rb
                .iter()
                .map(|c| {
                    Arc::new(Vectors::ColumnVector(ColumnVector::new(
                        *c.get_type(),
                        c.iter()
                            .enumerate()
                            .filter(|(i, _)| eval_col.get_value(i) == &Value::Boolean(true))
                            .map(|(_, v)| v.clone())
                            .collect(),
                    )))
                })
                .collect();
            yield RecordBatch::new(self.schema.clone(), cols)
        }
    }
}

impl PhysicalPlan for FilterExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn children(&self) -> Vec<PhysicalPlans> {
        vec![*self.input.clone()]
    }

    fn to_string(&self) -> String {
        format!("Filter: {}", self.expr)
    }
}

#[derive(Clone)]
pub struct LimitExec {
    schema: Schema,
    input: Box<PhysicalPlans>,
    limit: usize,
}

impl LimitExec {
    pub fn new(schema: Schema, input: PhysicalPlans, limit: usize) -> LimitExec {
        LimitExec {
            schema,
            input: Box::new(input),
            limit,
        }
    }

    #[try_stream(boxed, ok = RecordBatch, error = ZakuError)]
    pub async fn execute(&self) {
        if self.limit == 0 {
            let cols = self
                .schema
                .fields()
                .iter()
                .map(|f| {
                    let vec = ColumnVector::new(*f.datatype(), vec![]);
                    Arc::new(Vectors::ColumnVector(vec))
                })
                .collect();
            yield RecordBatch::new(self.schema.clone(), cols)
        }

        let mut counter = self.limit;
        #[for_await]
        for res in self.input.execute() {
            if counter == 0 {
                break;
            }
            let rb = res?;
            let take = if counter > rb.row_count() {
                counter -= rb.row_count();
                rb.row_count()
            } else {
                let temp = counter;
                counter = 0;
                temp
            };
            let cols: Vec<Arc<Vectors>> = rb
                .iter()
                .map(|c| {
                    Arc::new(Vectors::ColumnVector(ColumnVector::new(
                        *c.get_type(),
                        c.iter().take(take).cloned().collect(),
                    )))
                })
                .collect();

            yield RecordBatch::new(self.schema.clone(), cols)
        }
    }
}

impl PhysicalPlan for LimitExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn children(&self) -> Vec<PhysicalPlans> {
        vec![*self.input.clone()]
    }

    fn to_string(&self) -> String {
        format!("Limit: {}", self.limit)
    }
}

#[derive(Clone)]
pub struct SortExec {
    schema: Schema,
    input: Box<PhysicalPlans>,
    sort_keys: Vec<PhysicalExprs>,
    asc: Vec<bool>,
}

impl SortExec {
    pub fn new(
        schema: Schema,
        input: PhysicalPlans,
        sort_keys: Vec<PhysicalExprs>,
        asc: Vec<bool>,
    ) -> SortExec {
        SortExec {
            schema,
            input: Box::new(input),
            sort_keys,
            asc,
        }
    }

    #[try_stream(boxed, ok = RecordBatch, error = ZakuError)]
    pub async fn execute(&self) {
        let sort_keys_idx = self
            .sort_keys
            .iter()
            .flat_map(|e| match e {
                PhysicalExprs::Column(i) => Ok(*i),
                _ => Err(ZakuError::new("Sort keys must be column indexes")),
            })
            .collect::<Vec<usize>>();

        // Aggregate and materialize all values
        let mut cols: Vec<Vec<Value>> = self.schema().fields().iter().map(|_| vec![]).collect();
        #[for_await]
        for res in self.input.execute() {
            let rb = res?.sort(&sort_keys_idx, &self.asc)?;
            for (i, col) in rb.columns().iter().enumerate() {
                col.iter().for_each(|v| {
                    cols[i].push(v.clone());
                });
            }
        }

        // Sort all values
        let rev_asc: Vec<&bool> = self.asc.iter().rev().collect();
        sort_keys_idx.iter().rev().enumerate().for_each(|(i, k)| {
            let mut new_sorted_cols = vec![];
            let mut indices: Vec<usize> = (0..cols[*k].len()).collect();
            indices.sort_by_key(|&i| cols[*k][i].clone());

            if !rev_asc[i] {
                indices.reverse();
            }

            cols.iter().for_each(|col| {
                let mut reorder: Vec<Value> = Vec::with_capacity(col.len());
                indices.iter().for_each(|i| {
                    reorder.push(col[*i].clone());
                });
                new_sorted_cols.push(reorder);
            });
            cols = new_sorted_cols;
        });

        for rb in RecordBatch::to_record_batch(cols, &self.schema()) {
            yield rb
        }
    }
}

impl PhysicalPlan for SortExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn children(&self) -> Vec<PhysicalPlans> {
        vec![*self.input.clone()]
    }

    fn to_string(&self) -> String {
        format!(
            "Sort: keys={}",
            self.sort_keys
                .iter()
                .enumerate()
                .map(|(i, k)| {
                    let mut asc = "asc";
                    if !self.asc[i] {
                        asc = "desc";
                    }
                    format!("{} {}", k, asc)
                })
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

#[derive(Clone)]
pub struct HashAggregateExec {
    input: Box<PhysicalPlans>,
    group_expr: Vec<PhysicalExprs>,
    aggr_expr: Vec<AggregateExpressions>,
    schema: Schema,
}

impl HashAggregateExec {
    pub fn new(
        input: PhysicalPlans,
        group_expr: Vec<PhysicalExprs>,
        aggr_expr: Vec<AggregateExpressions>,
        schema: Schema,
    ) -> HashAggregateExec {
        HashAggregateExec {
            input: Box::new(input),
            group_expr,
            aggr_expr,
            schema,
        }
    }

    #[try_stream(boxed, ok = RecordBatch, error = ZakuError)]
    pub async fn execute(&self) {
        let mut aggregator_map: HashMap<Vec<Value>, Vec<Accumulators>> = HashMap::new();
        #[for_await]
        for res in self.input.execute() {
            let rb = res?;
            let group_keys: Vec<Arc<Vectors>> =
                self.group_expr.iter().map(|e| e.evaluate(&rb)).collect();
            let aggr_input: Vec<Arc<Vectors>> = self
                .aggr_expr
                .iter()
                .map(|e| e.input_expr().evaluate(&rb))
                .collect();

            (0..rb.row_count()).try_for_each(|i| {
                let row_key: Vec<Value> = group_keys
                    .iter()
                    .map(|key| key.get_value(&i).clone())
                    .collect();

                let accumulators = aggregator_map.entry(row_key).or_insert_with(|| {
                    self.aggr_expr
                        .iter()
                        .map(|e| e.create_accumulator())
                        .collect()
                });

                accumulators
                    .iter_mut()
                    .zip(aggr_input.iter())
                    .try_for_each(|(a, v)| a.accumulate(v.get_value(&i)))
            })?;
        }

        let mut columns: Vec<Vec<Value>> =
            self.schema().fields().iter().map(|_| Vec::new()).collect();
        aggregator_map.into_iter().for_each(|(k, v)| {
            let mut i = 0;
            k.into_iter().for_each(|key| {
                columns[i].push(key);
                i += 1;
            });
            v.into_iter().for_each(|a| {
                columns[i].push(a.get_value());
                i += 1;
            });
        });
        for rb in RecordBatch::to_record_batch(columns, &self.schema) {
            yield rb
        }
    }

    fn group_expr_str(&self) -> String {
        if self.group_expr.is_empty() {
            "None".to_string()
        } else {
            self.group_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        }
    }

    fn aggr_expr_str(&self) -> String {
        if self.aggr_expr.is_empty() {
            "None".to_string()
        } else {
            self.aggr_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        }
    }
}

impl PhysicalPlan for HashAggregateExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn children(&self) -> Vec<PhysicalPlans> {
        vec![*self.input.clone()]
    }

    fn to_string(&self) -> String {
        format!(
            "HashAggregate: group_expr={}, aggr_expr={}",
            self.group_expr_str(),
            self.aggr_expr_str()
        )
    }
}
