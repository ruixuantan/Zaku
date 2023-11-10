use enum_dispatch::enum_dispatch;
use std::{collections::HashMap, sync::Arc};

use crate::{
    datasources::datasource::Datasource,
    datatypes::{
        column_vector::{ColumnVector, Vector, Vectors},
        record_batch::RecordBatch,
        schema::Schema,
        types::Value,
    },
    physical_plans::accumulator::{Accumulator, Accumulators},
};

use super::{
    accumulator::AggregateExpressions,
    physical_expr::{PhysicalExpr, PhysicalExprs},
};

#[enum_dispatch]
pub trait PhysicalPlan {
    fn schema(&self) -> Schema;

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_>;

    fn children(&self) -> Vec<PhysicalPlans>;
}

#[derive(Clone)]
#[enum_dispatch(PhysicalPlan)]
pub enum PhysicalPlans {
    Scan(ScanExec),
    Projection(ProjectionExec),
    Filter(FilterExec),
    Limit(LimitExec),
    HashAggregate(HashAggregateExec),
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
}

impl PhysicalPlan for ScanExec {
    fn schema(&self) -> Schema {
        self.datasource.schema().select(&self.projection)
    }

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        self.datasource.scan()
    }

    fn children(&self) -> Vec<PhysicalPlans> {
        Vec::new()
    }
}

#[derive(Clone)]
pub struct ProjectionExec {
    schema: Schema,
    physical_plan: Box<PhysicalPlans>,
    expr: Vec<PhysicalExprs>,
}

impl ProjectionExec {
    pub fn new(
        schema: Schema,
        physical_plan: PhysicalPlans,
        expr: Vec<PhysicalExprs>,
    ) -> ProjectionExec {
        ProjectionExec {
            schema,
            physical_plan: Box::new(physical_plan),
            expr,
        }
    }
}

impl PhysicalPlan for ProjectionExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        Box::new(self.physical_plan.execute().map(|rb| {
            let columns = self.expr.iter().map(|e| e.evaluate(&rb)).collect();
            RecordBatch::new(self.schema.clone(), columns)
        }))
    }

    fn children(&self) -> Vec<PhysicalPlans> {
        vec![*self.physical_plan.clone()]
    }
}

#[derive(Clone)]
pub struct FilterExec {
    schema: Schema,
    physical_plan: Box<PhysicalPlans>,
    expr: PhysicalExprs,
}

impl FilterExec {
    pub fn new(schema: Schema, physical_plan: PhysicalPlans, expr: PhysicalExprs) -> FilterExec {
        FilterExec {
            schema,
            physical_plan: Box::new(physical_plan),
            expr,
        }
    }
}

impl PhysicalPlan for FilterExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        let res = self.physical_plan.execute().map(|rb| {
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

            RecordBatch::new(self.schema.clone(), cols)
        });

        Box::new(res)
    }

    fn children(&self) -> Vec<PhysicalPlans> {
        vec![*self.physical_plan.clone()]
    }
}

#[derive(Clone)]
pub struct LimitExec {
    schema: Schema,
    physical_plan: Box<PhysicalPlans>,
    limit: usize,
}

impl LimitExec {
    pub fn new(schema: Schema, physical_plan: PhysicalPlans, limit: usize) -> LimitExec {
        LimitExec {
            schema,
            physical_plan: Box::new(physical_plan),
            limit,
        }
    }
}

impl PhysicalPlan for LimitExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        let mut counter = self.limit;
        let res = self.physical_plan.execute().map(move |rb| {
            let take = if counter > rb.row_count() {
                counter -= rb.row_count();
                rb.row_count()
            } else {
                let temp = counter;
                counter = 0;
                temp
            };
            let cols = rb
                .iter()
                .map(|c| {
                    Arc::new(Vectors::ColumnVector(ColumnVector::new(
                        *c.get_type(),
                        c.iter().take(take).cloned().collect(),
                    )))
                })
                .collect();

            RecordBatch::new(self.schema.clone(), cols)
        });

        Box::new(res)
    }

    fn children(&self) -> Vec<PhysicalPlans> {
        vec![*self.physical_plan.clone()]
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
}

impl PhysicalPlan for HashAggregateExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        let mut aggregator_map: HashMap<Vec<Value>, Vec<Accumulators>> = HashMap::new();
        self.input.execute().for_each(|rb| {
            let group_keys: Vec<Arc<Vectors>> =
                self.group_expr.iter().map(|e| e.evaluate(&rb)).collect();
            let aggr_input: Vec<Arc<Vectors>> = self
                .aggr_expr
                .iter()
                .map(|e| e.input_expr().evaluate(&rb))
                .collect();

            (0..rb.row_count()).for_each(|i| {
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
                    .for_each(|(a, v)| {
                        a.accumulate(v.get_value(&i)).unwrap();
                    });
            });
        });

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
        let arc_cols: Vec<Arc<Vectors>> = columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                Arc::new(Vectors::ColumnVector(ColumnVector::new(
                    *self
                        .schema()
                        .get_datatype_from_index(&i)
                        .expect("Index was taken from schema length"),
                    col.clone(),
                )))
            })
            .collect();
        let _ = [RecordBatch::new(self.schema.clone(), arc_cols)].iter();
        todo!()
    }

    fn children(&self) -> Vec<PhysicalPlans> {
        vec![*self.input.clone()]
    }
}
