use criterion::{criterion_group, criterion_main, Criterion};
use zaku::Dataframe;

static PATH: &str = "benches/lineitem.csv";
static DELIMITER: u8 = b'|';
static SAMPLE_SIZE: usize = 10;
static NUM_THREADS: usize = 4;

fn load_csv(c: &mut Criterion) {
    let mut group = c.benchmark_group("zaku-tpch load benchmark");
    group.sample_size(SAMPLE_SIZE);
    group.bench_function("Dataframe::from_csv", |b| {
        b.iter(|| Dataframe::from_csv(PATH, Some(DELIMITER)).unwrap());
    });
}

fn tpch1_execute(df: Dataframe) {
    let sql = "
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(l_orderkey) as count_order
    from
        lineitem
    where
        l_shipdate <= '1998-12-01'
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus;
    ";
    let _ = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(NUM_THREADS)
        .build()
        .unwrap()
        .block_on(zaku::execute(sql, df));
}

fn tpch1(c: &mut Criterion) {
    let mut group = c.benchmark_group("zaku-tpch1 benchmark");
    let df = Dataframe::from_csv(PATH, Some(DELIMITER)).unwrap();

    group.sample_size(SAMPLE_SIZE);
    group.bench_function("tpch1", |b| {
        b.iter(|| tpch1_execute(df.clone()));
    });
}

criterion_group!(benches, load_csv, tpch1);
criterion_main!(benches);
