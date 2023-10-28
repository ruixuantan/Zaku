use zaku::{logical_plans::dataframe::Dataframe, sql::parser::parse};

fn main() {
    let sql = "SELECT id, product_name FROM test";
    let df = Dataframe::from_csv("resources/test.csv").unwrap();
    let select_df = parse(sql, df).unwrap();
    let res = select_df.logical_plan().to_physical_plan().execute();
    println!("{}", res.column_count());
    println!("{}", res.row_count());
}
