use std::path::Path;

use zaku::{execute, test_utils::DatasinkBuilder, Dataframe, Datasink, ZakuError};

async fn run(sql: &str) -> Result<Datasink, ZakuError> {
    let binding = Path::new("resources").join("test.csv");
    let path = binding.to_str().expect("test.csv file should exist");
    let df = Dataframe::from_csv(path, None)?;
    let res = execute(sql, df).await?;
    Ok(res)
}

#[tokio::test]
async fn basic_query() {
    let sql = "SELECT * FROM test";
    let expected = DatasinkBuilder::default()
        .add_schema(
            vec![
                "id",
                "product_name",
                "is_available",
                "price",
                "quantity",
                "updated_on",
            ],
            vec!["num", "text", "bool", "num", "num", "date"],
        )
        .add_data(vec![
            vec!["1", "toothbrush", "true", "5.00", "100", "2023-06-06"],
            vec!["2", "toothpaste", "true", "10.00", "50", "2023-01-01"],
            vec!["3", "shampoo", "true", "15.50", "25", "2023-04-04"],
            vec!["4", "soap", "false", "2.00", "0", "2023-02-02"],
            vec!["5", "shaving cream", "true", "20.00", "10", "2023-03-03"],
        ])
        .build();
    assert_eq!(run(sql).await.unwrap(), expected);
}

#[tokio::test]
async fn projection_query() {
    let sql = "SELECT id, product_name FROM test";
    let expected = DatasinkBuilder::default()
        .add_schema(vec!["id", "product_name"], vec!["num", "text"])
        .add_data(vec![
            vec!["1", "toothbrush"],
            vec!["2", "toothpaste"],
            vec!["3", "shampoo"],
            vec!["4", "soap"],
            vec!["5", "shaving cream"],
        ])
        .build();
    assert_eq!(run(sql).await.unwrap(), expected);
}

#[tokio::test]
async fn filter_query() {
    let sql = "SELECT * FROM test WHERE price >= 10";
    let expected = DatasinkBuilder::default()
        .add_schema(
            vec![
                "id",
                "product_name",
                "is_available",
                "price",
                "quantity",
                "updated_on",
            ],
            vec!["num", "text", "bool", "num", "num", "date"],
        )
        .add_data(vec![
            vec!["2", "toothpaste", "true", "10.00", "50", "2023-01-01"],
            vec!["3", "shampoo", "true", "15.50", "25", "2023-04-04"],
            vec!["5", "shaving cream", "true", "20.00", "10", "2023-03-03"],
        ])
        .build();
    assert_eq!(run(sql).await.unwrap(), expected);
}

#[tokio::test]
async fn filter_no_records_query() {
    let sql = "SELECT * FROM test WHERE price < 0";
    let expected = DatasinkBuilder::default()
        .add_schema(
            vec![
                "id",
                "product_name",
                "is_available",
                "price",
                "quantity",
                "updated_on",
            ],
            vec!["num", "text", "bool", "num", "num", "date"],
        )
        .build();
    assert_eq!(run(sql).await.unwrap(), expected);
}

#[tokio::test]
async fn limit_query() {
    let sql = "SELECT * FROM test LIMIT 2";
    let expected = DatasinkBuilder::default()
        .add_schema(
            vec![
                "id",
                "product_name",
                "is_available",
                "price",
                "quantity",
                "updated_on",
            ],
            vec!["num", "text", "bool", "num", "num", "date"],
        )
        .add_data(vec![
            vec!["1", "toothbrush", "true", "5.00", "100", "2023-06-06"],
            vec!["2", "toothpaste", "true", "10.00", "50", "2023-01-01"],
        ])
        .build();
    assert_eq!(run(sql).await.unwrap(), expected);
}

#[tokio::test]
async fn limit_zero_query() {
    let sql = "SELECT * FROM test LIMIT 0";
    let expected = DatasinkBuilder::default()
        .add_schema(
            vec![
                "id",
                "product_name",
                "is_available",
                "price",
                "quantity",
                "updated_on",
            ],
            vec!["num", "text", "bool", "num", "num", "date"],
        )
        .build();
    assert_eq!(run(sql).await.unwrap(), expected);
}

#[tokio::test]
async fn order_by_query() {
    let sql = "SELECT id FROM test ORDER BY id DESC";
    let expected = DatasinkBuilder::default()
        .add_schema(vec!["id"], vec!["num"])
        .add_data(vec![vec!["5"], vec!["4"], vec!["3"], vec!["2"], vec!["1"]])
        .build();
    assert_eq!(run(sql).await.unwrap(), expected);
}

#[tokio::test]
async fn aggregate_query() {
    let sql = "SELECT SUM(price*2.0) AS inflation FROM test";
    let expected = DatasinkBuilder::default()
        .add_schema(vec!["inflation"], vec!["num"])
        .add_data(vec![vec!["105"]])
        .build();
    assert_eq!(run(sql).await.unwrap(), expected);
}

#[tokio::test]
async fn aggregate_group_by_query() {
    let sql = "SELECT AVG(price) * SUM(quantity) AS estimated FROM test WHERE is_available = true GROUP BY is_available";
    let expected = DatasinkBuilder::default()
        .add_schema(vec!["estimated"], vec!["num"])
        .add_data(vec![vec!["2335.625"]])
        .build();
    assert_eq!(run(sql).await.unwrap(), expected);
}

#[tokio::test]
async fn group_by_wildcard_query() {
    let sql = "SELECT * FROM test GROUP BY is_available";
    assert!(run(sql).await.is_err());
}

#[tokio::test]
async fn aggregate_in_where_query() {
    let sql = "SELECT COUNT(*) AS count FROM test WHERE COUNT(*) > 2";
    assert!(run(sql).await.is_err());
}

#[tokio::test]
async fn having_query() {
    let sql = "SELECT COUNT(id) AS count FROM test GROUP BY is_available HAVING COUNT(id) > 2 AND is_available = true";
    let expected = DatasinkBuilder::default()
        .add_schema(vec!["count"], vec!["num"])
        .add_data(vec![vec!["4"]])
        .build();
    assert_eq!(run(sql).await.unwrap(), expected);
}

#[tokio::test]
async fn having_aggregate_query() {
    let sql =
        "SELECT COUNT(id), SUM(price) FROM test GROUP BY is_available HAVING MIN(quantity) > 0";
    let expected = DatasinkBuilder::default()
        .add_schema(vec!["count", "sum"], vec!["num", "num"])
        .add_data(vec![vec!["4", "50.50"]])
        .build();
    assert_eq!(run(sql).await.unwrap(), expected);
}

#[tokio::test]
async fn complex_query() {
    let sql =
        "SELECT id, product_name, (price*quantity) AS total FROM test WHERE quantity <> 0 LIMIT 3";
    let expected = DatasinkBuilder::default()
        .add_schema(
            vec!["id", "product_name", "total"],
            vec!["num", "text", "num"],
        )
        .add_data(vec![
            vec!["1", "toothbrush", "500.00"],
            vec!["2", "toothpaste", "500.00"],
            vec!["3", "shampoo", "387.50"],
        ])
        .build();
    assert_eq!(run(sql).await.unwrap(), expected);
}

#[tokio::test]
async fn explain_query() {
    let sql = "EXPLAIN SELECT * FROM test";
    assert!(run(sql).await.is_ok());
}
