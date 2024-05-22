# Zaku

A simple SQL query engine on singular CSV files, built with Rust.

## Features

* `SELECT` queries with `WHERE`, `LIMIT`, `GROUP BY`, `HAVING`, `ORDER BY`
* `EXPLAIN` statements
* `COPY TO` csv file commands (but not `COPY FROM`)
* Schema of csv file can be printed with input: `schema`

## Setting up

Install rust nightly and pre-commit.
Run `pre-commit install` prior to any commits.

### Versions used

* cargo 1.80.0-nightly
* pre-commit 3.7.0

### Usage

* `make build` to build the project in debug mode
* `make test` to run the tests
* `make lint` to lint the code
* `make fmt` to format the code
* `make run` to run the cli app with the `resources/test.csv` file
* `make release` to build the project in release mode
* `make cli` to run the cli app with the `resources/test.csv` file in release mode
* `make bench` to run the benchmarks

To start the query engine with your choice of csv file, run

```bash
./target/debug/cli <path-to-csv-file>
```
