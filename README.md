# Zaku

A simple SQL query engine on CSV files, built on Rust.

## Requirements
* cargo 1.73.0

## Usage
Use `cargo build` to build the project, and `cargo run` to run the project.
Unit tests can be run with `cargo test`.

To start the query engine with printing of physical plans, run
```bash
./target/debug/zaku -e <path-to-csv-file>
```
