release:
	cargo build --release

test:
	cargo test

lint:
	cargo clippy --all-targets --all-features -- -D warnings

cli:
	./target/release/cli resources/test.csv

build:
	cargo build

run:
	./target/debug/cli resources/test.csv

fmt:
	cargo fmt

.PHONY: cli
