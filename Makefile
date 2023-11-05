build:
	cargo build --release

test:
	cargo test | cargo clippy --all-targets --all-features -- -D warnings

cli:
	./target/release/cli resources/test.csv

.PHONY: cli
