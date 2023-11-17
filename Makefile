release:
	cargo build --release

test:
	cargo test -- --nocapture

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

bench:
	cargo bench

clean:
	cargo clean

.PHONY: cli
