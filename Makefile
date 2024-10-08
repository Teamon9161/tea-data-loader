format:
	cargo fmt --all
	cargo clippy -- -D warnings

test:
	cargo test --all --features "map-fac,tick-fac"
