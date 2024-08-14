check:
	cargo check
check-tests:
	cargo test --no-run
	
check-all: check check-tests