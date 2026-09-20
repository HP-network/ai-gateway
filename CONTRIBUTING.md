# Contributing

Install Rust 1.88 or newer, then run the same checks used by CI before opening a pull request:

```sh
cargo fmt --check
cargo test --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo build --release --locked
```

Provider changes should include a unit test for both the request payload and response normalization. HTTP behavior should also be checked against a local fake provider before release. Do not commit API keys, bearer tokens, database files, or private configuration files.

Changes to authentication, rate limiting, the SQLite store, or admin endpoints should include an HTTP-level test and must not expose plaintext keys in logs or API responses after creation.
