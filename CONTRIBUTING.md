# Contributing

Run the test suite before opening a pull request:

```sh
PYTHONPATH=src python -m unittest discover -s tests -v
```

Provider changes should include a fixture or unit test for both the request payload and response normalization. Do not commit API keys, bearer tokens, or private configuration files.

Changes to authentication, rate limiting, the SQLite store, or admin endpoints should include an HTTP-level test and must not expose plaintext keys in logs or API responses after creation.
