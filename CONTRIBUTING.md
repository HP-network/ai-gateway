# Contributing

Run the test suite before opening a pull request:

```sh
PYTHONPATH=src python -m unittest discover -s tests -v
```

Provider changes should include a fixture or unit test for both the request payload and response normalization. Do not commit API keys, bearer tokens, or private configuration files.
