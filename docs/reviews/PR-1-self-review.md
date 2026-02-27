# PR-1 Self-Review: Add unit tests for CLI api_server command argument parsing

## What changed and why?

Added a new test file `test_api_server_arg_parsing.py` with 22 unit tests that verify the
argparse configuration for the `api-server` CLI subcommand. The tests cover:

- **--port / -p**: Parses to integer correctly; rejects non-integer input
- **--workers / -w**: Parses to integer correctly; rejects non-integer input
- **--apps**: Accepts `all`, `core`, `execution`, comma-separated values; defaults to `all`
- **--host / -H**: Accepts hostname/IP strings
- **--worker-timeout / -t**: Parses to integer; defaults to 120; rejects non-integer input
- **--proxy-headers**: Boolean store_true flag; defaults to False
- **--dev / -d**: Boolean store_true flag; defaults to False
- **--ssl-cert / --ssl-key**: Accepts file path strings
- **--log-config**: Accepts file path strings
- Combined flags and parametrized combinations

## Why is this the right test layer (unit/integration/UI)?

**Unit** is the correct layer because we are testing argument parsing logic in isolation.
No running server, no database, no network calls are needed. The parser is a pure
function that transforms CLI strings into a namespace object.

## What could still break / what's not covered?

- Validation of port *range* (e.g., port 0 or 99999) is not enforced by argparse and
  therefore not tested here — that is a runtime concern.
- The `--apps` flag accepts any string; semantic validation (e.g., rejecting "invalid_app")
  happens downstream in the command handler, not the parser.
- Interaction between `--daemon`, `--pid`, `--stdout`, `--stderr`, and `--log-file` is not
  covered (already tested in `test_api_server_command.py`).

## What risks or follow-ups remain?

- If new CLI flags are added to `api-server` in the future, corresponding parser tests
  should be added to this file.
- The existing `test_api_server_command.py` tests exercise the full command path including
  mocks of uvicorn/gunicorn; these new tests focus exclusively on argument parsing for
  faster, more focused regression detection.
