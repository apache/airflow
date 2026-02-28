# Self-Review: 630:P1 SimpleAuthManager Password File Edge Cases

## What changed and why?

Added 7 new tests to `airflow-core/tests/unit/api_fastapi/auth/managers/simple/test_simple_auth_manager.py`:

- **Corrupt JSON**: Verifies `JSONDecodeError` is raised when password file contains malformed JSON
- **Missing file**: Verifies `FileNotFoundError` when the password file doesn't exist
- **Password generation quality**: Checks that init creates unique 16-char passwords for all configured users
- **Idempotent init**: Confirms existing passwords are preserved when `init()` is called again
- **Incremental user addition**: New users get passwords added while existing users keep theirs
- **Custom password file path**: Config-driven password file location works correctly
- **Null role denial**: User with `role=None` is denied authorization

The existing tests covered happy-path init and basic password reading but missed error paths and edge cases that could cause silent failures in production.

## Why is this the right test layer (unit/integration/UI)?

**Unit tests** — `SimpleAuthManager` is a pure Python class with file I/O. Tests use `tmp_path` and `conf_vars` to isolate from real config. No database, no HTTP server needed. These directly test the auth manager's internal logic.

## What could still break / what's not covered?

- File locking behavior with `fcntl.flock` under concurrent access (would need process-level integration tests)
- Behavior when the password file directory doesn't exist (the code opens with `a+` which would fail)
- Password file permissions (e.g., world-readable passwords)

## What risks or follow-ups remain?

- The `_generate_password` method uses `random.choices` which is not cryptographically secure — this is a known limitation of SimpleAuthManager (dev-only)
- Could add a test for the `BlockingIOError` path in `init()` to verify the file-locking skip behavior
