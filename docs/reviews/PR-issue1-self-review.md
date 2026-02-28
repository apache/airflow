# Self-Review: 630:P1 FastAPI App Plugin Validation & Legacy Route Tests

## What changed and why?

Added 10 new tests to `airflow-core/tests/unit/api_fastapi/test_app.py` covering:

- **Plugin validation gaps**: Missing `app` key, missing `url_prefix` key, valid plugin mounting
- **Reserved prefix exhaustiveness**: Parametrized test over ALL reserved URL prefixes (`/api/v2`, `/ui`, `/execution`)
- **Middleware validation**: Missing `middleware` key, non-callable middleware value
- **Legacy route handling**: SPA catch-all returns HTML, `/api/v1/*` returns 404 with removal message, `/health` returns 404 with migration pointer, invalid `/api/*` returns 404

The existing tests only covered 2 out of 5 plugin error paths and did not test the actual HTTP behavior of legacy routes at all.

## Why is this the right test layer (unit/integration/UI)?

**Unit tests** — these test the `init_plugins()` function in isolation with mocked `plugins_manager` return values, and the legacy route handlers via `TestClient` without requiring a running server or database. The `@pytest.mark.db_test` marker is inherited from the module but the plugin tests themselves don't touch the DB.

## What could still break / what's not covered?

- Plugin mounting with custom `args`/`kwargs` (not tested, but the code path is simple pass-through)
- Interaction between multiple plugins mounted simultaneously
- WSGI middleware for Flask plugins (`init_flask_plugins`) is not covered here (separate concern)

## What risks or follow-ups remain?

- The `client` fixture creates a full app which is expensive; could consider a lighter fixture for plugin-only tests
- The `/api/v1` deprecation tests rely on the exact error message string; if the message changes, tests break (intentional — we want to detect message regressions)
