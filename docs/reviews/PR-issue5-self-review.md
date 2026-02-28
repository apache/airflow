# Self-Review: 630:P1 core_api init_views Static and Catch-All Route Tests

## What changed and why?

Added 11 new tests in a `TestInitViews` class to `airflow-core/tests/unit/api_fastapi/core_api/test_app.py`:

- **SPA catch-all coverage**: Verifies HTML is returned for `/dashboard`, nested paths (`/dags/my_dag/grid`), and root `/`
- **Legacy route handling**: `/health` returns 404 + migration pointer, `/api/v1/*` returns 404 + removal notice, `/api/v1` nested paths work correctly
- **API catch-all**: `/api/<unknown>` returns "API route not found"
- **Static mount**: `/static` mount is present in routes
- **Route ordering**: Catch-all is the last route (prevents shadowing API routes)
- **DEV_MODE**: In dev mode, `/static/i18n/locales` mount is added; in non-dev mode it is absent

Previously `test_app.py` only had a single test for gzip middleware. The `init_views` function was completely untested in isolation.

## Why is this the right test layer (unit/integration/UI)?

**Unit tests** — `init_views()` is called directly on a bare `FastAPI()` instance with no database or auth setup. `TestClient` provides request/response testing without a running server. The `DEV_MODE` tests use `mock.patch.dict` to control environment variables.

## What could still break / what's not covered?

- Actual static file serving (the test checks mount existence but not file content delivery, since the `dist` directory may be empty during tests)
- The `Jinja2Templates` rendering of `index.html` with `backend_server_base_url` context variable
- CORS and GZip middleware interaction with these routes

## What risks or follow-ups remain?

- If the `_AIRFLOW_PATH` computation in `core_api/app.py` changes, the `ui/dist` directory resolution could break silently
- The DEV_MODE test depends on the `dev_i18n_static` name literal; a rename would break the test (intentional — we want to detect accidental removals)
