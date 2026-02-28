# Self-Review: 630:P1 Strengthen DAGs API List/Filter Parameter Tests

## What changed and why?

Added 8 new tests to `TestGetDags` class in `airflow-core/tests/unit/api_fastapi/core_api/routes/public/test_dags.py`:

- **Multiple tags filter**: Verifies filtering by tags correctly returns only DAGs with matching tags
- **No-match pattern**: `dag_id_pattern` that matches nothing returns empty result
- **Negative offset**: Confirms HTTP 422 validation error for `offset=-1`
- **Zero limit**: `limit=0` returns empty dags list but valid `total_entries`
- **Combined owner + tag**: Both filters applied simultaneously narrow results correctly
- **Combined paused + owner**: Multi-filter combination works as expected
- **Invalid order_by**: Non-existent sort field returns 422
- **Response schema**: Verifies each DAG object contains expected top-level fields

The existing parametrized tests covered many filter combinations but missed boundary values (negative offset, zero limit), validation errors (invalid order_by), and multi-filter combinations.

## Why is this the right test layer (unit/integration/UI)?

**Unit/integration tests** — these hit the actual FastAPI router via `TestClient` and use a real SQLite database (via the `@pytest.mark.db_test` marker and `dag_maker` fixture). This is the correct layer because the filtering logic lives in SQLAlchemy queries within the route handler.

## What could still break / what's not covered?

- Pagination with very large `limit` values (no max limit validation tested)
- `dag_display_name_pattern` edge cases (regex injection, empty string)
- Ordering by multiple fields with mixed asc/desc

## What risks or follow-ups remain?

- The `test_get_dags_multiple_tags_filter` creates DAGs via `dag_maker` which may interact with the module-scoped `dagbag` fixture — currently no conflicts observed
- The 422 validation tests depend on Pydantic/FastAPI's validation behavior which could change in major version upgrades
