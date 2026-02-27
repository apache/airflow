# PR-2 Self-Review: Improve test infrastructure — shared fixture for minimal DAG bag

## What changed and why?

Added a reusable `minimal_dagbag` pytest fixture to `airflow-core/tests/conftest.py`.
The fixture creates a temporary directory with a single no-op DAG file and returns a
fully constructed `DagBag` instance, eliminating the need for tests to duplicate
boilerplate DAG-file creation logic.

Also added `test_minimal_dagbag_fixture.py` with 7 tests that validate the fixture
behaves correctly (correct DAG count, expected task, no import errors, etc.), serving
both as regression protection for the fixture itself and as documentation-by-example
for other developers.

## Why is this the right test layer (unit/integration/UI)?

**Unit** — the fixture itself produces an in-memory DagBag parsed from a temporary file.
It does not require a running Airflow instance, database, or network. The validation
tests are likewise pure unit tests.

## What could still break / what's not covered?

- The fixture currently always creates a DAG with `schedule=None`. Tests needing a
  scheduled DAG will need their own setup or a parametrized variant.
- The fixture loads `EmptyOperator` from `airflow.providers.standard`, which assumes
  the standard provider is installed.

## What risks or follow-ups remain?

- If `EmptyOperator` moves to a different package, the fixture's DAG file will need
  updating.
- Future work could add a `minimal_dagbag_factory` fixture that accepts parameters
  (number of DAGs, tasks per DAG, schedule) for greater flexibility.
- Existing tests across the repo that create one-off DagBags with a single DAG can
  gradually be migrated to use this fixture.
