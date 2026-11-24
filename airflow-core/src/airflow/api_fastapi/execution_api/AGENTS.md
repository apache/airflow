 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# Execution API — Agent Instructions

## Versioning

This API uses [Cadwyn](https://github.com/zmievsa/cadwyn) with CalVer (`vYYYY_MM_DD.py`).

Workers and API servers deploy independently, so backward compatibility is critical — older clients must work with newer servers.

### When Making Changes

1. Check the latest version file in `versions/`. If its date is in the future (unreleased), add your `VersionChange` class to that file. Otherwise create a new `vYYYY_MM_DD.py`.
2. Update the version bundle in `versions/__init__.py` only when creating a new file.
3. Regenerate Task SDK models:

```bash
uv run --active --group codegen --project apache-airflow-task-sdk --directory task-sdk -s dev/generate_task_sdk_models.py
```

4. Add tests for both the new and previous API versions.

### Common Patterns

- New schema field: `schema(Model).field("name").didnt_exist`
- New endpoint: `endpoint("/path", ["GET"]).didnt_exist`
- Response changes: implement `@convert_response_to_previous_version_for(Model)` and check field existence before popping.

### Pitfalls

- Don't use keyword arguments with `endpoint()` — use positional: `endpoint("/path", ["GET"])`.
- Don't add changes to already-released version files.
- Don't forget response converters for new fields in nested objects.
- Don't load the serialized Dag unconditionally on a per-task hot path such as `ti_run`. Gate the load so only the tasks that need it pay the cost (as `ti.is_stub` does), and add a `task_instance` column only when every task needs the value.
- Don't branch business logic by comparing the negotiated API version to a date/version string. For this case, subclass `VersionChangeWithSideEffects` for the version change and check its `is_applied` flag instead:

  ```python
  class AddArgBindingsToTIRunContext(VersionChangeWithSideEffects): ...


  def _client_supports_arg_bindings() -> bool:
      return AddArgBindingsToTIRunContext.is_applied
  ```

  See [Cadwyn's docs](https://docs.cadwyn.dev/concepts/version_changes/#version-changes-with-side-effects).

### Adding a New Feature End-to-End

Adding a new Execution API feature touches multiple packages. All of these must stay in sync:

1. **Datamodels** — add request/response schemas in `datamodels/`.
2. **Route** — add the endpoint in `routes/`.
3. **Version migration** — add Cadwyn `VersionChange` (see above).
4. **Task SDK message types** — add request/response to `task-sdk/src/airflow/sdk/execution_time/comms.py`.
5. **Task SDK client** — add the client method in `task-sdk/src/airflow/sdk/api/client.py`.
6. **Supervisor** — handle the new message in `task-sdk/src/airflow/sdk/execution_time/supervisor.py`.
7. **Dag processor & triggerer exclusions** — these use `InProcessExecutionAPI` and have explicit message type unions. Add new types to their handler or exclusion lists in `airflow/dag_processing/processor.py` and `airflow/jobs/triggerer_job_runner.py`.
8. **Regenerate models** — `uv run --active --group codegen --project apache-airflow-task-sdk --directory task-sdk -s dev/generate_task_sdk_models.py`.
9. **Tests** — if the new message type requires an API endpoint, add tests in all of these:
   - `airflow-core/tests/unit/api_fastapi/execution_api/` — endpoint tests
   - `task-sdk/tests/task_sdk/api/test_client.py` — client method tests
   - `task-sdk/tests/task_sdk/execution_time/test_supervisor.py` — add a `RequestTestCase` entry to `REQUEST_TEST_CASES` list
   - `task-sdk/tests/task_sdk/execution_time/test_task_runner.py` — task runner integration tests
   - `airflow-core/tests/unit/api_fastapi/execution_api/test_task_sdk_client_contract.py` — if the client method interprets the response (see below)

### Client/server contract tests

The task-sdk client tests use a mocked server, and the endpoint tests use a hand-written client, so neither catches a client that assumes something the server doesn't do. When a client method does more than send a request and parse the reply (it maps a status code to an `ErrorResponse`, parses a header, or falls back to another request), add a test to `test_task_sdk_client_contract.py`. It uses the `task_sdk_client` fixture, which sends the real client's requests, with its real `Airflow-API-Version` header, to this tree's routes in process.

- Set up rows with `dag_maker` or the models, and assert on the client's return value, not on HTTP details.
- The in-process API always answers as the newest version. Keep the mocked tests in `test_client.py` for behaviour against older servers.
- In-process requests skip authorization, so these tests can't cover 401/403 handling. The mocked tests and `test_security.py` do.

### Key Paths

- Models: `datamodels/`
- Routes: `routes/`
- Versions: `versions/`
- Message types: `task-sdk/src/airflow/sdk/execution_time/comms.py`
- Client: `task-sdk/src/airflow/sdk/api/client.py`
- Supervisor: `task-sdk/src/airflow/sdk/execution_time/supervisor.py`
- Dag processor handler: `airflow-core/src/airflow/dag_processing/processor.py`
- Triggerer handler: `airflow-core/src/airflow/jobs/triggerer_job_runner.py`
- Task SDK generated models: `task-sdk/src/airflow/sdk/api/datamodels/_generated.py`
- Full versioning guide: [`contributing-docs/19_execution_api_versioning.rst`](../../../../../contributing-docs/19_execution_api_versioning.rst)

## Token Scope Infrastructure

Token types (`"execution"`, `"workload"`, `"callback"`, `"dag_processor_session"`, `"dag_processor"`, `"dag_parse"`), route-level enforcement via `ExecutionAPIRoute` + `require_auth`, and the `ti:self` path-parameter validation are documented in the module docstring of `security.py`. A route that admits `dag_processor` or `dag_parse` tokens for Connection, Variable, XCom, Dag run or task-instance reads must also bind the request to a granted Dag bundle (`SelectedDagBundle` or `DagInGrantedBundle`); the `/jobs` routes bind to the `job_id` claim instead. `test_token_scope_boundaries.py` lists the bundle-bound routes and the parse-time messages that use them.

## Runtime verification

Use the [Task SDK section](../../../../../task-sdk/AGENTS.md#runtime-verification) and prove the
change with a triggered run, not only with `airflow dags test`. Without `--use-executor`,
`dags test` calls these routes in its own process through `InProcessExecutionAPI`, which replaces
the token and access checks with `always_allow` (see `in_process.py`). A triggered run sends the
requests of the task over HTTP to the API server, with the token of the task, the way a worker
does.

The Dag processor and the triggerer of this instance also use `InProcessExecutionAPI`, so their
requests skip the token checks in `security.py`. This instance cannot verify a change to the
`dag_processor`, `dag_parse` or triggerer token rules. Say so in the report, and rely on
`test_token_scope_boundaries.py` for those rules.
