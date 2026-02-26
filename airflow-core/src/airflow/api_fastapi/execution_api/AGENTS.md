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
cd task-sdk && python dev/generate_task_sdk_models.py
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

### Adding a New Feature End-to-End

Adding a new Execution API feature touches multiple packages. All of these must stay in sync:

1. **Datamodels** — add request/response schemas in `datamodels/`.
2. **Route** — add the endpoint in `routes/`.
3. **Version migration** — add Cadwyn `VersionChange` (see above).
4. **Task SDK message types** — add request/response to `task-sdk/src/airflow/sdk/execution_time/comms.py`.
5. **Task SDK client** — add the client method in `task-sdk/src/airflow/sdk/api/client.py`.
6. **Supervisor** — handle the new message in `task-sdk/src/airflow/sdk/execution_time/supervisor.py`.
7. **DAG processor & triggerer exclusions** — these use `InProcessExecutionAPI` and have explicit message type unions. Add new types to their handler or exclusion lists in `airflow/dag_processing/processor.py` and `airflow/jobs/triggerer_job_runner.py`.
8. **Regenerate models** — `cd task-sdk && python dev/generate_task_sdk_models.py`.
9. **Tests** — if the new message type requires an API endpoint, add tests in all of these:
   - `airflow-core/tests/unit/api_fastapi/execution_api/` — endpoint tests
   - `task-sdk/tests/task_sdk/api/test_client.py` — client method tests
   - `task-sdk/tests/task_sdk/execution_time/test_supervisor.py` — add a `RequestTestCase` entry to `REQUEST_TEST_CASES` list
   - `task-sdk/tests/task_sdk/execution_time/test_task_runner.py` — task runner integration tests

### Key Paths

- Models: `datamodels/`
- Routes: `routes/`
- Versions: `versions/`
- Message types: `task-sdk/src/airflow/sdk/execution_time/comms.py`
- Client: `task-sdk/src/airflow/sdk/api/client.py`
- Supervisor: `task-sdk/src/airflow/sdk/execution_time/supervisor.py`
- DAG processor handler: `airflow-core/src/airflow/dag_processing/processor.py`
- Triggerer handler: `airflow-core/src/airflow/jobs/triggerer_job_runner.py`
- Task SDK generated models: `task-sdk/src/airflow/sdk/api/datamodels/_generated.py`
- Full versioning guide: [`contributing-docs/19_execution_api_versioning.rst`](../../../../contributing-docs/19_execution_api_versioning.rst)
