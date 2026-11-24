<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Core API — Agent Instructions

## Runtime verification

When you verify a change against a running Airflow, start it with the
[runtime verification skill](../../../../../.agents/skills/airflow-runtime-verification/SKILL.md).
The skill defines the placeholders used below. Most checks need data. Create a Dag run with the
[Task SDK section](../../../../../task-sdk/AGENTS.md#runtime-verification) first.

Save the status code with the body:

```bash
curl -s -w '\nhttp_status=%{http_code}\n' http://localhost:<web>/api/v2/dags \
  | tee -a files/verify/<run>/api.log
```

- **Error paths.** `GET /api/v2/dags/no_such_dag` returns `404` with a `detail` string. A body that
  fails validation returns `422`, and `detail[].loc` names the field.
- **A change and its read-back.** Send `PATCH /api/v2/dags/<dag_id>` with `{"is_paused": true}`,
  then `GET /api/v2/dags/<dag_id>`. Then read the same fact from the CLI with
  `airflow dags list -o plain`, as the
  [CLI section](../../cli/AGENTS.md#runtime-verification) shows.
- **Routes of the UI.** The React application calls `/ui/...` routes, for example
  `/ui/dags?limit=5`. They are private and change freely. When a change claims an effect on the
  API, prove it on a public `/api/v2` route.

The running instance serves its API reference at `http://localhost:<web>/docs` and the OpenAPI
document at `http://localhost:<web>/openapi.json`.
