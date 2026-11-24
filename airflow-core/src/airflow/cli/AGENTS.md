<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# CLI — Agent Instructions

## Runtime verification

When you verify a change against a running Airflow, start it with the
[runtime verification skill](../../../../.agents/skills/airflow-runtime-verification/SKILL.md).
The skill defines the placeholders used below.

Run a command in the running container of the instance:

```bash
breeze exec --project-name verify-<run> -- \
  -c "AIRFLOW__CORE__LOAD_EXAMPLES=False airflow dags list -o plain"; echo "exit_code=$?"
```

`breeze exec` runs what follows `--` with `bash` in that container, with the Breeze environment and
the Dags folder set up. Breeze prints its own checks first, and the output of the command comes
last. The CLI process reads its own configuration, so repeat `AIRFLOW__CORE__LOAD_EXAMPLES=False` in
every command.

Prove a CLI change with the command a user types and its exit code. Read the state it changed back
from the REST API, for example `GET http://localhost:<web>/api/v2/dags/<dag_id>` after
`airflow dags pause <dag_id>`.
