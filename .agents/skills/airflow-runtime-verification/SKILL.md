---
name: airflow-runtime-verification
description: Verify a change to Airflow core or the Task SDK against a running Airflow started from the checkout in Breeze. Use when asked to check that a change works in a running Airflow, or to show what a user sees before and after a change. Covers starting an isolated instance without a terminal, driving it through a Dag file, the REST API, the CLI and the UI, keeping the evidence and removing the instance.
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Verify a change against a running Airflow

Unit tests show that a function returns what the test expects. They do not show that a Dag file is
parsed, scheduled and run, or that the REST API, the CLI and the UI report the result. This skill
starts Airflow from the sources of the current checkout, uses it the way a user does and keeps what
it saw.

It covers `airflow-core` and `task-sdk`. The commands that depend on the changed area live next to
its code (step 1), so a new area gets runtime verification by adding a section there, without
changing this skill. It does not cover providers that need an external service, and it does not
measure speed: use the [performance harness](../airflow-performance-harness/SKILL.md) for that.
It adds to the tests the change needs; it does not replace them. To reproduce a reported issue from
its example code, follow
[`reproducer-conventions.md`](../../../.apache-magpie-overrides/reproducer-conventions.md); use this
skill when the behaviour needs the scheduler, the API server or the UI.

Breeze needs Docker. If Breeze cannot run, stop and say which call failed; do not build a
substitute environment.

## Workflow

Replace `<run>` with a short name for this verification (lowercase letters, digits, dashes) and
`<web>`, `<ssh>`, `<flower>`, `<postgres>` with four free host ports, for example `31080`, `31022`,
`31055`, `31032`. Use a new `<run>` for every start. The compose project of the instance is
`verify-<run>`. The sections that step 1 finds use these names, plus `<dag_id>` and `<dag_run_id>`
for the Dag and the run under test. Run every command from the root of the checkout under test.

1. **Find the recipes of the changed area.** Each area keeps its recipe in the
   `## Runtime verification` section of an `AGENTS.md` next to its code. Run this in the worktree of
   the change. It prints every `AGENTS.md` with such a section between a changed file and the root
   of the repository:

   ```bash
   base=$(git merge-base HEAD upstream/main) || { echo "No merge base with upstream/main: run git fetch upstream main" >&2; false; } && {
     changed=$(git diff --name-only "$base"; git ls-files --others --exclude-standard)
     git ls-files --cached --others --exclude-standard '*AGENTS.md' | xargs grep -l '^## Runtime verification$' \
       | while read -r agents_file; do
           if printf '%s\n' "$changed" | grep -q "^${agents_file%AGENTS.md}"; then echo "$agents_file"; fi
         done
   }
   ```

   If it prints nothing, follow the baseline Dag run in
   [`task-sdk/AGENTS.md`](../../../task-sdk/AGENTS.md#runtime-verification), and name in the report
   the changed folders that have no section of their own. A section can name options to add to the
   start in step 2.

2. **Start Airflow in its own compose project.** Run this as a background command; it keeps running
   until the instance is removed.

   ```bash
   mkdir -p files/verify/<run>
   ANSWER=no WEB_HOST_PORT=<web> SSH_PORT=<ssh> FLOWER_HOST_PORT=<flower> POSTGRES_HOST_PORT=<postgres> \
     breeze shell --project-name verify-<run> --backend postgres --python 3.11 --quiet \
     "AIRFLOW__CORE__LOAD_EXAMPLES=False AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True airflow standalone" \
     > files/verify/<run>/standalone.log 2>&1 < /dev/null
   ```

   `airflow standalone` runs the API server, the scheduler, the Dag processor and the triggerer with
   the `LocalExecutor`. `--project-name` and the four ports keep the instance apart from the default
   Breeze environment and from other runs. Use the Python version and the backend the task names;
   pass both every time, because Breeze remembers the last ones used. With `--backend mysql` the
   database port is `MYSQL_HOST_PORT`; `sqlite` publishes none. Without
   `AIRFLOW__CORE__LOAD_EXAMPLES=False` the instance parses the example Dags of core and of every
   provider. `ANSWER=no` answers the question Breeze asks when the CI image is older than the
   checkout ("Do you want to build the image"): the start goes on with the image that exists.

3. **Wait until every component is healthy.** An open port is not enough.

   ```bash
   for attempt in $(seq 1 60); do
     healthy=$(curl -sf http://localhost:<web>/api/v2/monitor/health | grep -o '"status":"healthy"' | wc -l)
     [ "$healthy" -eq 4 ] && echo "healthy" && break
     sleep 5
   done
   ```

   The four are `metadatabase`, `scheduler`, `triggerer` and `dag_processor`. If the loop ends
   without printing `healthy`, the reason is at the end of `files/verify/<run>/standalone.log`.

4. **Check that the instance runs this checkout.** Find the container, then the sources it mounts:

   ```bash
   docker ps --filter label=com.docker.compose.project=verify-<run> \
     --filter label=com.docker.compose.service=airflow --format '{{.Names}} {{.Ports}}'
   docker inspect <container> \
     --format '{{range .Mounts}}{{if eq .Destination "/opt/airflow/airflow-core"}}{{.Source}}{{end}}{{end}}'
   ```

   The first command prints the container name, which ends in a random suffix and changes with
   every start, and its ports, which must contain `<web>->8080`. The second must print `airflow-core`
   of this checkout (Docker Desktop on macOS puts `/host_mnt` in front of the path). Repeat the check
   whenever a result is surprising. The scheduler and the API server load the Python sources when
   they start: after editing `airflow-core/` or `task-sdk/`, remove the instance and start it again.

5. **Drive it the way a user would.** Follow every section that step 1 found. Prove the change on
   the surface the user of the change touches, and read the result back from a second one.

6. **Keep the evidence.** Save what proves the result under `files/verify/<run>/`: the request and
   the response with its status code, the command with its output and exit code, the screenshot.
   This form keeps a command, its output and its exit code together:

   ```bash
   { <command>; echo "exit_code=$?"; } 2>&1 | tee -a files/verify/<run>/commands.log
   ```

   Report the run name, the commit and these paths with the result.

7. **Remove the instance.**

   ```bash
   breeze down --project-name verify-<run>
   ```

   The background command from step 2 then exits with code 143; that is expected. Delete the Dag
   files this run added to `files/dags/`. The evidence stays.

## What counts as proof

- The real user path: a Dag file the Dag processor parsed, a public `/api/v2` route, a real CLI
  command. Not an internal function called from a script, not a direct database write.
- The action and the state after it: the trigger call and the final run state, the `PATCH` and a
  later `GET`.
- A second surface: a run triggered through the API is read back in the CLI or the UI.
- For a before and after claim, two instances from two worktrees (the change and its merge base),
  two run names, two sets of ports, the same Dag and the same commands.
- A step that could not be done is reported as skipped with the reason, never as verified another
  way.

## Pitfalls

- Never run `breeze down` without `--project-name`: it removes the Breeze environment of the whole
  checkout, including its database.
- Because of `ANSWER=no`, an outdated CI image is not rebuilt. If the start then fails on an
  import, for example `ModuleNotFoundError` for a provider, rebase onto a recent `main` or rebuild
  with `breeze ci-image build --python 3.11`.
- Every user is an administrator in this instance, so permissions are not exercised, and the
  recipes send no credentials. For a change about authentication or permissions, leave out
  `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS`. A request without a token then gets `401`. Get a
  token for one of the users Breeze configures (`admin`, `viewer`, `user`, `op`, each with its name
  as the password) and send it with every request:

  ```bash
  curl -s -X POST -H 'Content-Type: application/json' http://localhost:<web>/auth/token \
    -d '{"username": "viewer", "password": "viewer"}'
  curl -s -H "Authorization: Bearer <access_token>" http://localhost:<web>/api/v2/dags
  ```

- `files/dags/` belongs to the checkout, so every instance started from it parses the same files.
  Start each Dag file name and Dag id with `verify_`, so they are easy to find and to delete.
- Breeze also writes the task logs to `logs/` in the checkout. Git ignores that folder and
  `breeze down` does not empty it.
