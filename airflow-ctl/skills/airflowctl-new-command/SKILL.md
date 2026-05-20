 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- Licensed to the Apache Software Foundation (ASF) under one
     or more contributor license agreements.  See the NOTICE file
     distributed with this work for additional information
     regarding copyright ownership.  The ASF licenses this file
     to you under the Apache License, Version 2.0 (the
     "License"); you may not use this file except in compliance
     with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing,
     software distributed under the License is distributed on an
     "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
     KIND, either express or implied.  See the License for the
     specific language governing permissions and limitations
     under the License. -->

---
name: airflowctl-new-command
description: |
  Add a new `airflowctl` CLI command following the two-shape pattern
  used by the `airflow-ctl/` distribution. Pick Shape A when one
  existing core API endpoint already does the work — the CLI surface
  is generated from `airflow-ctl/src/airflowctl/api/operations.py`
  and no `ctl/cli_config.py` edit is needed. Pick Shape B when the
  command needs more than one endpoint, request shaping, or output
  transformation — then a custom command module under
  `ctl/commands/` plus a registration in `ctl/cli_config.py` is
  required.
when_to_use: |
  Invoke when the user says "add an airflowctl command", "expose
  this API endpoint via airflowctl", "wrap the new core endpoint
  in the CLI", "add a custom airflowctl subcommand", or anything
  that maps to creating / extending the user-facing `airflowctl`
  CLI surface.
license: Apache-2.0
---

# Adding an `airflowctl` command

`airflow-ctl/` exposes the **public** Airflow API as a CLI. New commands
fall into one of two shapes. Decide which shape applies **before**
touching any file — they have different file footprints.

Scope reminder: only endpoints under
`airflow-core/src/airflow/api_fastapi/core_api/routes/public/` are
candidates for `airflowctl`. Routes under `core_api/routes/ui/` (or any
other non-public router) are out of scope — they are internal-only and
must not be exposed through the CLI.

## Decision rule

**Shape A — single-endpoint passthrough**. Use it when **all three** are
true:

- A single endpoint already exists under
  `airflow-core/src/airflow/api_fastapi/core_api/routes/public/` that
  does exactly what the command should do. Verify by reading the route
  module and cross-checking against the generated OpenAPI spec at
  `airflow-core/src/airflow/api_fastapi/core_api/openapi/v2-rest-api-generated.yaml`.
- The CLI arguments map 1:1 to the endpoint's path / query / body params.
- The output is the endpoint's response as-is (no merging, no derived
  fields, no client-side filtering).

Otherwise, use **Shape B — custom command**.

You should change *only* the files listed under the chosen shape. Adding
both a Shape-A operation method **and** a `cli_config.py` entry for the
same command is a smell — the CLI surface is already generated from
the operation. Equally, a Shape-B command that contains no transformation
beyond a single API call should be reworked into Shape A.

---

## Shape A — single-endpoint passthrough (no `cli_config.py` edit)

The CLI command is **derived** at startup from
`airflow-ctl/src/airflowctl/api/operations.py`. `cli_config.py` walks the
operation classes via reflection (`_create_args_map_from_operation`,
`_create_func_map_from_operation`) and builds `ActionCommand`s + arg
parsers from the method signatures and pydantic body models.

### Steps

1. **Confirm the core endpoint exists** in
   `airflow-core/src/airflow/api_fastapi/core_api/routes/` (or the auth
   manager OpenAPI). If it does not, add it there first — this skill
   does not cover writing core API endpoints.

2. **Regenerate the pydantic datamodels** so the new request/response
   bodies are available:

   ```
   prek run generate-airflowctl-datamodels --all-files
   ```

   The hook writes to
   `airflow-ctl/src/airflowctl/api/datamodels/generated.py` (and
   `auth_generated.py`). Do not hand-edit those files.

3. **Add an operation method** to the appropriate
   `*Operations` class in
   `airflow-ctl/src/airflowctl/api/operations.py`. The class must
   inherit `BaseOperations`. Use the existing methods (e.g.
   `PoolsOperations.get`, `ConnectionsOperations.delete`) as templates:

   ```python
   from airflowctl.api.datamodels.generated import FooResponse, FooBody


   class FoosOperations(BaseOperations):
       def get(self, foo_id: str) -> FooResponse | ServerResponseError: ...

       def create(self, foo: FooBody) -> FooResponse | ServerResponseError: ...
   ```

   Rules:

   - All request bodies and responses **must** be pydantic models
     imported from `airflowctl.api.datamodels.generated` (or
     `auth_generated`). Never accept or return free `dict`s here.
   - Method names map 1:1 to CLI subcommand names with `_` → `-`
     (`get_details` becomes `get-details`).
   - Required, non-bool primitive params become CLI positional args;
     bool stays `--flag/--no-flag`; `| None` annotations become
     optional `--name`. The reflection layer handles this.

4. **Do not touch** `airflow-ctl/src/airflowctl/ctl/cli_config.py`.
   No `Arg`, no `ActionCommand`, no `GroupCommand` is needed — the
   command is generated. If you find yourself wanting to add one,
   you are in Shape B, not Shape A.

   This includes the `CommandFactory` class and its excluded lists
   (`excluded_parameters`, `exclude_operation_names`,
   `exclude_method_names`). New operation methods occasionally surface
   a method or parameter the factory would need an exclusion for to
   auto-generate cleanly — **do not add to those lists yourself**.
   `CommandFactory` is shared infra and must not be edited as part of
   adding a new command. If the auto-generation fails because of an
   exclusion gap, stop and surface the case to a human reviewer
   instead. The fix is usually to rename the method / restructure
   the parameter so it fits the factory's existing contract, not to
   widen the factory's allowlist.

5. **If — and only if — this is the first command in a brand-new
   `*Operations` class** (i.e. the class did not previously exist in
   `operations.py`), register the resulting CLI group name in
   `scripts/in_container/run_capture_airflowctl_help.py`. Append the
   group name (e.g. `"tasks"` for a new `TaskOperations`) to the
   `COMMANDS` list so the help-image capture covers it. Existing
   groups need no change. Skipping this step leaves the new group
   without a captured help SVG and the next help-image regeneration
   will show an unexplained drift.

6. **Test the operation** in
   `airflow-ctl/tests/airflow_ctl/api/test_operations.py` (or sibling
   files) using the standard `httpx` mock pattern. Run with:

   ```
   uv run --project airflow-ctl pytest airflow-ctl/tests/airflow_ctl/api -xvs
   ```

7. **Add an integration test** in
   `airflow-ctl-tests/tests/airflowctl_tests/test_airflowctl_commands.py`
   (or a sibling file in the same package). The integration suite
   exercises the real CLI end-to-end against a running API; every new
   command must appear there — extend an existing parametrized list
   (see `test_airflowctl_commands` for the pattern) rather than
   creating a one-off test. Skipping this leaves the command
   untested in CI's integration job. Run locally with:

   ```
   breeze testing airflow-ctl-tests
   ```

8. **Smoke-check** the CLI surface:

   ```
   uv run --project airflow-ctl airflowctl --help
   uv run --project airflow-ctl airflowctl <group> --help
   ```

   The new subcommand and its args should appear automatically.

---

## Shape B — custom command (requires `cli_config.py` edit)

Use when the command needs to fan out to multiple endpoints, shape
input before calling the API, or transform output (file IO, pretty
printing, bulk import/export, etc.). The existing
`pool_command.import_/export` and `auth_command.login` commands are
the canonical examples.

### Steps

1. **Write the command function** in
   `airflow-ctl/src/airflowctl/ctl/commands/<area>_command.py`
   (create the file if the area is new). The function signature is
   `def func(args, api_client: Client = NEW_API_CLIENT) -> None`,
   decorated with `@provide_api_client(kind=ClientKind.CLI)` so the
   client is injected. Example:

   ```python
   from airflowctl.api.client import (
       NEW_API_CLIENT,
       Client,
       ClientKind,
       provide_api_client,
   )
   from airflowctl.api.datamodels.generated import FooBody


   @provide_api_client(kind=ClientKind.CLI)
   def do_thing(args, api_client: Client = NEW_API_CLIENT) -> None:
       body = FooBody(name=args.name, value=args.value)
       result = api_client.foos.create(foo=body)
       rich.print(result)
   ```

   Rules:

   - Request/response objects **must** come from
     `airflowctl.api.datamodels.generated`. Construct them explicitly
     in the command so validation happens before the API call.
   - Use `rich.print` / `AirflowConsole` for output, never plain
     `print`. JSON-output paths gate on `args.output == "json"`.
   - Raise `SystemExit(<message>)` on user-facing errors. Do not
     swallow `ServerResponseError`.

2. **Add `Arg` definitions** for any new flags in
   `airflow-ctl/src/airflowctl/ctl/cli_config.py`, grouped by area
   (e.g. `ARG_<AREA>_<NAME>`). Reuse existing `Arg`s where possible
   (`ARG_FILE`, `ARG_OUTPUT`, etc.) rather than duplicating.

3. **Register the command** in `cli_config.py`. Before adding
   anything, grep for the target area first so you do not create a
   duplicate group or shadow an existing subcommand:

   ```
   grep -nE '^(\w+_COMMANDS = |    GroupCommand\(name="<group>")' \
       airflow-ctl/src/airflowctl/ctl/cli_config.py
   ```

   - **If a `<AREA>_COMMANDS` tuple already exists** (e.g. `POOL_COMMANDS`,
     `CONNECTION_COMMANDS`): append your new `ActionCommand` inside that
     existing tuple. Do **not** create a second tuple for the same area.
   - **If a `GroupCommand(name="<group>", …)` for your group already
     exists in `core_commands`**: do **not** add another. Only the
     subcommand list (the `_COMMANDS` tuple it points at) changes.
   - **Subcommand names must be unique within their group.** Search the
     existing tuple for an `ActionCommand(name="<your-name>", …)` —
     if one exists, either rename your command or extend the existing
     function rather than registering a second one with the same name.
   - The reflection layer in `cli_config.py` *also* synthesizes
     `GroupCommand`s from `operations.py`. If a Shape-A operation
     class already produces the same group name your Shape-B group
     would use, do not add a duplicate Shape-B `GroupCommand` — extend
     the operation method instead, or pick a distinct group name.

   Only when none of the above is true, add a new tuple plus a new
   `GroupCommand`:

   ```python
   FOO_COMMANDS = (
       ActionCommand(
           name="do-thing",
           help="Short user-facing help",
           description="Longer description shown in --help",
           func=lazy_load_command("airflowctl.ctl.commands.foo_command.do_thing"),
           args=(ARG_FOO_NAME, ARG_FOO_VALUE, ARG_OUTPUT),
       ),
   )
   ```

   Then add the group to `core_commands`:

   ```python
   core_commands: list[CLICommand] = [
       # ... existing entries unchanged ...
       GroupCommand(
           name="foos",
           help="Manage foos",
           subcommands=FOO_COMMANDS,
       ),
   ]
   ```

   Always use `lazy_load_command(...)` for `func` — the CLI imports
   modules on demand and direct imports here break startup time.

4. **Write unit tests** in
   `airflow-ctl/tests/airflow_ctl/ctl/commands/test_<area>_command.py`.
   Use the existing command tests as templates (mock the API client,
   not the network). Run with:

   ```
   uv run --project airflow-ctl pytest airflow-ctl/tests/airflow_ctl/ctl/commands -xvs
   ```

5. **Add an integration test** in
   `airflow-ctl-tests/tests/airflowctl_tests/test_airflowctl_commands.py`
   (or a sibling file in the same package). Every new CLI command must
   show up in the integration suite — extend an existing parametrized
   list (see `test_airflowctl_commands`) rather than writing a one-off
   test. Run locally with:

   ```
   breeze testing airflow-ctl-tests
   ```

6. **Smoke-check** the CLI:

   ```
   uv run --project airflow-ctl airflowctl <group> <name> --help
   ```

---

## Validation before opening a PR

Run these from the repo root:

- `prek run --from-ref main --stage pre-commit` — fast static checks
  (includes `generate-airflowctl-datamodels`, `check-airflowctl-help-texts`,
  `check-airflowctl-command-coverage`, ruff, mypy).
- `prek run mypy-airflow-ctl --all-files` — type-check the changed
  module.
- `breeze testing airflow-ctl-tests` — run the full ctl suite.

The coverage and help-text checks will fail if you add a Shape-B
command without `ActionCommand` registration, or a Shape-A operation
whose datamodels are stale.

---

## What **not** to do

- Do not hand-edit `airflow-ctl/src/airflowctl/api/datamodels/generated.py`
  or `auth_generated.py` — they are regenerated from core's OpenAPI
  spec and your edits will be wiped.
- Do not edit `airflow-ctl/RELEASE_NOTES.rst`. It is regenerated by
  the airflow-ctl release manager from `git log` at release time;
  any per-PR edits will be overwritten. User-visible notes belong in
  the PR description, not in this file.
- Do not modify the `CommandFactory` class in `cli_config.py` — that
  includes its `excluded_parameters`, `exclude_operation_names`, and
  `exclude_method_names` lists. If your new operation hits an
  exclusion gap, stop and surface the case to a human reviewer
  instead of widening the factory's allowlist.
- Do not pass raw `dict` payloads to the API client. Always go via
  the pydantic models from `datamodels/generated`.
- Do not bypass `provide_api_client` to construct a client manually
  in Shape B — the decorator handles auth env wiring.
- Do not register both a Shape-A operation **and** a Shape-B command
  for the same user-facing subcommand — pick one shape.
- Do not write commands that talk to the metadata database directly.
  `airflowctl` is API-only by design.
