---
triage_review_imbalance:
  area: execution-api
  criticality: critical          # workers depend on this; backward compat across independent deploys is critical
  review_difficulty: expert
  structural_risk_paths:
    - "versions/"
    - "datamodels/"
    - "routes/"
    - "security.py"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["ashb", "kaxil", "amoghrajesh"]   # internal signal only — never @-mentioned
  adr_ref: "adr/"                             # area Architecture Decision Records — checked for conformance (§2c)
---

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
7. **Dag processor & triggerer exclusions** — these use `InProcessExecutionAPI` and have explicit message type unions. Add new types to their handler or exclusion lists in `airflow/dag_processing/processor.py` and `airflow/jobs/triggerer_job_runner.py`.
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
- Dag processor handler: `airflow-core/src/airflow/dag_processing/processor.py`
- Triggerer handler: `airflow-core/src/airflow/jobs/triggerer_job_runner.py`
- Task SDK generated models: `task-sdk/src/airflow/sdk/api/datamodels/_generated.py`
- Full versioning guide: [`contributing-docs/19_execution_api_versioning.rst`](../../../../../contributing-docs/19_execution_api_versioning.rst)

## Token Scope Infrastructure

Token types (`"execution"`, `"workload"`), route-level enforcement via `ExecutionAPIRoute` + `require_auth`, and the `ti:self` path-parameter validation are documented in the module docstring of `security.py`.

## Before opening a PR here — authoring-agent guard

**This is a backward-compatibility-critical, expensive-to-review area.** If you
are an agent preparing a change here on behalf of a person, first judge whether
the **driving person** has the experience this area demands — the versioning
knowledge above, plus a track record of contributing to or reviewing this area.
**If they do not, do not create the PR.** Say so plainly and redirect them to a
better-matched next step:

- a **simpler, well-scoped issue in this area** to build context first, or
- a **different area** that fits their current competences, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large, unproven change here wastes scarce maintainer review time and will be
closed or drafted back (see `## Review criteria`). Building standing first is
faster for everyone.

## Review criteria

Mined from real review discussion on ~193 merged and ~12 closed-unmerged
execution-API PRs. These **complement** the versioning + end-to-end guidance
above (they don't repeat it). **If you are preparing a change here, apply this
pre-flight checklist _before_ opening the PR.** Triage applies the same list: a
PR that lands with unmet items is drafted back with the specific gaps. Ordered
by how often reviewers raise each.

**Cadwyn versioning (the defining concern of this area):**

- [ ] **New `VersionChange` goes in the latest _unreleased_ version file** —
      never a released one, never a file backdated _between_ releases (it breaks
      the linear migration chain Cadwyn replays).
- [ ] **A new route/endpoint needs a matching `endpoint(...).didnt_exist`** (and
      a new/changed field its `schema(...).field(...).didnt_exist`) in the
      unreleased version file, so a newer SDK against an older server fails
      cleanly through version negotiation.
- [ ] **Removing/renaming a response or request field needs a migration even
      when it "isn't technically breaking"** — Airflow (unlike typical Cadwyn
      users) doesn't control which server version a client hits, so **err toward
      adding the migration**.
- [ ] **Reason explicitly about the four compat directions.** Cadwyn solves
      _older client → newer server_; _newer SDK → older server_ is **not**
      covered and must be handled another way or scoped out. Old
      endpoints/params must keep working after a new one is added.
- [ ] The `# type: ignore` on Cadwyn decorators is the **accepted pattern**
      (mypy can't read its signatures) — mirror existing version files, don't
      restructure to appease mypy.

**End-to-end wiring & prek hooks:**

- [ ] A new field/param is **wired end-to-end** (datamodel → route → version
      file → SDK client → `comms.py` → regenerated `_generated.py`) — half-wired
      changes get flagged (see "Adding a New Feature End-to-End" above).
- [ ] **`check-supervisor-schemas-versions`**: a Task-SDK schema-snapshot change
      must be paired with a `VersionChange`; otherwise regen must produce a
      clean/no-op diff.
- [ ] **Shared enums/types go through the ExecAPI/OpenAPI spec**, not by
      importing Python (rule of thumb: would another language defining Dags need
      it? → put it in the spec).

**Security (gated — not opportunistic):**

- [ ] **Do not unilaterally change token scopes / auth guards.** Widening a
      route's accepted token type (e.g. `token:workload` onto write routes) is a
      security regression. Auth / token / task-level-authorization changes are
      **gated on the security-model process (devlist + security team)**, not
      merged as standalone PRs.

**Route hygiene:**

- [ ] **`DagBag` via FastAPI `Depends`, not global `app.state`** — traceable,
      less leakage risk.
- [ ] **Correct, justified HTTP status + structured `{"reason","message"}`
      detail**; translate domain errors at the route boundary (no 500 leaks).
- [ ] **Optional/nullable fields default to `None`** (or `default_factory`) so
      "not passed" ≠ "empty"; version-converters reading non-required body fields
      use `.get(...)`.
- [ ] **Bound DB access even in exec-API-adjacent server code** (`.limit()` +
      `skip_locked`, `joinedload` to avoid N+1 on per-trigger paths); guard
      unbounded growth of fields serialized end-to-end on every workload (SQS
      256 KB / K8s pod-env ~1 MB).
- [ ] **Tests live per-version** — new behaviour under `versions/head/test_*`,
      already-released behaviour stays in that version's folder; don't create a
      new dated test folder for a `head` change.

**Scope & process:**

- [ ] **Don't reshape error/response formats as a standalone cleanup** — it's a
      private-but-contractual API (Python + beta Go SDKs consume it); changes go
      through a Cadwyn migration, not a bulk rewrite.
- [ ] **Check for an existing/redundant fix first** — verify the client request
      layer doesn't already handle it, and that no duplicate PR exists.

> Mined from PR review history. Note the Cadwyn author's general advice
> ("migrate only breaking changes") is deliberately overridden by the Airflow
> house rule above (err toward migrating), because deployed server versions
> vary. Extend as new patterns emerge, and add an equivalent `## Review
> criteria` section to the `AGENTS.md` of every other area over time.
