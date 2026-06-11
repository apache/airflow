<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Supervisor Schemas — Agent Instructions

## What this package owns

A Cadwyn [`VersionBundle`][cadwyn-versions] and a thin
`SchemaVersionMigrator` for the wire shapes the Task SDK supervisor
exchanges with a lang-SDK runtime subprocess (Java, Go, Rust, ...)
launched by a coordinator. **No Pydantic models live here.** The
models stay in their semantic homes:

- Task-execution channel (supervisor ↔ task runner): the
  `ToTask` and `ToSupervisor` discriminated unions in
  `airflow.sdk.execution_time.comms`.
- Dag-processing channel (manager ↔ parser-supervisor): the
  `ToManager` and `ToDagProcessor` discriminated unions in
  `airflow.dag_processing.processor`.

`registered_models_by_name()` introspects those four unions on first
call, so the snapshot the prek hook commits to `schema.json` always
matches the exact set of classes `CommsDecoder` actually decodes
against — there is no hand-maintained list to keep in sync. The
Triggerer's unions (`ToTriggerRunner`, `ToTriggerSupervisor`) **are
intentionally excluded**; lang-SDK coordinators do not handle the
Triggerer channel today.

The bundle references registered classes via `schema(...)` instructions
in `versions/v<date>.py` files.

This is **independent** of `airflow.api_fastapi.execution_api.versions.bundle`,
which governs the HTTP contract between Task SDK clients and the API
server. A supervisor schema change does **not** force a HTTP API
version bump, and vice versa.

[cadwyn-versions]: https://docs.cadwyn.dev/concepts/version_changes/

## Files in this folder

- `__init__.py` — re-exports `bundle`, the migrator, the
  `registered_models_by_name()` registry, and `resolve_body_class()`.
- `migrator.py` — `SchemaVersionMigrator` + `get_schema_version_migrator()`.
- `versions/__init__.py` — the `VersionBundle` itself
  (`HeadVersion()` + dated `Version(...)` entries).
- `versions/vYYYY_MM_DD.py` — one file per release. The most recent
  file is the **in-progress** version; PRs append to it.
- `schema.json` — generated head-version JSON Schema snapshot for
  lang-SDK codegen. Managed by the
  `generate-supervisor-schemas-snapshot` prek hook (which lives at
  `scripts/ci/prek/dump_supervisor_schemas.py` and walks
  `registered_models_by_name()` in sorted-name order); do not edit by
  hand.

## When making changes

### Adding a new body to the versioned contract

Append the class to the relevant discriminated union in its semantic
home — `ToTask` / `ToSupervisor` in `comms.py`, or `ToManager` /
`ToDagProcessor` in `processor.py`. That is the *only* registration
step; `registered_models_by_name()` picks it up automatically the next
time the snapshot hook runs.

No `VersionChange` entry is required on the first introduction — the
head shape *is* the schema for the new body.

### Adding a field to a registered body

1. Add the field to the model in its semantic home (e.g.
   `comms.py:StartupDetails`).
2. Open the in-progress `versions/vYYYY_MM_DD.py` file (the one with
   the most recent date) and append a `VersionChange` subclass:

   ```python
   class AddSentryTraceField(VersionChange):
       """Add `sentry_trace_id` to StartupDetails."""

       description = __doc__

       instructions_to_migrate_to_previous_version = (
           schema(StartupDetails).field("sentry_trace_id").didnt_exist,
       )
   ```

3. Reference the new `VersionChange` from the bundle in
   `versions/__init__.py`:

   ```python
   Version("2026-06-16", AddRetryDelay, AddSentryTraceField),
   ```

4. The `generate-supervisor-schemas-snapshot` prek hook will
   regenerate `schema.json` on commit. Re-stage the file.

### Removing or renaming a field

Same pattern as adding, but with the inverse instruction
(`schema(X).field(...).existed_as(...)` etc.). See the execution-API
`versions/` folder for richer examples.

## Version cadence

- **Bump per release.** The release manager freezes the in-progress
  `vYYYY_MM_DD.py` file at release time and opens a new in-progress
  file dated past the next planned release.
- **Accumulate per change.** Each PR that mutates a registered body
  appends a `VersionChange` entry to the head in-progress file.
  Contributors never invent a new version date.

## Prek hooks

Two hooks enforce the contract:

- `generate-supervisor-schemas-snapshot` — regenerates `schema.json`
  on commit when any registered model or any `versions/v*.py` file
  changes. Fails if the committed snapshot is stale.
- `check-supervisor-schemas-versions` — fails if the regenerated
  snapshot differs from the upstream target-branch snapshot but no
  file under `versions/` was touched.

The check is per-commit; the file bump is per-release.
