---
triage_review_imbalance:
  area: airflow-ctl
  criticality: high              # base tier; the generated API client + client/auth layer promoted to `critical` via structural_risk_paths
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "api/client.py"
    - "api/operations.py"
    - "api/datamodels/"
    - "ctl/cli_config.py"
    - "ctl/commands/"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["bugraoz93", "potiuk", "dheerajturaga", "henry3260"]   # internal signal only — never @-mentioned in drafted PR text
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# airflowctl (remote management CLI) — Agent Instructions

This directory holds `airflowctl` — the standalone `apache-airflow-ctl`
distribution that manages a running Airflow deployment **remotely**. It is
**not** the in-process `airflow` CLI in airflow-core: it ships as its own
package, imports **no** airflow-core code, and never touches the metadata
database. It reaches Airflow through exactly one seam — the **public REST API
v2** on the API server — authenticating as an ordinary API client with a
token/JWT, like any external integrator. Its `api/` layer is **generated** from
the API server's OpenAPI spec, and its command surface, flags, and
`--output` formats are a contract that operators and scripts depend on. A
defect here breaks remote management for every deployment that drives Airflow
from a client, and a careless shape change silently breaks the scripts wrapped
around it.

## Why changes here are expensive to review

- The distribution sits **behind a network + trust boundary**: it is a remote
  API _client_, so the correctness question is often "does this still go
  through the public REST API as an authenticated client?" — a change that
  quietly imports airflow-core, reads a config file the server owns, or assumes
  co-location breaks the remote model, and that is frequently **not** visible
  from the diff.
- The `api/datamodels/` modules are **generated** (`datamodel-codegen` from the
  server's `v2-rest-api-generated.yaml` / `v2-simple-auth-manager-generated.yaml`).
  A hand-edit to a generated file looks innocent in a diff but is reverted by
  the next regeneration and means the client no longer matches the published
  contract.
- The **CLI surface is a UX + machine-readable-output contract**. Renaming a
  command, changing a flag, making an argument positional, or altering the
  `json`/`table`/`yaml` output shape can break every script and pipeline built
  on it — the blast radius is invisible in the diff.
- **Token / credential handling** (keyring storage, per-environment token keys,
  JWT bearer auth) is security-sensitive and runs on operator machines; a slip
  here leaks credentials or opens path-traversal / injection on the local host.

## Knowledge a reviewer (and a substantial contributor) needs

- The **remote-client model**: `airflowctl` talks to the API server only over
  the public REST API v2. It does **not** import `airflow.*` core modules,
  open a DB session, or read server-side config — it is a separate distribution
  with its own dependency set.
- The **generated API layer**: `api/datamodels/generated.py` and
  `auth_generated.py` are produced by `datamodel-codegen` (the
  `generate-airflowctl-datamodels` prek hook) from the server's committed
  OpenAPI specs. `api/operations.py` wraps the endpoints; `api/client.py` is
  the `httpx`-based transport. The server's API change is what drives a client
  change — not a hand-edit on this side.
- The **CLI wiring**: `ctl/cli_parser.py` + `ctl/cli_config.py` register command
  groups declaratively; `ctl/commands/…` hold the handlers; help text lives in
  `ctl/help_texts.yaml`. A `check-airflowctl-command-coverage` prek hook pairs
  each operation with an integration test.
- **Output formatting**: `ctl/console_formatting.py` (`AirflowConsole`) renders
  `--output` as `json` / `yaml` / `table` / `plain`; the payload must stay
  machine-parseable (data to stdout, diagnostics to stderr).
- **Auth & environments**: credentials are stored via `keyring` under a
  per-environment token key (`AIRFLOW_CLI_ENVIRONMENT`); the token is a JWT sent
  as a bearer to the API server. Environment names are untrusted input and are
  validated against path traversal.
- **Release process**: airflow-ctl is released from `main` and does **not** use
  newsfragments — user-facing notes go in `airflow-ctl/RELEASE_NOTES.rst`.

## Before opening a PR here — authoring-agent guard

**This is a high-criticality, expensive-to-review area that spans a trust
boundary and a generated public contract.** If you are an agent preparing a
change here on behalf of a person, first judge whether the change can be
**demonstrated end to end**: have you run the built CLI against a live API server,
authenticated through the real keyring/token path rather than a mock, and shown
the command's output and exit code before and after? Anything touching the
generated client also needs the regeneration re-run, not hand-edited.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** to build context first, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`).

## Review criteria

Mined from real review discussion on ~248 merged and 88 closed-unmerged PRs
touching this area — the changes reviewers repeatedly required, and the reasons
changes here get closed.
**If you are preparing a change here, treat this as a pre-flight checklist and
fix every applicable item _before_ opening the PR.** Triage applies the same
list: a PR that lands with unmet items is drafted back to its author with the
specific gaps. Ordered by how often reviewers raise each.

**Remote-client boundary (the defining concern here):**

- [ ] **Reach Airflow only through the public REST API.** A command handler
      must call the generated `api/operations.py` layer over `api/client.py` —
      **never** import an airflow-core module, open a DB session, or read
      server-side config/state. `airflowctl` is a separate distribution that
      runs on a remote host.
- [ ] **Don't add an airflow-core (or provider) runtime dependency** to make a
      command work — that re-couples the client to the server it is supposed to
      manage remotely. If the data isn't on the API, the fix is a server-side
      endpoint, not a local import.
- [ ] **Send `limit` / pagination and query params the API defines** — don't
      fetch unbounded or hand-roll a query shape the endpoint doesn't accept
      (cursor pagination and query-param handling are recurring review catches).
- [ ] **Tolerate an older server.** The client is installed separately from the
      deployment it manages, so it routinely talks to an Airflow older than the
      spec it was generated from. A request body carrying a field the older
      server rejects (`extra_forbidden`) breaks the command against every
      pre-release deployment — omit unset fields rather than sending nulls.

**Don't open a duplicate — this area is raced:**

- [ ] **Search for an open PR before writing one.** The AIP-94 command-porting
      backlog is issue-driven and each issue attracts two to four parallel
      attempts — two contributors shipped `tasks clear` at once, and two shipped
      the per-Dag-run task-state command at once. Comment on the issue to claim
      it, and if a PR already exists, review or build on it instead of opening a
      second. Parallel work is allowed and the better PR wins, so an existing
      open PR is a reason to coordinate, not a ground for closing either one.
- [ ] **Check whether the bug is already fixed on `main`.** Several closures here
      were "fixed by another PR" against a branch hundreds of commits stale —
      rebase onto `main` and re-confirm the failure before opening.

**Generated API layer & spec parity:**

- [ ] **Never hand-edit `api/datamodels/generated.py` or `auth_generated.py`.**
      They are produced by `datamodel-codegen` from the server's OpenAPI specs
      (`generate-airflowctl-datamodels` prek hook); regenerate from the updated
      server spec instead — a manual edit is reverted on the next run and drifts
      the client from the published contract.
- [ ] **A client change that consumes a new/changed field must follow a server
      API change**, not anticipate one — the datamodels track
      `v2-rest-api-generated.yaml`; if the field isn't in the regenerated spec,
      the server PR lands first.
- [ ] **Wrap new endpoints in `api/operations.py`** and keep the transport in
      `api/client.py` — don't scatter raw `httpx` calls through command bodies.

**CLI surface & output contract (backward-compatibility-sensitive):**

- [ ] **Command/subcommand names, flags, and argument arity are a stable UX
      contract** — renaming a command, flipping an option to positional, or
      changing a flag is a breaking change for scripts; justify it and note it
      in `airflow-ctl/RELEASE_NOTES.rst` (this distribution uses **no**
      newsfragments — it is released from `main`).
- [ ] **`--output` shapes stay machine-parseable and honoured for every mode**
      (`json` / `yaml` / `table` / `plain`) — a command that ignores `--output`,
      or leaks logs/warnings into the `json` payload on stdout, breaks pipelines.
      Diagnostics go to **stderr**.
- [ ] **Flag names stay consistent across related commands**, and required vs.
      optional parameters follow the established convention (required →
      positional, optional → `--flag`).
- [ ] Follow [`adr/0004`](adr/0004-cli-conventions-are-settled-once-for-the-whole-command-surface.md) —
      a convention spanning more than one command (argument style, error wording,
      exit codes, prompt behaviour) is agreed for the whole surface and applied
      across it, not changed for one command.
- [ ] Follow [`adr/0005`](adr/0005-cross-cutting-behaviour-is-derived-from-the-api-models.md) —
      required-field validation, input parsing and error mapping are derived from
      the generated API models for every command at once, not hand-rolled in a
      handler the next regeneration would overwrite.
- [ ] **Every operation has integration-test coverage** — the
      `check-airflowctl-command-coverage` prek hook pairs each `operations.py`
      method with a test in `test_airflowctl_commands.py`; a new command without
      one fails the hook.

**Auth, credentials & local-host safety:**

- [ ] **Treat token/keyring handling as security-sensitive** — store tokens
      under the per-environment key, don't print secrets, and let commands that
      don't need auth (e.g. a remote version check) run without prompting for
      credentials.
- [ ] **A token is printed only by a command whose whole purpose is printing it**
      — reviewers rejected a `--print-token` flag on `auth login` and asked for a
      separate `auth token` command instead, on the `gh auth login` / `gh auth
      token` model. A secret must never be a side effect of a command the user
      ran for another reason, and a command that exists to emit one needs no
      confirmation prompt.
- [ ] **Validate untrusted local input** — environment names and file paths from
      env vars / flags are sanitised against path traversal before use; don't
      interpolate them into filesystem paths unchecked.

**Code quality reviewers consistently require:**

- [ ] **No `raise AirflowException`** — use a Python built-in or a dedicated
      class in `airflowctl/exceptions.py`; surface the right **exit code** so a
      failed command is scriptable.
- [ ] **Imports at module top**; local/inline imports only for genuine
      circular-import reasons (say why). The one legitimate lazy path is CLI
      command loading via `lazy_load_command`.
- [ ] **Action-verb / intent-revealing names**; no shadowing builtins; reuse
      existing helpers (`ctl/utils`, `utils/`) rather than a third copy of a
      derivation that will drift between client and command.
- [ ] **Don't swallow exceptions with a broad `except`** — narrow to the real
      classes (`ServerResponseError`, the `AirflowCtl*` exceptions) so API and
      auth failures surface with a clear message and exit code.

**Tests, compatibility, process:**

- [ ] Test **exercises the actual new path and fails without the change** — for
      client work that means asserting the request sent to (and response parsed
      from) the API, with `httpx` transport mocked via `spec`/`autospec`, not a
      bare `MagicMock`. Assert on structured `caplog`, not substrings.
- [ ] **Rebase and go green before asking for review.** The single most common
      state of a closed PR here is a branch several hundred commits behind
      `main` with failing static checks and failing CTL integration tests. Run
      `prek run --from-ref main --stage pre-commit` locally; a reviewer will not
      unblock CI for you, and an untouched red PR is drafted back and then
      closed.
- [ ] **Show evidence of testing** — the command actually run against a server,
      and its output. Take contentious CLI semantics to the devlist before
      writing the command.

> Mined from PR review history; the sample skews to the airflow-ctl era (this
> distribution is new in the Airflow-3 line and still maturing its command
> surface), so conventions here are evolving faster than in core areas. Extend
> as new patterns emerge, and add an equivalent `## Review criteria` section to
> the `AGENTS.md` of every other area over time.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
The remote-client boundary, the generated-datamodel parity, and the CLI/output
contract are best aligned on _before_ the code, not during review. A new
top-level command group, or a change to what a command does or how it renders,
benefits from a short issue describing intent first; a client change that needs
a matching server API change should reference (or wait on) that server PR.
