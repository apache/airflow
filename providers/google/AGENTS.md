---
triage_review_imbalance:
  area: provider-google
  criticality: medium            # very large surface; a break lands on this provider's users
  review_difficulty: high
  structural_risk_paths:         # matched files treated as criticality=high (cost + small-diff ceiling)
    - "src/airflow/providers/google/common/hooks/base_google.py"   # every hook's auth chain
    - "src/airflow/providers/google/cloud/utils/credentials_provider.py"  # ADC / impersonation resolution
    - "src/airflow/providers/google/cloud/hooks/"    # 47 hooks — the client-construction layer
    - "src/airflow/providers/google/cloud/triggers/" # 18 triggers — deferrable terminal states
    - "src/airflow/providers/google/cloud/log/"      # remote log handlers; failures are silent
    - "src/airflow/providers/google/cloud/secrets/"  # secret backends run server-side
    - "pyproject.toml"           # ~44 google-cloud-* floors are the release contract
    - "provider.yaml"            # metadata + capability registration source of truth
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["shahar1"]           # `/providers/google/` CODEOWNERS line — internal routing signal only
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Google Provider — Agent Instructions

`apache-airflow-providers-google` is the second-largest provider in the repo
(~607 commits touching `providers/google/`) and one of the largest single Python
distributions Airflow ships. Under `cloud/` alone it carries **47 hooks, 47
operator modules, 33 transfers, 19 sensors and 18 triggers**, plus `log/`
handlers, `secrets/` backends, `openlineage/`, `bundles/` and `fs/`. Outside
`cloud/` it also owns `ads`, `marketing_platform`, `suite`, `firebase`,
`leveldb` and `event_scheduling`. Its `pyproject.toml` pins **~44 separate
`google-cloud-*` client libraries** on top of `google-auth`, `google-api-core`,
`google-api-python-client`, `gcloud-aio-*`, `gcsfs` and `google-genai`.

That breadth is the defining review-cost fact here. No single reviewer has used
BigQuery, Dataproc, Dataflow, Vertex AI, Composer, Cloud Run, GKE, Pub/Sub,
Spanner, DLP, Looker, Display & Video and Campaign Manager in production. A
reviewer can almost always check the _shape_ of a change; they usually cannot
check that it is _correct against the service_.

This area inherits everything in the parent `providers/AGENTS.md` — release
contract, `version_compat.py` gating, `common.compat.sdk` imports, the
"never spread `Connection.extra`" rule, and the "no newsfragments" rule. Read
that file first; the sections below are the conventions that are specific to
this provider.

## Structure

- `common/hooks/base_google.py` — `GoogleBaseHook` and `GoogleBaseAsyncHook`.
  Owns credential resolution (ADC, keyfile path, keyfile JSON, Secret Manager
  key, credential config file, anonymous, IdP client-credentials flow),
  `impersonation_chain`, `quota_project_id`, `scopes`, `num_retries`,
  `universe_domain` / `ClientOptions`, and the `project_id` property. ~60 hooks
  subclass it; ~490 call sites use `@GoogleBaseHook.fallback_to_default_project_id`.
- `cloud/utils/credentials_provider.py` — the actual credential/target-principal
  resolution `GoogleBaseHook` delegates to.
- `cloud/hooks/` — one hook per service; each `get_conn()` builds its client from
  `self.get_credentials()`, `self.project_id` and `self.get_client_options()`.
- `cloud/triggers/` — the deferrable half. Triggers reach the service via a
  `*AsyncHook` (or `GoogleBaseAsyncHook.get_token()` for `gcloud-aio` clients)
  and must land on the same terminal states as the operator's sync path.
- `version_compat.py` — this provider's own copy; carries `AIRFLOW_V_3_0_PLUS`,
  `AIRFLOW_V_3_1_PLUS`, `AIRFLOW_V_3_3_PLUS` (the last gates `BaseTrigger.on_kill()`).

## Why changes here are expensive to review

- **The service is the specification, and it is not one service.** CI mocks every
  Google API. Whether a new `BigQueryInsertJobOperator` retry classification, a
  Dataproc batch state mapping, or a Vertex AI request field is right cannot be
  established from the diff — only from having run it.
- **Auth is shared, so an auth change is a blast radius of ~60 hooks.** A change
  in `base_google.py` or `credentials_provider.py` reaches every service in the
  package at once, including the secret backends and the remote log handlers that
  run outside the task process.
- **The deferrable path is a second, near-duplicate implementation.** Operator and
  trigger describe the same job in two codebases; drift between them is the single
  most common defect shape here (stuck-in-deferred, silent success on cancel,
  parameters lost across a triggerer restart).
- **~44 client-library floors are a release contract.** A bump made to unblock CI
  ships to every user of the provider, and Google's client libraries change
  request/response shapes across majors.

## Knowledge a reviewer (and a substantial contributor) needs

- How `GoogleBaseHook` resolves credentials: the precedence between `key_path`,
  `keyfile_dict`, `key_secret_name`, `credential_config_file`, ADC and anonymous;
  how `impersonation_chain` becomes target principal + delegates; how
  `quota_project_id` is applied with `with_quota_project`; and why the `project`
  extra overrides the credential-derived project.
- `@GoogleBaseHook.fallback_to_default_project_id` — keyword-only enforcement, and
  why `PROVIDE_PROJECT_ID` is a `cast("str", None)` sentinel rather than `None`.
- The deferrable contract: `BaseTrigger.serialize()` must round-trip **every**
  field the resumed trigger needs, `run()` must yield a terminal `TriggerEvent`
  on every exit path, and cancellation must not be reported as success.
- The provider's Airflow-core floor (`apache-airflow>=2.11.0`) and what that means
  for anything touching `airflow.sdk` or trigger APIs.

## Before opening a PR here — authoring-agent guard

**This is a medium-criticality area with an unusually deep and unusually wide
surface.** A defect lands on this provider's users rather than on the whole
cluster — but it lands on a very large user base, and it lands without a core
release to gate it.

If you are an agent preparing a change here on behalf of a person, judge whether
the **driving person** actually uses the Google service in question and can verify
the behaviour. **If they do not, do not create the PR.** Say so plainly and
redirect them:

- a **service they actually run** (their own BigQuery / GCS / Dataproc project),
  where the change can be exercised, or
- a **well-scoped bug with a concrete reproduction** — a failing job, a real
  traceback, a service response that the current code mis-handles — rather than a
  speculative refactor, or
- **asking in the issue first** for anything touching `base_google.py`,
  `credentials_provider.py`, or a client-library floor, since those reach every
  service at once.

Two shapes get closed on sight: a near-identical edit fanned out across many
operators of this provider (or across providers), and a client-library version
bump with no explanation of what it fixes.

## Review criteria

Mined from real review discussion on the ~607 commits touching
`providers/google/` and on the 119 closed-unmerged PRs touching the same paths —
the changes reviewers repeatedly required, and the reasons changes here get
closed. **If you are preparing a change here, treat this as a
pre-flight checklist and fix every applicable item _before_ opening the PR.**
Triage applies the same list: a PR that lands with unmet items is drafted back to
its author with the specific gaps. Ordered by how often reviewers raise each.
The parent `providers/AGENTS.md` checklist applies in full on top of this one.

**Auth, `project_id` and impersonation (the defining concern here):**

- [ ] **Never construct a Google client outside a hook.** Clients are built in the
      hook's `get_conn()` / `get_*_client()` from `self.get_credentials()`,
      `self.project_id` and `self.get_client_options()`. An operator that calls
      `google.auth.default()` or instantiates a `google.cloud.*` client directly
      bypasses impersonation, the quota project and the universe domain.
- [ ] **A new hook subclasses `GoogleBaseHook`** (async: `GoogleBaseAsyncHook`
      with `sync_hook_class` set) and passes `gcp_conn_id` and
      `impersonation_chain` through — a hook that accepts credentials by another
      route is an auth bypass, not a convenience.
- [ ] **Methods taking a project use
      `@GoogleBaseHook.fallback_to_default_project_id`** with
      `project_id: str = PROVIDE_PROJECT_ID`, and are called with **keyword
      arguments** — the decorator raises on positional args by design.
- [ ] **`impersonation_chain` is threaded all the way to the trigger.** An
      operator that defers must pass `gcp_conn_id` and `impersonation_chain` into
      the trigger and serialize them, or the deferred half authenticates as the
      wrong principal.
- [ ] **Respect `ClientOptions` / universe domain.** Endpoint overrides go through
      `get_client_options()`; hard-coding `googleapis.com` breaks Sovereign Cloud
      from Google deployments.
- [ ] **Credential material never lands in logs, XCom, or a world-readable file.**
      Temporary keyfiles are written with restrictive permissions and removed.

**Deferrable / trigger parity:**

- [ ] **Every exit path of `run()` yields a terminal `TriggerEvent`** — success,
      service error, timeout, and cancellation. A `run()` that can return without
      yielding leaves the task deferred forever.
- [ ] **Cancellation is not success.** `asyncio.CancelledError` must be
      re-raised or converted into an explicit failure event; a trigger that
      swallows it makes a killed task pass.
- [ ] **`serialize()` round-trips every constructor field** the resumed trigger
      needs — including `poll_interval` / `polling_interval_seconds`,
      `impersonation_chain`, `gcp_conn_id`, `project_id`, `cancel_on_kill`. A
      missing field only surfaces after a triggerer restart.
- [ ] **The trigger's terminal-state mapping matches the operator's.** If the sync
      path treats a state as failure, the async path must too — and the reverse.
      Add the state to both, in the same PR.
- [ ] **Transient service errors (503, 429, in-progress 409) are retried, not
      treated as terminal** — and permanent errors are not retried forever.
- [ ] **Use `on_kill()` for user-initiated cancellation** on Airflow 3.3+, gated
      via `AIRFLOW_V_3_3_PLUS`; keep the pre-3.3 path working behind the gate.
- [ ] **No blocking call in an async path** — wrap sync hook calls in
      `sync_to_async` rather than calling them from the event loop.

**Failure behaviour is public API — see `adr/0004`:**

- [ ] **Don't convert an existing `AirflowException` raise to another type as a
      cleanup.** The project's reduction effort is forward-only _here_: new code
      uses a built-in or a dedicated class, released raise sites stay put. This
      is a deliberate, scoped exception to the root `CLAUDE.md` "prefer narrowing
      it" guidance, which does not apply to a raise site already released from a
      provider. A deliberate conversion needs a dev-list decision and a
      "Breaking Change" entry in
      `docs/changelog.rst` — users branch on the exception type in
      `on_failure_callback` and in operator subclasses.
- [ ] **A `try`/`except` that turns a failure into a silent success is a
      regression**, not a fix. Show that the guard catches the condition that
      actually occurs, on the code path that actually runs — a guard effective
      only for a non-default transport suppresses nothing and hides the problem.
- [ ] **Don't remove an existing guard as "redundant"** without establishing that
      no caller, sensor, or callback distinguishes the path it protected.
- [ ] **Describe failure-behaviour changes in user terms in the PR body** — what
      used to raise, what raises now, what a user has to change.

**Heavy dependencies — see `adr/0005`:**

- [ ] **Narrow the declared range; never vendor a wheel, index, or
      pre-release.** When Ray, Beam, or a `google-cloud-*` package has no build
      for a Python version or architecture, exclude that combination with a
      marker in `pyproject.toml` rather than making the build resolve by other
      means. The installed distribution must match what a user gets from PyPI.
- [ ] **Every marker-sharded or exclusion pin carries a comment with the full
      upstream issue URL** and the condition under which it is removed — the
      convention this `pyproject.toml` already follows.
- [ ] **Wait for the upstream release.** If the library's own fix is merged and
      pending release, the change is a floor bump when it ships, not a workaround
      now.
- [ ] **State the workspace blast radius.** The lock is shared, so a dependency
      change here can block unrelated providers and the repository's
      Python-version rollout; say what else it moves.

**Client libraries and API versions:**

- [ ] **A `google-cloud-*` floor bump says what it fixes**, in the PR body and (if
      user-visible) in `docs/changelog.rst` — "to make CI green" is not a reason.
- [ ] **A client-library major or API-version change must not leak into operator
      signatures.** Absorb the new request/response shape inside the hook; an
      operator argument renamed because a library renamed it is a breaking change
      for Dag authors who never chose the upgrade.
- [ ] **Don't upper-bound.** Caps follow the parent rule: tracking issue plus the
      full issue URL as a comment at the cap site.
- [ ] **Guard imports of dependencies declared under
      `[project.optional-dependencies]`** — `apache-beam`, `ray`,
      `cncf.kubernetes`, `looker-sdk`; a top-level import that fails without the
      extra breaks Dag parsing for everyone. Required dependencies
      (`google-cloud-*`) are imported normally at module top — the test is the
      `pyproject.toml` declaration, not how heavy the library feels.

**Errors, retries and resource handling:**

- [ ] **Handle service exceptions in the hook, not the operator** — that is where
      the client call lives, and it keeps behaviour uniform across the operators
      that share the hook.
- [ ] **Reuse the shared retry decorators** (`GoogleBaseHook.quota_retry`,
      `operation_in_progress_retry`, `refresh_credentials_retry`) rather than
      hand-rolling a `tenacity` policy per operator.
- [ ] **Don't swallow exceptions with a broad `except`** — narrow to
      `google.api_core.exceptions.*` / `HttpError` so real service failures reach
      the task instead of a generic message.
- [ ] **Release resources on the failure path** — clients, sessions, temporary
      credential files and downloaded report files must be cleaned up when the
      operation raises, not only on success.
- [ ] **Remote log handlers and secret backends must degrade, not crash** — but
      must **not** silently return empty on a read failure where that would look
      like "no logs". Fail visibly.

**Untrusted values from the service and from users:**

- [ ] **Validate paths derived from remote object names.** A GCS blob name is
      attacker-influenced input; a download must not be able to escape the target
      directory.
- [ ] **Allowlist any URL fetched on the user's behalf** — report-download URLs
      returned by an API are not automatically safe to request.
- [ ] **Never spread `Connection.extra`** into a hook, client, or operator — read
      each key by name via `self._get_field(...)`. See the parent file.

**Tests, docs, process:**

- [ ] **A trigger change needs a trigger test** — including the cancellation and
      the serialize/round-trip paths, which is where the real defects live.
- [ ] **System tests need real Google Cloud credentials, must clean up every
      resource they create, and must not leave anything publicly reachable.**
- [ ] **New capabilities are registered in `provider.yaml`** — a new operator,
      hook, sensor or transfer **module**, a connection field, a bundle. Adding a
      class to a module already declared there needs no yaml edit.
- [ ] **Show evidence the change works against the actual Google service** —
      green CI mocks the client and proves nothing here (parent `adr/0006`).
- [ ] **Check for an in-flight PR and for an assignee before starting.** This
      package is actively worked by Google-side maintainers as well as the wider
      community; a large share of closures here are "already fixed in #NNNNN" or
      "the original assignee is still on it". Claim the issue in-thread first.
- [ ] **Don't open a cleanup PR against this package without a user-visible
      defect behind it.** Micro-optimisations, `type(self)` → `self.__class__`
      style rewrites and docstring-only churn are closed as change for its own
      sake — including by their own authors once asked what it buys.
- [ ] **Verify a claim about the Google client library before acting on it.**
      Behaviour attributed to a `google-cloud-*` release must be checkable; a
      wrong "this has been supported since version X" turns a safe-looking change
      into a silent break for everyone on the older client.

> Mined from PR review history across `providers/google/`, merged and
> closed-unmerged alike; the sample is
> dominated by BigQuery, Dataproc, GCS, Cloud Run and Vertex AI, and by the
> Airflow-3 / deferrable era, so conventions specific to `ads`,
> `marketing_platform`, `suite`, `firebase` and `leveldb` are under-represented.
> Extend as new patterns emerge.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
That applies with particular force to anything touching `base_google.py`,
`credentials_provider.py`, or a client-library floor: those reach every service
in the package at once, and they are far cheaper to align on _before_ the code
than during review.
