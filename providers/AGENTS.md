---
triage_review_imbalance:
  area: providers
  criticality: medium            # a provider break affects that provider's users, not the whole cluster
  review_difficulty: medium
  structural_risk_paths:         # matched files treated as criticality=high (cost + small-diff ceiling)
    - "*/provider.yaml"          # metadata + capability registration source of truth
    - "*/pyproject.toml"         # generated from a template; dependency lists are the release contract
    - "*/docs/changelog.rst"     # the only user-visible release note channel (no newsfragments)
    - "*/version_compat.py"      # per-provider core-version gate, copied not imported
    - "common/compat/"           # cross-version shim every other provider depends on
    - "common/sql/"              # depended on by ~30 SQL providers
  codeowners_ref: ".github/CODEOWNERS"
  # NOTE: ownership here is PER-PROVIDER — `.github/CODEOWNERS` lists a different
  # owner (or none) for each provider directory. There is no single owner for all
  # of `providers/`. The list below is a small set of frequent cross-provider
  # reviewers, used as an internal routing signal only — never @-mentioned in
  # drafted PR text. For a concrete PR, prefer the per-provider CODEOWNERS entry.
  experts: ["potiuk", "eladkal", "jscheffl", "shahar1"]
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Providers — Agent Instructions

Each provider is an independent package with its own `pyproject.toml`, tests, and documentation.

## Structure

- `provider.yaml` — metadata, dependencies, and configuration for the provider.
- Building blocks: Hooks, Operators, Sensors, Transfers.
- Use `version_compat.py` patterns for cross-version compatibility.

## Checklist

- Keep `provider.yaml` metadata, docs, and tests in sync.
- Don't upper-bound dependencies by default; add limits only with justification.
- Changing a symbol another provider imports from yours? Bump the consuming provider's dependency with a `# use next version` marker so independently-released packages stay compatible — see [`contributing-docs/13_airflow_dependencies_and_extras.rst`](../contributing-docs/13_airflow_dependencies_and_extras.rst).
- Tests live alongside the provider — mirror source paths in test directories.
- Full guide: [`contributing-docs/12_provider_distributions.rst`](../contributing-docs/12_provider_distributions.rst)

## Changelog — never use newsfragments

**Never create newsfragments for providers.** Providers are released from `main` in waves, so
per-PR newsfragments are not consumed by the release process — the release manager regenerates the
changelog from `git log`. The towncrier-managed `newsfragments/` workflow is used only by
`airflow-core/`, `chart/`, and `dev/mypy/`. (`airflow-ctl/` follows the same "no newsfragments,
direct edit" pattern as providers — see `dev/README_RELEASE_AIRFLOWCTL.md`.)

When a provider change needs a user-visible note (typically a breaking change or important behavior
change that warrants explanation), update the provider's `docs/changelog.rst` directly in the same
PR, just below the `Changelog` header — exactly as the in-file `NOTE TO CONTRIBUTORS` block
describes. Routine entries (features, bug fixes, misc) are collected automatically by the release
manager from commit messages, so most PRs do not need to touch the changelog at all.

## Security: Connection extras must not be forwarded blindly to hooks/operators

**Never pass the whole `Connection.extra` dict (or `**conn.extra_dejson`) as
keyword arguments to a hook, operator, client constructor, or any underlying
library call.** Forward only the specific extra keys you have explicitly
reviewed and know are safe to expose.

### Why this matters — different security boundaries

Airflow has two distinct user roles with different trust levels:

- **Connection editors** — UI/API users with permission to create or edit
  Connections. They control the host, login, password, and the `extra` JSON
  blob.
- **Dag authors** — users who write the Python code that constructs hooks
  and operators. They control which arguments are passed at call sites.

These are *not* the same population, and the security model treats them
differently. A Connection editor is trusted to supply credentials for a target
system; they are **not** trusted to alter how the worker process behaves, load
arbitrary Python code, change file paths the worker reads, or pass options
into client libraries that the Dag author did not opt into.

### What goes wrong when extras are forwarded blindly

Many client libraries accept constructor or method kwargs that are dangerous
when attacker-controlled. Concrete examples seen in the wild:

- A kwarg that names a Python callable, plugin path, or import string —
  attacker sets it to a module they control → **remote code execution** on
  the worker.
- A kwarg that points at a local file (cert path, key file, log path,
  config file) — attacker redirects it to read or overwrite worker-side
  files.
- A kwarg that sets a proxy, endpoint URL, or hostname — attacker
  redirects traffic to an MITM endpoint and harvests credentials or
  tokens for *other* systems.
- A kwarg that toggles TLS verification, signing, or auth — attacker
  silently downgrades security.
- A kwarg that controls subprocess execution, shell invocation, or
  template rendering — attacker reaches command/template injection.

In all of these, the Connection editor effectively gains capabilities
(RCE, file read/write, traffic redirection, auth bypass) that the security
model does not grant them.

### The rule

- **Allowlist, never passthrough.** In the hook, read each extra key by
  name (`conn.extra_dejson.get("region_name")`,
  `conn.extra_dejson.get("verify")`, …) and pass only those named values
  forward. Reject or ignore unknown keys.
- **Do not** write `SomeClient(**conn.extra_dejson)`,
  `hook = MyHook(**conn.extra_dejson)`, or
  `kwargs.update(conn.extra_dejson)` followed by a downstream call.
- When adding support for a new extra key, treat it like any other public
  argument: review what the underlying library does with it, and document
  it in the provider's connection docs.
- If a Dag author genuinely needs to pass a non-allowlisted option, that
  option should be a **Dag-author-supplied argument** on the operator or
  hook (with its own review), not something a Connection editor can set.

### When reviewing provider PRs

Flag any of these patterns:

- `**conn.extra_dejson` or `**self.extra_dejson` spread into a constructor
  or call.
- Looping over `conn.extra_dejson.items()` and forwarding every key.
- New code paths where extras are merged into `kwargs` and then passed on
  unfiltered.
- "Convenience" features that let users put arbitrary client kwargs into
  `extra` — they widen the Connection-editor blast radius.

## Before opening a PR here — authoring-agent guard

**This is a medium-criticality area with an unusually wide surface.** A defect in
one provider affects that provider's users rather than the whole cluster, so the
bar is lower than for scheduler or Dag-processing changes — but each provider is
an **independently released distribution** with its own users, its own version
history, and (usually) its own CODEOWNER. Three things make changes here
expensive out of proportion to their diff size:

- The **third-party service** the provider wraps is the real specification. A
  reviewer who has not used that service cannot confirm the change is correct,
  and CI mocks the service rather than calling it.
- The change ships to users on a **provider release cadence independent of
  core**, against a *range* of Airflow versions — so "works on my main checkout"
  is not evidence it works where it ships.
- Dependency and metadata edits (`provider.yaml`, `pyproject.toml` dependency
  lists) are the release contract, and a mistake there breaks installs for
  everyone on that provider, not just users of the code path you touched.

If you are an agent preparing a change here on behalf of a person, judge whether
the **driving person** actually knows the third-party service and has a reason to
believe the change is correct against it. **If they do not, do not create the
PR.** Say so plainly and redirect them:

- a **provider they actually use in production**, where they can verify behaviour, or
- a **well-scoped bug with a concrete reproduction** rather than a speculative
  refactor across many providers, or
- **asking in the issue first** when the change spans several providers or
  touches a `common.*` provider that others depend on.

Mass, near-identical changes fanned out across many providers are the single most
common shape of low-value provider PR. They multiply reviewer load by the number
of providers touched and are routinely closed. Split by provider only when each
split is independently justified; otherwise do not open it.

## Review criteria

Mined from real review discussion on the ~4,600 commits touching `providers/` —
the changes reviewers repeatedly required, and the reasons changes here get
closed. **If you are preparing a change here, treat this as a pre-flight
checklist and fix every applicable item *before* opening the PR.** Triage applies
the same list: a PR that lands with unmet items is drafted back to its author
with the specific gaps. Ordered by how often reviewers raise each.

**Cross-version compatibility with Airflow core (the defining concern here):**

- [ ] **The provider must still work on the oldest core it declares.** The floor
      is the `apache-airflow>=X.Y.Z` pin in the provider's `pyproject.toml` (most
      providers are still on `>=2.11.0`). Using a core API that only exists in a
      newer release, without a version gate, silently breaks every user on the
      declared floor.
- [ ] **Gate new-core-only behaviour through the provider's own
      `version_compat.py`** (`AIRFLOW_V_3_0_PLUS`, `AIRFLOW_V_3_1_PLUS`, …). That
      file is **copied** into each provider deliberately, not imported from
      another provider or from tests — a prek hook rejects a `version_compat`
      import that resolves outside the provider's own root.
- [ ] **Import `conf` from `airflow.providers.common.compat.sdk`**, never from
      `airflow.configuration` or `airflow.sdk.configuration` (executors, which run
      inside airflow-core, are the one carve-out). Enforced by a prek hook.
- [ ] **Raise the declared core floor rather than silently depending on a newer
      core** — if the change genuinely needs a newer Airflow, bump the pin and say
      so, rather than leaving the old floor declared and broken.

**Dependencies and generated metadata:**

- [ ] **Edit dependencies in the provider's `pyproject.toml` in place**, then run
      `prek update-providers-dependencies --all-files`. The file is generated from
      `dev/breeze/src/airflow_breeze/templates/pyproject_TEMPLATE.toml.jinja2` and
      carries a "THIS FILE IS AUTOMATICALLY GENERATED" banner — everything except
      the dependency lists is regenerated, so hand-edits elsewhere are lost.
- [ ] **`provider.yaml` is the source of truth for provider metadata** —
      `versions`, `integrations`, `operators`, `hooks`, `connection-types`,
      `config`, `executors`, `excluded-platforms`. Registering a new capability
      means adding it there, not only in Python; several prek hooks fail when the
      code and `provider.yaml` disagree.
- [ ] **Don't upper-bound a dependency by default.** A cap is a deliberate,
      justified decision with a tracking issue and a comment carrying the full
      issue URL at the cap site, not a way to make CI green.
- [ ] **Bump the consuming provider's dependency with a `# use next version`
      marker** when you change a symbol another provider imports from yours —
      independently-released packages otherwise drift out of compatibility. See
      `contributing-docs/13_airflow_dependencies_and_extras.rst`.
- [ ] **Adding an extra means documenting it** — optional dependency groups show
      up in the provider's generated docs and README; keep them in sync rather
      than leaving a stale or undocumented extra.

**Boundaries (architecture invariants, not preferences):**

- [ ] **Consume core through the public / Task-SDK surface only** — subclass
      `airflow.sdk` base classes and import from `airflow.sdk`; do not reach into
      airflow-core internals, private modules, or the ORM. Provider code runs on
      workers, which must not touch the metadata DB directly.
- [ ] **Never spread `Connection.extra` into a hook, operator, or client
      constructor** — allowlist each key by name. See the security section above;
      this is the highest-severity recurring finding in provider review.
- [ ] **Don't add a cross-provider import to reuse a helper.** Copy the small
      helper (as `version_compat.py` is copied) or put it in a `common.*`
      provider — an incidental import creates a release-coupling between two
      independently versioned packages.

**Code quality reviewers consistently require:**

- [ ] **Release resources on the failure path too** — file handles, sessions, and
      clients opened in a hook must be closed when the operation raises, not only
      on the happy path.
- [ ] **Don't swallow exceptions with a broad `except`** — narrow to the real
      classes so genuine service errors surface instead of being reported as a
      generic failure.
- [ ] **Imports at module top**; heavy third-party SDK imports guarded with
      `TYPE_CHECKING` where they are type-only. No provider discovery or file I/O
      at import time.
- [ ] **Action-verb / intent-revealing names**; reuse the provider's existing hook
      rather than opening a second client construction path that will drift.
- [ ] **Malformed data from the remote service must degrade, not crash** — a bad
      log line or unexpected response shape should not take down log fetching or
      the task.

**Docs, changelog, tests, process:**

- [ ] **Never add a newsfragment.** Providers are released from `main` and the
      release manager regenerates the changelog from `git log`. For a genuinely
      user-visible note (typically a breaking change), edit
      `providers/<provider>/docs/changelog.rst` directly, just under the
      `Changelog` header, as the in-file `NOTE TO CONTRIBUTORS` block describes.
- [ ] **A breaking change is called out explicitly** — in the PR description and,
      when users need migration guidance, in the changelog. Silent behaviour
      changes in a provider surface as a production break on the next release.
- [ ] **Tests mirror the source path** under the provider's own `tests/`, use
      `spec`/`autospec` when mocking the service client, and **fail without the
      change**. Do not assert on raw log text.
- [ ] **System tests need real credentials and must not leave public
      infrastructure behind** — a system test that provisions a publicly
      reachable resource is a defect.
- [ ] **Follow the PR template**, disclose AI assistance, and show evidence the
      change works against the actual service — low-effort, mass-generated, or
      fanned-out-across-providers PRs get closed.

> Mined from PR review history across `providers/`; the sample is dominated by
> the handful of large providers (amazon, google, cncf.kubernetes, common.*) and
> by the Airflow-3 era, so conventions specific to small or rarely-touched
> providers are under-represented. Ownership is per-provider — always check
> `.github/CODEOWNERS` for the directory actually being changed. Extend as new
> patterns emerge.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
That applies especially to changes that **span multiple providers**, touch a
`common.*` provider other providers depend on, or raise a provider's declared
Airflow-core floor: those are release-contract decisions and are far cheaper to
align on *before* the code than during review.
