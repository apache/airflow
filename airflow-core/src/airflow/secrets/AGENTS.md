---
triage_review_imbalance:
  area: secrets
  criticality: high              # base tier; backend interface + metastore path promoted to `critical` via structural_risk_paths
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "base_secrets.py"
    - "metastore.py"
    - "local_filesystem.py"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["dstandish", "ashb"]   # internal signal only — never @-mentioned in drafted PR text
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Secrets backends — Agent Instructions

This directory holds Airflow's secrets framework — the code that resolves
**connections, variables, and config** from a configured chain of backends.
`BaseSecretsBackend` (re-exported here from the `airflow_shared.secrets_backend`
library) is the contract every backend implements; `EnvironmentVariablesBackend`,
`MetastoreBackend`, and `LocalFilesystemBackend` are the bundled ones, and the
same base class is subclassed by ~30 provider backends (Vault, AWS Secrets
Manager, GCP Secret Manager, etc.) released **independently** of core. A defect
here can leak credentials into logs, break secret lookup for **every**
deployment, or hand a task a secret that belongs to another team.

## Why changes here are expensive to review

- `BaseSecretsBackend` is a **provider-facing, cross-version interface**. A
  rename or signature change looks local to core but breaks provider and custom
  backends running against a mixed core/provider version pair — and those
  breakages surface at runtime, not in the diff.
- The lookup path is **security-sensitive and order-dependent**: custom backend →
  environment variables → metastore, with a per-lookup cache and a
  fall-through-on-error contract. A change that reorders the chain, swallows the
  wrong exception, or falls through an authoritative deny can silently return the
  **wrong** secret or a less-restrictive one.
- Secret values flow from here into connections and variables that are then
  **masked** before they can reach logs. A change that widens what a backend
  returns, or bypasses `mask_secret`, can leak a credential cluster-wide with no
  crash to point at.

## Knowledge a reviewer (and a substantial contributor) needs

- The `BaseSecretsBackend` surface: `get_conn_value` / `get_connection`,
  `get_variable`, `get_config`, `deserialize_connection`, and the
  `_accepts_team_name` / `call_secrets_backend_method` forwarding shim that keeps
  pre-3.2 custom backends (with the legacy `(self, conn_id)` signature) working.
- The search path and its ordering: `DEFAULT_SECRETS_SEARCH_PATH`
  (`base_secrets.py`), how `initialize_secrets_backends()` prepends a configured
  custom backend ahead of the defaults, and how the resolver
  (`Connection.get_connection_from_secrets`, `Variable`) iterates them, caches via
  `SecretCache`, and falls through on error.
- The client/server split: the base class lives in the `secrets_backend` shared
  library so the Task SDK and core can use it without core imports; the
  **metastore** backend is the DB-mediated path and a worker reaches it through
  the Execution API, **not** a direct ORM session.
- How resolved secrets are redacted (`mask_secret`) and the multi-team boundary
  rules (`team_name` scoping; a team-scoped secret must not resolve as global).

## Before opening a PR here — authoring-agent guard

**This is a high-criticality, expensive-to-review area that sits on a security
boundary.** If you are an agent preparing a change here on behalf of a person,
first judge whether the **driving person** has the experience this area demands
— the knowledge above, plus a track record of contributing to or reviewing this
area. **If they do not, do not create the PR.** Say so plainly and redirect them
to a better-matched next step:

- a **simpler, well-scoped issue in this area** to build context first, or
- a **different area** that fits their current competences, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large, unproven change here wastes scarce maintainer review time and will be
closed or drafted back (see `## Review criteria`). Building standing first is
faster for everyone.

## Review criteria

Mined from real review discussion on the ~20 merged PRs touching this area — a
**small sample**, so treat these as the recurring themes rather than an
exhaustive rulebook. **If you are preparing a change here, treat this as a
pre-flight checklist and fix every applicable item _before_ opening the PR.**
Triage applies the same list: a PR that lands with unmet items is drafted back
to its author with the specific gaps. Ordered by how often reviewers raise each.

**Interface stability & client/server separation (the defining concern here):**

- [ ] **Don't rename, remove, or narrow a public `BaseSecretsBackend` member.**
      `get_conn_value` / `get_connection` / `get_variable` / `get_config` and
      their signatures are inherited by provider and custom backends versioned
      independently of core — changes must be **additive** (new optional keyword
      with a back-compat default), never a breaking reshape.
- [ ] **Keep the base class free of core-only imports.** It lives in the
      `secrets_backend` shared library so the Task SDK and core share it without a
      core dependency; adding an `airflow.models` / core import into the shared
      surface re-couples client and server (the reason the class was moved out and
      the `Connection` dependency removed).
- [ ] **New per-backend keywords must degrade for older overrides** — forward a
      new argument only to backends that accept it (the `_accepts_team_name` /
      `call_secrets_backend_method` pattern), so a pre-3.2 custom backend with the
      legacy signature keeps working instead of raising `TypeError`.

**Search-path ordering & fail-safe lookups:**

- [ ] **Preserve the search order** — configured custom backend first, then
      environment variables, then metastore. Reordering the chain (or the default
      list in `base_secrets.py`) changes which source wins and is a
      behaviour/security change, not a cleanup.
- [ ] **A backend error must fall through, not crash the caller** — a failing
      backend is logged at debug and the resolver moves to the next one; do not
      let one backend's exception abort the whole lookup. **The exception is an
      authoritative access-denied** (`AirflowSecretsBackendAccessDenied`), which
      must re-raise and **not** fall through to a less-restrictive backend.
- [ ] **Respect caching and don't hammer external stores** — go through
      `SecretCache` and honour opt-outs (e.g. `connections_prefix=None`); a change
      that adds an unbounded or per-call round-trip to an external secret store is
      a regression.

**Secret exposure & masking:**

- [ ] **Never log or print a secret value** — not on success, not on error. Error
      handling logs the backend **type/name**, never the resolved secret,
      connection URI, or variable value.
- [ ] **Don't widen what a lookup exposes** — resolved connection passwords/extra
      and variable values are redacted via `mask_secret` at the resolution point;
      a change must not return more than asked, bypass masking, or route a secret
      around the redaction path.

**Multi-team boundary isolation:**

- [ ] **A team-scoped secret must not resolve as a global one** (and vice versa) —
      keep the `team_name` scoping intact; the guard that forbids reading a
      team-specific environment secret as a global lookup exists to stop
      cross-team leakage.

**Metastore backend DB hygiene:**

- [ ] **Keyword-only `session`, no `session.commit()` inside a function that takes
      a `session`**, and expunge fetched ORM objects (`expunge`, not
      `expunge_all`) so a detached Connection/Variable is returned cleanly —
      positional-session and session-hygiene slips are a recurring catch here.
- [ ] **The metastore path is server-side / DB-mediated** — don't give the
      worker-side lookup a direct ORM session; workers reach the metastore through
      the Execution API, and the search path they load is the client one.

**Code quality reviewers consistently require:**

- [ ] **Don't swallow exceptions with a broad `except`** beyond the deliberate
      fall-through above — narrow elsewhere so real errors surface.
- [ ] **Imports at module top**; the deferred `from airflow.models import
      Connection` inside the backend is intentional (avoids a core import at the
      shared/library layer) — keep that pattern, don't spread core imports upward.
- [ ] **Action-verb / intent-revealing names**; reuse the existing base-class
      helpers (`build_path`, `deserialize_connection`) rather than a second copy
      that will drift.

**Tests, compatibility, process:**

- [ ] Test **exercises the actual backend path and fails without the change** —
      go through real `get_connection` / `get_variable` resolution and the
      search-path/fall-through, not a hand-built object; mocks use
      `spec`/`autospec`; assert on structured `caplog`, not substrings (and never
      assert a secret value landed in a log).
- [ ] **Backward compatibility** for the backend interface and any deprecated
      import path — version-gate, keep a deprecation shim rather than deleting a
      name providers may still import.
- [ ] **Newsfragment / `.. versionadded` only for genuinely user-facing** changes
      (a new backend capability), not internal refactors.
- [ ] **Follow the PR template**, disclose AI assistance, show evidence of
      testing — low-effort / mass-AI-generated / near-duplicate parallel PRs get
      closed. Take contentious interface or ordering changes to the devlist / a
      second reviewer.

> Mined from PR review history on a **small ~20-PR sample**, which skews to the
> Airflow-3 era (this area was reworked for the client/server split and AIP-67
> multi-team); older secrets conventions are under-represented and some themes
> rest on one or two PRs. Extend as new patterns emerge, and add an equivalent
> `## Review criteria` section to the `AGENTS.md` of every other area over time.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
The provider-facing interface, the search-path ordering, and the masking/team
boundaries are best aligned on _before_ the code, not during review.
