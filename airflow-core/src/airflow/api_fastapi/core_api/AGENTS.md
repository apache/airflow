---
triage_review_imbalance:
  area: core-api
  criticality: high              # base tier; the public contract surface + auth deps promoted to `critical` via structural_risk_paths
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "security.py"
    - "openapi/"
    - "routes/"
    - "datamodels/"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["pierrejeambrun", "ephraimbuddy", "bugraoz93"]   # internal signal only — never @-mentioned in drafted PR text
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Core API (public REST API v2 & UI endpoints) — Agent Instructions

This directory is the **API server**: the FastAPI application (`app.py`) that
serves the public REST API v2 (`routes/public/`) and the endpoints the React UI
consumes (`routes/ui/`). It is the **single mediator between every client and
the metadata database** — external REST clients, generated language SDKs, and
the bundled UI all reach Airflow's data only through the routes here, gated by
the auth dependencies in `security.py`. A change here is visible to every
integrator: the request/response shapes are a published, OpenAPI-generated
contract (`openapi/v2-rest-api-generated.yaml`), and a regression can break
downstream clients, leak internals through the wrong status code, or hand a
caller data they are not authorized to see.

## Why changes here are expensive to review

- Response and request shapes are a **stable, generated contract**. The UI's
  TypeScript client and the published Python/Go SDKs are code-generated from the
  OpenAPI spec in `openapi/`; a field renamed, retyped, or dropped ripples out to
  consumers a reviewer cannot see from the diff.
- **Authorization is enforced by FastAPI dependencies**, not by the route body.
  Whether a new or edited endpoint carries the right `requires_access_*` /
  permitted-filter dependency — and scopes it to the right resource — is easy to
  get subtly wrong and rarely obvious from the handler code alone.
- **Error handling is contractual.** A domain error that escapes as a raw `500`
  both leaks internals and misleads clients; getting the status code and detail
  right at the route boundary is a recurring, easy-to-miss review dimension.
- Endpoints run **user-driven queries against shared tables**. An unpaginated or
  N+1 query that passes in a test can degrade the API server under a real
  deployment's data volume.
- This area has the **highest parallel-work collision rate in the repo**. Endpoint
  bugs are user-visible and look small, so the same defect routinely draws three
  or four concurrent PRs; a reviewer then has to compare them and close all but
  one. Checking for in-flight work is not politeness here, it is the main way to
  avoid wasting both your effort and a maintainer's.
- **Performance PRs dominate the closed-unmerged record.** Optimisation claims are
  cheap to write and expensive to disprove, and disproving them falls on a
  maintainer with their own hardware. Reviewers here benchmark before believing.

## Knowledge a reviewer (and a substantial contributor) needs

- The FastAPI app wiring: how `routes/public/` and `routes/ui/` mount, and that
  `routes/ui/` is a **private, UI-only** surface (spec `_private_ui.yaml`) while
  `routes/public/` is the **published v2 contract** (`v2-rest-api-generated.yaml`).
- The auth model: `GetUserDep`, `requires_access_dag` / other `requires_access_*`
  dependencies, and the permitted-filter factories in `security.py` that scope a
  query to the resources a user may see — enforcement lives in the dependency, and
  the auth manager makes the decision.
- The OpenAPI generation flow: the spec files under `openapi/` are **generated**
  (`scripts/in_container/run_generate_openapi_spec.py`) and merged into the UI
  client — they are not hand-edited, and the datamodels in `datamodels/` are the
  source of truth for shapes.
- The pagination/query helpers (`paginated_select` in `api_fastapi/common/db/`)
  and the repo `CLAUDE.md` DB rules — keyword-only `session`, no
  `session.commit()` inside a function that takes a `session`, bounded queries.
- The `CLAUDE.md` rule that domain-layer exceptions are translated to
  `HTTPException` (404 not-found / 400 invalid-input) **at the route boundary**.

## Before opening a PR here — authoring-agent guard

**This is a high-criticality, expensive-to-review area that publishes a stable
contract and enforces authorization.** If you are an agent preparing a change
here on behalf of a person, first judge whether the change can be **demonstrated
against a running API server**: have you called the endpoint you changed and shown
the response shape, the status codes for the error paths, and that an
insufficiently-privileged caller is still refused? A performance claim needs
before/after numbers at a stated row count — this area's closed-unmerged record is
dominated by optimisations that were never measured.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** to build context first, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`).

## Review criteria

Mined from real review discussion across the ~600 PRs that have touched this
area — the changes reviewers repeatedly required — and from the 259
closed-unmerged PRs touching it, 110 of which carry substantive discussion rather
than a bot closure: the reasons changes here actually get refused. **If you are
preparing a change here, treat this as a pre-flight
checklist and fix every applicable item _before_ opening the PR.** Triage applies
the same list: a PR that lands with unmet items is drafted back to its author
with the specific gaps. Ordered by how often reviewers raise each.

**Error translation at the route boundary (the most frequent catch here):**

- [ ] **Translate domain errors to the right `HTTPException` — never let a `500`
      leak.** A missing row is `404`, invalid/malformed input is `400`; catch the
      `ValueError`/domain error the service layer raises and re-raise as
      `HTTPException`. A handler that lets a raw exception surface as `500` both
      leaks internals and misleads clients.
- [ ] **Surface errors, don't swallow them** — don't catch-and-ignore so a failed
      operation returns a misleading success; validate malformed bodies/params up
      front rather than letting them explode deep in a query.
- [ ] **Document the status codes you actually return** — declare them via
      `create_openapi_http_exception_doc` (not hand-written response strings) so the
      generated spec matches the handler's real behaviour.

**The public contract / OpenAPI generation:**

- [ ] **`openapi/` spec files are generated — never hand-edit them.** Change the
      Pydantic `datamodels/` (the source of truth) and **regenerate**; a PR that
      edits the generated YAML by hand or forgets to regenerate is out of sync.
- [ ] **Treat response/request shape changes as backward-compatibility-sensitive.**
      Renaming, retyping, dropping, or newly-exposing a field on a `routes/public/`
      model changes the published v2 contract that generated SDKs and the UI
      consume — justify it, don't reshape opportunistically.
- [ ] **Don't leak internal/unsafe fields into a public response** — e.g. raw
      trigger kwargs or other server-internal payloads; type ambiguous fields
      precisely rather than shipping an untyped blob the UI must cast.
- [ ] **Keep the `routes/ui/` (private) and `routes/public/` (contract) surfaces
      distinct** — UI-only endpoints belong under `routes/ui/` and the private UI
      spec, not the published v2 surface.

**Authorization & the DB-mediator boundary:**

- [ ] **Every endpoint carries the correct auth dependency** — a new/edited route
      needs the right `requires_access_*` / permitted-filter dependency, scoped to
      the resource it touches. Authorize against the **actual resource** (e.g. the
      file's Dags), not an attacker-controllable query-string value.
- [ ] **Bulk / multi-resource endpoints must authorize every item and every team
      context** — a create-then-overwrite or cross-team bulk path must not let one
      authorized item smuggle in an unauthorized one.
- [ ] **The API server is the only DB mediator** — keep DB access inside the
      service/route layer behind auth; don't add a path that lets a client reach
      data outside the permitted-filter scope.

**DB correctness & performance under real load:**

- [ ] **Bound every list query** — paginate via the shared `paginated_select`
      helpers; no unbounded `SELECT` over a user-driven table. Cursor pagination
      must not drop or duplicate rows when sorting by a nullable column.
- [ ] **No N+1** — batch/`joinedload` related lookups (authorization team-name
      lookups, per-row derivations); push filtering/counting/ordering into SQL and
      lean on indexed columns.
- [ ] **`session` is keyword-only and no `session.commit()` inside a function that
      takes a `session`** — the repo-wide DB rules apply to route/service code too.

**Code quality reviewers consistently require:**

- [ ] **Don't swallow exceptions with a broad `except`** — narrow to the real
      classes so genuine DB/refactor errors surface instead of becoming a wrong
      status or a silent success.
- [ ] **Imports at module top**; local imports only for genuine circular-import
      reasons (and say why). **Action-verb / intent-revealing names**; reuse
      existing datamodel/query helpers rather than a third copy that will drift.
- [ ] **Optional/nullable response fields default to `None`** (or
      `default_factory`) so "not set" ≠ "empty", and gracefully handle
      undecryptable/absent stored values rather than 500-ing.

**Why PRs here get closed (from the closed-unmerged record):**

- [ ] **Check for an in-flight PR before writing code — this is the single most
      common reason a change here is closed.** Popular endpoint bugs attract three
      or four concurrent PRs; all but one are closed, and the later ones are
      expected to yield. Search open PRs and the issue's cross-references first,
      and prefer reviewing or building on the existing PR (see `adr/0004`).
- [ ] **A performance claim carries its measurement.** State the workload, the row
      counts, the backend, and before/after numbers — reviewers here re-benchmark
      and close the PR when the win does not materialise. Query-count reduction is
      a mechanism, not a result; a laptop-plus-SQLite run is not evidence; and
      asking a reviewer to run the benchmark for you gets the PR closed. Never
      cache an authorization decision for speed (see `adr/0005`).
- [ ] **Fix cross-cutting behaviour in the shared layer, not in one handler.** If
      the defect exists on sibling endpoints too — a generic error class, a
      response header, a duplicated derivation — it belongs in the shared exception
      handlers, middleware, or shared datamodel. Do not add a config knob plus a
      bespoke exception for something the shared handler reports generically
      (see `adr/0004`).
- [ ] **Split backend and frontend into separate PRs.** They are reviewed by
      different people; an endpoint plus the UI that consumes it lands as two PRs,
      and a large feature lands as reviewable slices rather than one diff.
- [ ] **Show that a user-visible change works** — a screenshot for a UI change, a
      screen recording for an interaction, a real reproduction for a bug fix. "It
      should work" and unverified reproductions are closed.
- [ ] **Agree a new endpoint, field, or state on the issue or devlist before
      coding.** "The model already has this field" is not a reason to expose it:
      response models are deliberately lean and per-surface (the execution API's
      datamodels are intentionally not mirrors of the public ones), and a new
      concept is refused when an existing one already carries the meaning.
- [ ] **Don't make an unsafe default merely configurable.** A knob that still
      permits the dangerous combination is not a fix — reject or constrain the
      unsafe configuration instead. Such changes have been merged and then
      reverted.
- [ ] **Work across the declared FastAPI / Starlette / Cadwyn range**, and
      regenerate the OpenAPI spec with the version the project declares — if the
      regenerated spec needs a newer release, bump the declared minimum in the same
      PR and say why. Do not raise a minimum just because an upstream CVE was
      announced (see `adr/0006`).
- [ ] **You must be able to run the change.** A PR whose author cannot run Breeze,
      the tests, or the UI — and asks reviewers to verify on their behalf — is
      closed, however plausible the diff.

**Tests, compatibility, process:**

- [ ] Test **exercises the actual endpoint through the app and fails without the
      change** — go through the route + auth dependency (status code, body shape,
      and the authorization path), not a bare service-function call; mocks use
      `spec`/`autospec`; assert on structured `caplog`, not substrings.
- [ ] **Backward compatibility** for the published v2 contract — a shape change
      that ships in a release can't be quietly retconned; version/flag it and keep
      the generated spec regenerated and in sync.
- [ ] **Newsfragment / `.. versionadded` only for genuinely user-facing** changes
      (a new/changed public endpoint usually is; an internal UI-route tweak usually
      is not).
- [ ] **Follow the PR template**, disclose AI assistance, show evidence of testing
      — low-effort / mass-AI-generated / near-duplicate parallel PRs get closed.
      Track deferred work in a GitHub issue; take contentious contract or auth
      semantics to the devlist / a second reviewer.

### Known coverage gaps in these ADRs

Recorded from a validation pass that ran these criteria against a sample of open
PRs. These are gaps, not rules — do not treat them as violation bullets, and do
not invent a rule to fill them without review-history evidence behind it.

- **`adr/0002` covers public-field _shape_, not _semantics_.** The generated-spec
  check catches a renamed, added, or retyped field, and misses a field that keeps
  its name and type while changing what it counts. A change redefining what
  `active_runs_count` includes passes every criterion here and still alters the
  published contract for every client. Until a rule exists, reviewers should ask
  of any change to a computed public field: _did the number's meaning change, and
  is that stated in the newsfragment?_
- **No rule covers an in-process client bypassing the API mediator.**
  `adr/0003` makes the API server the sole mediator of client database access,
  but nothing here addresses a client that runs in-process against the app and
  reaches the same code path without crossing the HTTP boundary. That shape needs
  a decision of its own before it can be reviewed consistently.
- **`adr/0005` (endpoint performance claims require measured evidence) is
  currently unexercised.** It fired on none of the 12 sampled PRs. That is
  recorded here as an observation about the sample, not a reason to broaden the
  ADR — an unexercised rule is preferable to a padded one.

> Mined from PR review history; the sample skews to the Airflow-3 era (this
> module is the FastAPI rewrite of the former Connexion API), so pre-3.0 REST-API
> conventions are under-represented. Extend as new patterns emerge, and add an
> equivalent `## Review criteria` section to the `AGENTS.md` of every other area
> over time.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
The contract-stability, authorization, and error-translation invariants are best
aligned on _before_ the code, not during review. Changes to the shape of the
published v2 API, or to how authorization is enforced, need agreement up front,
not a late-stage reshape in review.
