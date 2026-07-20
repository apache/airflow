---
triage_review_imbalance:
  area: auth-manager
  criticality: critical           # authorization & authentication surface — a defect here is a security defect
  review_difficulty: expert
  structural_risk_paths:          # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "managers/base_auth_manager.py"
    - "tokens.py"
    - "managers/simple/"
    - "middlewares/"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["vincbeck"]           # internal signal only — never @-mentioned in drafted PR text
  adr_ref: "adr/"                 # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Auth manager (authorization & authentication) — Agent Instructions

This directory is the **pluggable auth-manager surface**: `BaseAuthManager`
(`managers/base_auth_manager.py`) is the interface every auth manager implements,
the reference `SimpleAuthManager` (`managers/simple/`) ships in core, and the FAB
and Keycloak managers live in provider distributions and subclass the same base.
The manager is the **single decision point** for every authorization question the
API server asks (`is_authorized_dag`, `is_authorized_connection`, … and the
`filter_authorized_*` / `get_authorized_*` helpers), and it owns the
authentication path — JWT signing and validation (`tokens.py`), token revocation,
and the login/refresh flow (`middlewares/`, `managers/simple/routes/`). A defect
here is a security defect: it can hand a caller access they should not have, leak
across the multi-team boundary, or accept a token that should have been rejected —
across **every** deployment and every downstream auth-manager provider.

## Why changes here are expensive to review

- A wrong answer is a **silent authorization hole**, not a crash. Whether
  `is_authorized_*` returns the right boolean for the right resource — and scopes
  it to the right team in multi-team mode — is a correctness property a diff rarely
  makes obvious, and a test with one fixture user rarely exercises.
- `BaseAuthManager` is a **provider-facing contract**. FAB and Keycloak managers,
  released independently of core, subclass it and override its hooks; a renamed or
  re-signatured method looks local to core but breaks those managers in the
  mixed-core/provider-version combinations Airflow supports.
- **Token handling is cryptographic and adversarial.** Signing algorithm
  selection, `kid` handling, audience/issuer validation, revocation, and cookie
  scoping/flags each have failure modes where the token still "works" in a happy-path
  test but is forgeable, over-scoped, or accepted after logout.
- **The token surface changes as a whole, on a schedule this PR does not control.**
  Claim validation, algorithm and `kid` handling, lifetime, refresh, revocation,
  and cookie flags interact with each other, with the execution API's task tokens,
  and with every auth-manager provider. That is why even security _improvements_
  here are closed as standalone PRs and folded into the coordinated design — a
  reviewer's job includes recognising when a diff is a piece of a system nobody has
  yet reasoned about whole.
- The **fail-closed default matters more than the feature.** A change that widens
  what an unauthenticated or anonymous request can reach, or that turns a deny into
  an allow on an error path, is a security-model change — not the local convenience
  it looks like.

## Knowledge a reviewer (and a substantial contributor) needs

- The decision/enforcement split: the auth manager **decides** (`is_authorized_*`),
  and the API server **enforces** by wiring the `requires_access_*` /
  permitted-filter dependencies from `core_api/security.py` onto routes — route
  bodies do not re-implement the check (see `adr/0001`).
- `BaseAuthManager`'s public surface: the `is_authorized_*` methods, the
  `batch_is_authorized_*` / `filter_authorized_*` / `get_authorized_*` helpers, the
  `serialize_user` / `deserialize_user` / `get_user_from_token` contract, and the
  extension points (`get_fastapi_app`, `get_fastapi_middlewares`, `get_cli_commands`,
  `get_db_manager`) that providers rely on (see `adr/0002`).
- JWT handling in `tokens.py`: `JWTGenerator` / `JWTValidator`, symmetric vs.
  asymmetric keys and algorithm guessing, required claims (`exp`/`iat`/`nbf`),
  audience/issuer, `kid` header handling, JWKS fetch, and revocation via
  `RevokedToken`.
- The multi-team model: how `team_name` threads through `get_authorized_dag_ids`
  and the other `get_authorized_*` helpers, and why the default managers must fail
  closed (`is_authorized_team` raises `NotImplementedError`) rather than silently
  allow when a manager is not multi-team-aware.
- The repo `CLAUDE.md` security model — what is _not_ a vulnerability, the
  fail-closed / secure-by-default expectation, and that widening access or changing
  the security model is devlist/security-team gated (see `adr/0003`).

## Before opening a PR here — authoring-agent guard

**This is a critical, expensive-to-review area where every change is a security
change.** If you are an agent preparing a change here on behalf of a person, first
judge whether the change can be **demonstrated to deny what it should deny**: have
you run an API server with your change and shown both halves — the authorized
caller still gets through, _and_ the unauthorized one, the expired token, the
wrong-algorithm token are still rejected? Auth bugs are almost never visible as a
failing test; they are visible as a request that succeeds when it should not.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **simpler, well-scoped issue in this area** to build context first, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any code.

A large change here that nobody can verify wastes scarce maintainer review time
and will be closed or drafted back (see `## Review criteria`). Security-model
changes are not opportunistic PRs — take them to the devlist / security process
before writing code.

## Review criteria

Mined from real review discussion across the ~120 merged PRs (excluding dependency
bumps) that have touched this area — the changes reviewers repeatedly required —
and from the 191 closed-unmerged PRs touching it, 35 of which carry substantive
discussion rather than a bot closure: the reasons changes here actually get
refused. **If you are preparing a change here, treat
this as a pre-flight checklist and fix every applicable item _before_ opening the
PR.** Triage applies the same list: a PR that lands with unmet items is drafted back
to its author with the specific gaps. Ordered by how often reviewers raise each.

**Fail closed & the security model (the defining concern here):**

- [ ] **Deny by default; never turn a deny into an allow on an error path.** A
      malformed/expired/unknown-`kid` token, a deserialization failure, or an
      unhandled case must reject (raise `InvalidTokenError` / return `False`), not
      fall through to access. Anonymous / public access is a deliberate,
      configured widening, never an accidental default.
- [ ] **Widening access or changing the security model is devlist/security-team
      gated** — not an opportunistic PR. Honouring a public-role config, adding an
      anonymous path, or relaxing a check needs agreement up front (per the repo
      `CLAUDE.md` security model and "what is NOT a vulnerability").
- [ ] **Use constant-time / cryptographically-secure primitives** on the auth path
      — compare secrets with `hmac.compare_digest`, generate secrets/passwords with
      a secure RNG, and set cookie flags (`Secure` on HTTPS, `SameSite`) correctly.

**Authorization correctness (decision vs. enforcement):**

- [ ] **The auth manager is the single decision point** — authorization logic lives
      in `is_authorized_*` (and the `filter_/get_authorized_*` helpers), not
      re-implemented in a route body or a UI check. Routes enforce via the
      `requires_access_*` dependency (see `adr/0001`).
- [ ] **Authorize against the actual resource, scoped to the right team.** Check the
      resource the operation acts on (and its `team_name` in multi-team mode), not a
      client-controllable value; bulk / multi-resource paths must authorize **every**
      item so an authorized one can't smuggle in an unauthorized one.
- [ ] **List/filter endpoints return only what the user may see** — go through
      `get_authorized_*` / `filter_authorized_*`, and keep the per-team batching so
      the filter doesn't degrade to per-row `is_authorized_*` on large tables.

**The `BaseAuthManager` provider contract (see `adr/0002`):**

- [ ] **Don't break the interface providers implement.** FAB / Keycloak managers
      subclass `BaseAuthManager` and are released independently; renaming, removing,
      or re-signaturing a public method / hook (`is_authorized_*`,
      `serialize_user`, `get_fastapi_app`, `get_cli_commands`, `get_db_manager`, …)
      is a cross-version break. Make changes additive; deprecate, don't delete.
- [ ] **New optional behaviour has a default that preserves old semantics** — a new
      hook or keyword must not force every existing provider manager to override it
      (mirror the `is_authorized_team` "raise NotImplementedError unless multi-team"
      pattern rather than a default that silently allows).

**Token & authentication handling (`tokens.py`, `middlewares/`):**

- [ ] **Validate every security-relevant claim** — audience, issuer, `exp`/`iat`/`nbf`,
      and a `kid` that must match a known key (a non-matching `kid` rejects, it does
      not fall back). Keep signer and validator reading the **same** config keys
      (audience/algorithm) so a token that is signed also validates.
- [ ] **Revocation and logout actually invalidate** — a revoked/`jti`-listed token
      is rejected, and the refresh/logout flow can't leave a stale `_token` cookie
      that re-authenticates; scope cookies to the correct base/root path.

**Code quality reviewers consistently require:**

- [ ] **Don't swallow exceptions with a broad `except`** on the auth path — narrow to
      the real classes (`InvalidTokenError`, `ValueError`, `KeyError`) so a failure
      surfaces as a rejection, not a silent allow or a misleading 500.
- [ ] **Define/raise specific exceptions**, not a bare `AirflowException`; translate
      domain errors to the right `HTTPException` at the route boundary.
- [ ] **Imports at module top**; local imports only for genuine circular-import /
      config-timing reasons (and say why). **Action-verb / intent-revealing names**;
      reuse the existing `filter_/get_authorized_*` helpers rather than a third copy
      of a derivation that will drift.

**Why PRs here get closed (from the closed-unmerged record):**

- [ ] **Hardening is coordinated too, not just widening.** A standalone PR that
      tightens JWT or cookie semantics — extra cookie flags, stricter claim checks,
      extra token access checks — is closed and folded into the security team's
      coordinated design, because the token surface is a system, not a set of
      independent switches. "It is strictly more secure" is not an exemption
      (see `adr/0004`). Report suspected vulnerabilities through the ASF security
      process, not a public PR.
- [ ] **An upstream CVE is not by itself a reason to change anything here.** Assess
      whether Airflow's usage is affected before raising a declared minimum or
      changing a default; keep version ranges relaxed, and cover a vulnerable
      transitive range with the lockfile override mechanism rather than by
      narrowing declarations.
- [ ] **Don't relax a production default for local-development convenience** — a
      setting that only helps one contributor's machine but weakens the deployed
      API server is refused even when it looks unrelated to auth.
- [ ] **`managers/simple/ui/` is a second, independent frontend bundle** with its
      own lockfile and build. Dependency upgrades there land on `main` first, are
      validated by actually exercising the login UI (not just green CI), and are
      never opened as a bulk bump against a maintenance branch — those have been
      reverted (see `adr/0005`). Keep its frontend changes out of Python
      auth-manager PRs.
- [ ] **Check for an in-flight PR first**, and keep the branch rebased. Duplicate
      fixes for the same auth defect, and branches left far behind `main`, are
      closed rather than reconciled.
- [ ] **Provider-side permission mapping belongs in the provider.** A FAB or
      Keycloak action/method mapping gap is fixed in that manager's mapping, not by
      changing a core enum or route to accommodate it.

**Tests, compatibility, process:**

- [ ] Test **exercises the real authorization/authentication path and fails without
      the change** — assert an unauthorized user is actually denied (not only that an
      authorized one is allowed), and go through real token generate/validate rather
      than a stubbed claim dict; mocks use `spec`/`autospec`; assert on structured
      `caplog`, not substrings.
- [ ] **Backward compatibility** for the provider-facing interface and for persisted
      tokens/cookies — version-gate, keep a deprecated shim, and don't invalidate
      tokens issued by a released version without a migration path.
- [ ] **Newsfragment / `.. versionadded` only for genuinely user-facing** changes
      (a new config key or auth behaviour usually is; an internal refactor is not).
- [ ] **Follow the PR template**, disclose AI assistance, show evidence of testing —
      low-effort / mass-AI-generated / near-duplicate parallel PRs get closed. Take
      contentious auth or security-model semantics to the devlist / security team, not
      a bare PR.

> Mined from PR review history; the sample skews to the Airflow-3 era (this surface
> is the FastAPI-era auth-manager rewrite, with JWT/JWKS and multi-team added
> recently), so pre-3.0 auth conventions are under-represented. Extend as new
> patterns emerge, and add an equivalent `## Review criteria` section to the
> `AGENTS.md` of every other area over time.

## Expectation for large changes

Discuss the approach first — in an issue, on the dev list, or with the security team
— before a large PR. The authorization-decision boundary, the provider-facing
`BaseAuthManager` contract, and the token/security-model invariants are best aligned
on _before_ the code, not during review. A change to what an unauthenticated caller
can reach, or to how authorization is decided, needs agreement up front, not a
late-stage reshape in review.
