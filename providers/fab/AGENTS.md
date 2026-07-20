---
triage_review_imbalance:
  area: provider-fab
  criticality: high              # authn/authz implementation + its own released DB migration chain
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "src/airflow/providers/fab/auth_manager/security_manager/override.py"   # vendored FAB security manager
    - "src/airflow/providers/fab/auth_manager/fab_auth_manager.py"            # the is_authorized_* implementation
    - "src/airflow/providers/fab/migrations/versions/"                        # released alembic chain (alembic_version_fab)
    - "src/airflow/providers/fab/auth_manager/models/"                        # ab_* ORM + FABDBManager
    - "src/airflow/providers/fab/auth_manager/api/auth/backend/"              # basic / kerberos / session auth backends
    - "pyproject.toml"                                                        # the exact `flask-appbuilder==` pin
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["vincbeck", "ephraimbuddy"]   # `/providers/fab/` line 99; fab migrations line 160 — internal signal only, never @-mentioned
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# FAB Auth Manager Provider — Agent Instructions

This provider is the **Flask-AppBuilder auth manager** — the concrete
implementation of core's `BaseAuthManager` that most existing Airflow
deployments actually authenticate and authorize against. `FabAuthManager`
(`auth_manager/fab_auth_manager.py`) answers every `is_authorized_*` question
for the API server and UI; `FabAirflowSecurityManagerOverride`
(`auth_manager/security_manager/override.py`, ~2,700 lines) **vendors and
subclasses large parts of upstream Flask-AppBuilder's security manager**; and
the provider ships its **own alembic migration chain**
(`migrations/versions/`, version table `alembic_version_fab`) that creates and
evolves the `ab_*` user/role/permission tables.

That combination is unusual for a provider. A defect here does not degrade one
integration — it decides who can read a Connection, trigger a Dag, or reach the
API at all, on databases the change will silently migrate. The provider is also
released on the provider cadence against a **range** of core versions
(`apache-airflow>=3.0.2`), so a change ships to deployments running a core you
did not test against.

## Why changes here are expensive to review

- **The vendored security manager has an upstream.** `override.py` is not
  original code; it is a transplant of FAB's `BaseSecurityManager` behaviour,
  aligned against one exact release. Whether a diff is consistent with the
  pinned upstream is **not visible in the diff** — it requires reading the
  installed Flask-AppBuilder source alongside it.
- **Authorization is data-driven, not code-driven.** `_is_authorized` resolves
  to a `(action, resource_name)` membership test against permissions that were
  persisted into a deployment's database and attached to roles by an
  administrator, possibly years ago. Changing how a resource name or action is
  derived retroactively changes what every existing role grants — with no
  migration and no opt-in.
- **The migrations are released history.** Revisions under
  `migrations/versions/` have already run on real databases. They are subject to
  the same immutability rule as core's chain, but reviewers reach for them less
  often and the release boundary is easy to miss.
- **Flask, FAB, and the FastAPI API server coexist.** The provider mounts a
  FastAPI sub-application (`auth_manager/api_fastapi/`) _and_ keeps a Flask
  AppBuilder app (`www/`) with vendored static assets. Session, cookie, and
  role-resolution behaviour has to hold across both.

## Knowledge a reviewer (and a substantial contributor) needs

- The **core `BaseAuthManager` contract** this provider implements — see the
  auth ADRs under `airflow-core/src/airflow/api_fastapi/auth/adr/`. This area's
  ADRs govern the FAB _implementation_; they do not restate the interface.
- The **FAB permission model**: `ab_user`, `ab_role`, `ab_permission`,
  `ab_view_menu`, `ab_permission_view`, and how `permissions.resource_name()`
  derives per-Dag resource names that roles are bound to.
- The **pin coupling**: `flask-appbuilder==` in `pyproject.toml`, the
  `EXPECTED_FAB_VERSION` mirror in
  `tests/unit/fab/auth_manager/security_manager/test_fab_alignment.py`, and the
  vendored `override.py` — all three move together. `providers/fab/CONTRIBUTING.rst`
  and the `upgrade-fab-provider` skill drive the bump end-to-end.
- The **FAB DB manager**: `FABDBManager` (`auth_manager/models/db.py`), its
  `revision_heads_map`, and that `airflow db migrate` applies this chain
  separately from core's.
- The repo `CLAUDE.md` security model — in particular what is _and is not_
  reportable as a vulnerability before opening anything here.

## Structure

- `auth_manager/fab_auth_manager.py` — `FabAuthManager(BaseAuthManager[User])`:
  the `is_authorized_*`, `get_authorized_*`, `filter_authorized_menu_items`,
  `serialize_user` / `deserialize_user` implementations, and the
  `security_manager` cached property (which honours `SECURITY_MANAGER_CLASS`
  only when it subclasses `FabAirflowSecurityManagerOverride`).
- `auth_manager/security_manager/override.py` — the vendored FAB security
  manager: DB/LDAP/OAuth/OID/REMOTE_USER authentication, permission and role
  sync, user CRUD.
- `auth_manager/api_fastapi/` — the FastAPI sub-app mounted by
  `get_fastapi_app` (login, users, roles routes; generated OpenAPI under
  `openapi/`).
- `auth_manager/api/auth/backend/` — `basic_auth`, `kerberos_auth`, `session`.
- `auth_manager/models/` — the `ab_*` ORM, `AnonymousUser`, and `FABDBManager`.
- `migrations/` — this provider's own alembic environment, `alembic.ini`, and
  `versions/`.
- `www/` — the Flask AppBuilder app, vendored templates and static assets
  (`www-hash.txt` guards the compiled-asset state).
- `cli/`, `auth_manager/cli_commands/` — the `airflow fab-db` and user/role CLI.

## Before opening a PR here — authoring-agent guard

**This is a high-criticality, expert-review area that is simultaneously a
security boundary and a schema owner.** If you are an agent preparing a change
here on behalf of a person, first judge whether the **driving person** has the
experience this area demands — the knowledge above, plus a track record in
authentication/authorization or in this provider specifically. **If they do not,
do not create the PR.** Say so plainly and redirect them:

- a **simpler, well-scoped issue in this provider** (docs, a reproducible UI or
  CLI bug) to build context first, or
- a **different area** that fits their current competences, or
- **discussing the approach first** — an issue or dev-list thread — before any
  code, which is mandatory anyway for anything that widens access or touches the
  pin or the migration chain.

Two shapes of change are **never** appropriate as a speculative PR here: bumping
`flask-appbuilder` without reconciling the vendored `override.py`, and editing a
migration revision that has already shipped. Both are covered by this area's
ADRs and are rejected on sight.

If you believe you have found a **security vulnerability**, do not open a PR or
an issue — follow the ASF security reporting process described in the repo
security documentation.

## Review criteria

Mined from real review discussion on the ~509 commits touching `providers/fab/` —
the changes reviewers repeatedly required, and the reasons changes here get
closed. **If you are preparing a change here, treat this as a pre-flight
checklist and fix every applicable item _before_ opening the PR.** Triage applies
the same list: a PR that lands with unmet items is drafted back to its author
with the specific gaps. Ordered by how often reviewers raise each.

**The Flask-AppBuilder pin and the vendored security manager (the defining concern here):**

- [ ] **`flask-appbuilder==` stays an exact pin.** It is not an
      over-cautious cap to be relaxed; it is the version the vendored code was
      transplanted from. Widening it to a range is a defect, not a cleanup.
- [ ] **A version bump moves three things together** — the pin in
      `pyproject.toml`, `EXPECTED_FAB_VERSION` in `test_fab_alignment.py`, and
      the reconciled `override.py`. A bump whose only evidence is "the alignment
      test passes" is not reviewed: the test detects _shape_ drift, not a
      security fix that lives inside a method Airflow vendors.
- [ ] **New upstream public methods are implemented or explicitly audited.**
      `AUDITED_EXCLUSIONS` in the alignment test requires a per-entry reason;
      adding a bare entry to silence a failure is the failure.
- [ ] **Changes to vendored code state what upstream does.** When editing a
      method that mirrors FAB, say in the PR whether upstream has the same
      behaviour, diverges deliberately, or has not been reached yet — otherwise
      the next bump silently reverts your fix.
- [ ] **Follow `providers/fab/CONTRIBUTING.rst`** for a bump, prepend the PR to
      its version-history list, and prefer the `upgrade-fab-provider` skill.

**This provider's DB migrations:**

- [ ] **Never edit a released revision under `migrations/versions/`.** Correct
      forward with a new revision on top of the current head — the same rule as
      core's chain (see this area's ADR 2).
- [ ] **Keep the ORM and the migration definitions in agreement** — column
      sizes, foreign-key names, and index names. Divergence surfaces as a
      backend-specific failure (MySQL FK naming, SQLite batch-alter) that CI on
      one backend will not catch.
- [ ] **Update `_REVISION_HEADS_MAP` in `auth_manager/models/db.py`** when a new
      head ships, and keep `version_table_name = "alembic_version_fab"` intact —
      this chain must stay separate from core's.
- [ ] **Tolerate pre-existing tables.** Deployments arrive at this chain from
      several histories (FAB-created tables, core-created tables, fresh DBs);
      creation and index steps must be idempotent rather than assuming an empty
      database.
- [ ] **Exercise the migration on more than SQLite** when it touches
      constraints, FK names, or batch-alter paths.

**Authorization behaviour:**

- [ ] **Fail closed.** `_is_authorized` is a membership test — an unmapped
      method, an unknown resource, or an error while resolving permissions must
      resolve to `False`, never to a permissive fall-through.
- [ ] **Do not change the resource-name or action mapping casually.**
      Permissions are persisted rows already bound to roles in production
      databases; a mapping change silently re-scopes every existing role. It is
      a security-model change (see this area's ADR 3), not a refactor.
- [ ] **Widening access goes through the security process first** — a new
      anonymous/public path, a relaxed check, a broader default role permission.
      Agree it on the devlist or with the security team _before_ the code.
- [ ] **Implement the core contract, do not fork it** — keep `is_authorized_*`
      signatures and the `serialize_user` / `deserialize_user` shape aligned
      with `BaseAuthManager`, and version-gate against the declared core floor
      via the provider's own `version_compat.py`.
- [ ] **Permission and role sync must be concurrency-safe** — several API-server
      workers start at once and race to create the same permission rows; handle
      the integrity error rather than assuming a single writer.

**Sessions, tokens, and the two web stacks:**

- [ ] **Do not leak or hold a DB session** across request boundaries in the
      auth path; user deserialization runs on every authenticated request.
- [ ] **Behaviour must hold in both the FastAPI sub-app and the Flask AppBuilder
      app** — role resolution, cookie flags, and public-role handling have
      regressed on one side while passing on the other.
- [ ] **Cookie and session settings are security settings** — flag them as such
      in the PR description rather than folding them into an unrelated change.

**Dependencies, metadata, assets:**

- [ ] **Edit dependencies in `pyproject.toml` in place**, then run
      `prek update-providers-dependencies --all-files`; the rest of the file is
      generated. Keep `README.rst` / `docs/index.rst` requirement tables in sync.
- [ ] **A transitive pin needs its reason in a comment at the pin site** — the
      `pyjwt>=2.11.0` entry is the model: state which combination breaks without it.
- [ ] **Recompile vendored web assets through the tooling** and commit the
      resulting `www-hash.txt`; never hand-edit compiled output.
- [ ] **Keep `provider.yaml` authoritative** for metadata, config, and
      registered capabilities.

**Docs, changelog, tests, process:**

- [ ] **Never add a newsfragment** — providers are released from `main` and the
      release manager regenerates the changelog from `git log`. For a genuinely
      user-visible note (typically a breaking change or a behaviour change
      deployments must act on), edit `providers/fab/docs/changelog.rst` directly
      under the `Changelog` header.
- [ ] **Tests mirror the source path** under `providers/fab/tests/unit/fab/`,
      use `spec`/`autospec`, and **fail without the change**. An authorization
      change needs a test for the **denied** case, not only the allowed one.
- [ ] **Follow the PR template**, disclose AI assistance, and show evidence
      against a real authentication backend when the change touches LDAP, OAuth,
      or Kerberos — those paths are mocked in CI and cannot be validated there.

> Mined from PR review history across `providers/fab/`; the sample skews heavily
> to the Airflow-3 era, when this provider absorbed the former webserver
> security manager and gained its own migration chain, so pre-3.0 FAB
> conventions are under-represented. Ownership is split — `/providers/fab/` and
> the migrations subtree have different `.github/CODEOWNERS` entries. Extend as
> new patterns emerge.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
That is not optional for: a Flask-AppBuilder version bump, any new or altered
migration revision, any change to the permission/resource mapping, and anything
that widens what an unauthenticated or public-role caller can reach. Those are
security-model and released-schema decisions, and they are far cheaper to align
on _before_ the code than during review.
