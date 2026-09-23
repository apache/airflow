---
name: upgrade-fab-provider
description: >
  Upgrade the pinned Flask-AppBuilder (FAB) dependency in the Apache Airflow
  FAB provider (`providers/fab/`). Bumps the exact `flask-appbuilder==` pin and
  its mirror constant, regenerates `uv.lock`, drives the `test_fab_alignment.py`
  drift tripwire, reviews the vendored security-manager `override.py` against the
  new upstream FAB, and conditionally re-vendors static assets / DB migrations.
  Use when asked to "upgrade FAB", "bump flask-appbuilder", or move the FAB
  provider to a newer Flask-AppBuilder release.
license: Apache-2.0
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# upgrade-fab-provider

Airflow's FAB provider is **tightly coupled** to a specific Flask-AppBuilder
release because it vendors-in and subclasses large parts of FAB's security
manager. A version bump is therefore never "just change the pin" — it must be
reconciled against the vendored code, and that reconciliation is enforced by a
pytest **alignment test**, not a prek hook.

The canonical reference for a real bump is PR **#66841** ("Bump flask-appbuilder
to 5.2.1 and mirror new auth event hooks") — commit `c72b6613fd`. Read its diff
first when in doubt: `git show c72b6613fd`.

## The coupling — why this is not a one-line change

- `providers/fab/pyproject.toml:75-80` explains it: Airflow vendored FAB's
  security-manager code into
  `providers/fab/src/airflow/providers/fab/auth_manager/security_manager/override.py`
  (~2700 lines) as `FabAirflowSecurityManagerOverride`. Every bump must review
  that class against upstream FAB for new / changed / removed methods.
- `test_fab_alignment.py` mechanically detects drift between the **installed**
  FAB package and the vendored override, and **fails CI** until the developer
  reconciles it.

## Inputs

- **Target version** — the FAB version to move to. If not given, use the latest
  release on PyPI (`https://pypi.org/pypi/flask-appbuilder/json` → `info.version`).
  Confirm the target with the user if it is a **major or minor** bump (higher
  reconciliation risk); a **patch** bump can proceed.

## The files a bump touches

**Always:**

1. `providers/fab/pyproject.toml` — line ~80, the `flask-appbuilder==X.Y.Z` pin
   (the **only** real dependency pin in the repo).
2. `providers/fab/tests/unit/fab/auth_manager/security_manager/test_fab_alignment.py`
   — `EXPECTED_FAB_VERSION = "X.Y.Z"` (line ~43). Must move in lockstep with the pin.
3. `providers/fab/docs/index.rst` — the dependency table row
   ``` ``flask-appbuilder``   ``==X.Y.Z`` ``` (line ~114).
4. `providers/fab/README.rst` — the Requirements table row (line ~60). **Do
   not hand-edit** — it is auto-generated. Regenerate it from the bumped
   `pyproject.toml` with the `sync-provider-readme` prek hook (Step 8); the hook
   re-renders the table whenever `pyproject.toml` changes. (Pre-existing bumps
   that predate this hook left it to release-time regeneration; today the hook is
   per-commit, so CI flags the drift — run it.)
5. `uv.lock` — regenerated (see Step 4 for the pinned-uv caveat).

**Conditionally:**

6. `providers/fab/src/airflow/providers/fab/auth_manager/security_manager/override.py`
   — transplant any relevant upstream FAB changes (new auth hooks, changed
   signatures, ported fixes). The reference bump added +37 lines here. **Note:**
   a green alignment test does not prove the transplant is unnecessary — a
   security fix may live inside a method Airflow vendors (see the 5.2.2 worked
   example below).
7. `providers/fab/src/airflow/providers/fab/www/**` + `providers/fab/www-hash.txt`
   — only if the new FAB ships changed static assets / templates that are
   re-vendored (see Step 6).
8. `providers/fab/src/airflow/providers/fab/migrations/versions/**` — only if the
   new FAB version ships DB migrations (see Step 7).

**Never:** no newsfragment, and do **not** hand-edit `providers/fab/docs/changelog.rst`
— providers are released from `main` and the release manager regenerates the
changelog from `git log` (per `providers/AGENTS.md`). The commit subject is the
changelog entry.

## Procedure

### Step 1 — Determine the target version and current state

- Read the current pin: `grep flask-appbuilder== providers/fab/pyproject.toml`.
- Resolve the target (PyPI latest, or the user's requested version).
- Classify the jump: patch / minor / major. For minor/major, skim the FAB
  release notes (`https://github.com/dpgaspar/Flask-AppBuilder/releases`) for
  security-manager / model / template changes before editing.

### Step 2 — Bump the three source pins

Edit in lockstep:

- `providers/fab/pyproject.toml` — the `flask-appbuilder==X.Y.Z` line (keep the
  trailing `# Whenever updating the version, run test_fab_alignment.py to verify.`
  comment).
- `test_fab_alignment.py` — `EXPECTED_FAB_VERSION = "X.Y.Z"`. It is the tripwire;
  editing it now is fine — you will keep re-running the test until it and the
  other three tests pass.
- `providers/fab/docs/index.rst` — the dependency-table `==X.Y.Z` row.

### Step 3 — Install the new FAB into the provider venv

`uv --project providers/fab sync` installs the new pin. Confirm the installed
version:

```bash
uv run --project providers/fab python -c \
  "import importlib.metadata as m; print(m.version('flask-appbuilder'))"
```

### Step 4 — Regenerate uv.lock (pinned uv!)

The lock **must** be regenerated with the repo's pinned uv version, or it drifts
hundreds of unrelated marker lines:

```bash
AIRFLOW_UV_VERSION=$(grep -oE 'AIRFLOW_UV_VERSION=[0-9.]+' Dockerfile.ci | head -1 | cut -d= -f2)
uvx --from uv==$AIRFLOW_UV_VERSION uv lock
```

If a conflict is irrecoverable, delete `uv.lock` and re-run `uv lock` with the
pinned version. Confirm the diff is limited to the FAB bump, not a wholesale
marker rewrite.

### Step 5 — Run the alignment test and reconcile override.py

```bash
uv run --project providers/fab pytest \
  providers/fab/tests/unit/fab/auth_manager/security_manager/test_fab_alignment.py -xvs
```

Under the host sandbox the test needs a writable `AIRFLOW_HOME` and the
rerun-failures socket disabled — if it errors on a socket `bind` or on
`~/airflow`, run it as:

```bash
AIRFLOW_HOME="$TMPDIR/fab_home" uv run --project providers/fab pytest \
  providers/fab/tests/.../test_fab_alignment.py -q -p no:rerunfailures
```

The four tests and how to fix each:

1. **`test_fab_version_matches_expected`** — trips on the version mismatch. It
   passes once `EXPECTED_FAB_VERSION` == installed version, but only after you
   have done the review below.
2. **`test_no_unaudited_fab_methods`** — a new FAB public method exists that is
   neither implemented in `override.py` nor listed in `AUDITED_EXCLUSIONS`.
   Fix: either implement/override it in `override.py`, or add it to
   `AUDITED_EXCLUSIONS` **with a justification comment**.
3. **`test_no_stale_exclusions`** — `AUDITED_EXCLUSIONS` lists a method the new
   FAB no longer has. Fix: remove that entry.
4. **`test_shared_method_signatures_compatible`** — FAB changed a method
   signature (new required param). Fix: update the `override.py` signature, or
   add to `KNOWN_SIGNATURE_DEVIATIONS` if the divergence is intentional.

**The manual review that the test cannot fully automate:** diff the vendored
`override.py` against the new FAB's `flask_appbuilder/security/sqla/manager.py`
and `BaseSecurityManager` and **transplant behavioural changes** (bug fixes, new
auth event hooks), not just signatures — the test only checks method *presence*
and *required params*. Locate the installed source:

```bash
uv run --project providers/fab python -c \
  "import flask_appbuilder.security.sqla.manager as m; print(m.__file__)"
```

### Step 6 — Re-vendor static assets / templates (only if changed)

If the new FAB changed frontend assets that Airflow vendors under
`providers/fab/src/airflow/providers/fab/www/` (templates in
`templates/appbuilder/`, static JS/CSS), re-vendor them, then regenerate the
fingerprint:

```bash
prek run compile-fab-assets --all-files
```

This runs `scripts/ci/prek/compile_provider_assets.py fab` (pnpm build over
`www/`) and rewrites `providers/fab/www-hash.txt`. A patch bump usually does
**not** touch assets — skip this step unless FAB's templates/static changed.
Commit the regenerated `www-hash.txt` if it changed.

### Step 7 — DB migrations (only if FAB ships them)

If the new FAB adds/changes security-model tables, add the corresponding
migration under `providers/fab/src/airflow/providers/fab/migrations/versions/`
and run:

```bash
prek run update-migration-references-fab check-revision-heads-map-fab --all-files
```

Patch bumps normally have no migrations — skip unless the release notes mention
schema changes.

### Step 8 — Static checks + tests

```bash
prek run --from-ref main --stage pre-commit
uv run --project providers/fab pytest providers/fab/tests/unit/fab/auth_manager -xvs
```

The pre-commit stage runs `sync-provider-readme` (regenerating `README.rst`) and
other FAB hooks. Re-run the alignment test until all four tests pass. The full
provider suite is `breeze testing providers-tests --test-type "Providers[fab]"`.

### Step 9 — Self-review and commit

- `git diff main...HEAD` — verify only the intended files changed, and `uv.lock`
  is a clean FAB-scoped diff.
- Commit subject in imperative mood, plain prose, **no** Conventional-Commits
  prefix, e.g. `Bump flask-appbuilder to X.Y.Z in FAB provider`. Body explains
  *why* (what upstream changes were mirrored), not what.
- No newsfragment, no changelog edit (provider release manager regenerates from
  git log).
- Prepend your PR to the history list in `providers/fab/CONTRIBUTING.rst` so the
  FAB-upgrade record stays current.
- Push to `origin` and open the PR per the repo's PR conventions.

## Gotchas

- **`EXPECTED_FAB_VERSION` is a second pin.** Forgetting it makes
  `test_fab_alignment.py` fail even when everything else is correct.
- **`uv.lock` marker drift.** Always use the pinned uv (Step 4) — a bare
  `uv lock` rewrites hundreds of environment-marker lines and buries the real diff.
- **The alignment test uses AST, not import**, to read FAB's `SecurityManager`
  (to avoid SQLAlchemy model-registry collisions with Airflow's vendored models).
  A green test proves *structural* alignment; it does **not** prove behavioural
  parity — Step 5's manual transplant review is still required.
- **`docs/index.rst` dependency table** may look auto-generated but the reference
  PR edited it by hand; if a docs-regen prek hook rewrites it, let the hook win.
- **`README.rst` is generated, but the sync hook is per-commit.** Don't hand-edit
  it; run `prek run sync-provider-readme` (or the full pre-commit stage) after
  bumping `pyproject.toml` — CI fails on the drift otherwise.
- **Don't touch `providers/fab/docs/upgrading.rst`** — that is end-user guidance
  for upgrading the *provider package* in a deployment, not the developer bump
  workflow.

## Worked example — 5.2.1 to 5.2.2 (patch)

A patch bump that needed **no `override.py` change**, but only after a real
behavioural review — the green alignment test alone was not sufficient evidence:

- FAB 5.2.2 shipped three security-manager fixes: LDAP search-filter escaping,
  OAuth email-allowlist regex anchoring (`email + "$"`), and API-login provider
  validation.
- All three touch methods Airflow *vendors* (`_search_ldap`, `auth_user_ldap`,
  `auth_user_oauth`) — so the alignment test passing did **not** mean "nothing to
  do". Each had to be checked by hand:
  - **LDAP escaping** — Airflow's vendored `_search_ldap` **already** escapes via
    `ldap.filter.escape_filter_chars` (and adds filter-parenthesis validation); it
    was ahead of FAB. No transplant.
  - **OAuth allowlist anchoring** — the match lives in FAB's `AuthOAuthView`
    (`views.py`), and Airflow's `CustomAuthOAuthView.oauth_authorized` delegates
    via `super().oauth_authorized()`, so the fix is inherited from the installed
    FAB 5.2.2. No transplant.
  - **API-login validation / Azure-JWT warning / uuid4** — in FAB core Airflow
    doesn't vendor. Inherited.
- Net change set: `pyproject.toml`, `test_fab_alignment.py`, `docs/index.rst`,
  `README.rst` (via hook), `uv.lock`. Commit: `Bump flask-appbuilder to 5.2.2 in
  FAB provider`.

The lesson the skill encodes: **for every security/behavioural fix in the FAB
release notes, locate the method and check whether Airflow vendors it** — the
alignment test guards structure, you guard behaviour.
