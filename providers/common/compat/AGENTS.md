---
triage_review_imbalance:
  area: provider-common-compat
  criticality: high              # ~100 providers import these shims; a wrong shim breaks them on one core version
  review_difficulty: expert
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "src/airflow/providers/common/compat/sdk.py"          # the big lazy re-export map — every provider's front door
    - "src/airflow/providers/common/compat/_compat_utils.py" # `create_module_getattr` machinery behind every lazy shim
    - "src/airflow/providers/common/compat/version_compat.py" # AIRFLOW_V_3_x_PLUS gates; copied, not imported
    - "src/airflow/providers/common/compat/assets/"          # Dataset→Asset rename bridge for Airflow 2.x
    - "src/airflow/providers/common/compat/lineage/hook.py"  # runtime polyfill that mutates the core collector
    - "src/airflow/providers/common/compat/openlineage/"     # facet + version-check surface used by OL-emitting providers
    - "pyproject.toml"                                       # the `apache-airflow>=` floor every consumer inherits
  codeowners_ref: ".github/CODEOWNERS"
  # NOTE: there is NO `/providers/common/compat/` line in `.github/CODEOWNERS` —
  # this provider has no declared owner, despite being the widest-blast-radius
  # provider in the repo. The list below is derived from the top recent authors of
  # `providers/common/compat/` (`git log --format='%an'`) and is an internal
  # routing signal only — never @-mentioned in drafted PR text.
  experts: ["potiuk", "eladkal", "kaxil", "amoghrajesh"]
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Common Compat provider — Agent Instructions

This directory holds `apache-airflow-providers-common-compat` — the one provider
whose entire reason to exist is absorbing **differences between Airflow core
versions** so that the other providers do not each carry their own version
ladder. It is a dependency of roughly **100 provider distributions**: every
`from airflow.providers.common.compat.sdk import ...` in the repo resolves
through the lazy `__getattr__` built in `sdk.py`, and every wrong entry in that
map is a break in ~100 packages at once, on whichever core version the entry got
wrong.

Read the parent area first: `providers/AGENTS.md` and `providers/adr/`. The
decisions there — `provider.yaml` / generated `pyproject.toml` as the metadata
source of truth, independent release with a declared core floor, consuming core
only through the public SDK surface — apply here unchanged and are **not**
repeated below. `adr/` in this directory records the decisions that are specific
to being the shim layer.

## Why changes here are expensive to review

- **The blast radius is the whole provider ecosystem.** A shim that resolves to
  the wrong symbol, or that raises on one branch of a version gate, does not
  break _this_ provider — it breaks every consumer that imports it, and only on
  the core version the reviewer did not run.
- **The failure is invisible on a `main` checkout.** Most of the code here is a
  branch the CI default never takes: the Airflow 2.x arm, the pre-3.2 arm, the
  "OpenLineage client not installed" arm. `AIRFLOW_V_3_0_PLUS` is `True` in the
  dev environment, so the fallback path a diff touches is frequently the one
  nothing exercised.
- **Almost everything is lazy.** `sdk.py`, `standard/*.py` and friends resolve
  symbols at attribute-access time through `create_module_getattr`, so a typo in
  a module path is not an import error at review time — it is an `ImportError`
  in a user's task, months later. The `check_common_compat_lazy_imports` prek
  hook exists precisely because the `TYPE_CHECKING` block and the runtime maps
  can silently disagree.
- **The declared floor is a contract with ~100 packages.** Raising
  `apache-airflow>=` here, or reshaping a re-exported symbol, is a
  cross-provider release event, not a local edit — see `adr/0003`.

## Knowledge a reviewer (and a substantial contributor) needs

- How `create_module_getattr` in `_compat_utils.py` works: `_IMPORT_MAP` (tuple
  of paths tried newest-first), `_MODULE_MAP` (whole modules), `_RENAME_MAP`
  (new name at new path, old name at old path — the `Dataset` → `Asset` shape),
  and how each one degrades when every candidate path fails.
- Which core versions are actually in range. The floor here is
  `apache-airflow>=2.11.0`, and `version_compat.py` exposes
  `AIRFLOW_V_3_0_PLUS` / `_3_1_` / `_3_2_` / `_3_3_PLUS`. A shim must be correct
  on **all** of them, not just on the newest.
- Why `version_compat.py` is **copied** into each provider rather than imported
  from here — `run_check_imports_in_providers.py` rejects a `version_compat`
  import resolving outside the importing provider's own root. This provider
  holds the copy other providers copy _from_; it is not an import target for
  those constants.
- The runtime-polyfill patterns and their hazards: `lineage/hook.py` attaches
  `add_extra` to a collector instance that may already have it (a re-application
  bug there produced a real `RecursionError`), and `sqlalchemy/orm.py` fabricates
  `mapped_column` when SQLAlchemy is too old.
- The optional-dependency shape: `openlineage` and `standard` are **extras**, so
  the OpenLineage and Standard shims must degrade cleanly when the underlying
  package is absent — that is what
  `require_provider_version` / `require_openlineage_version` and
  `AirflowOptionalProviderFeatureException` are for.

## Before opening a PR here — authoring-agent guard

**This is a high-criticality, expensive-to-review area with the widest fan-out of
any provider.** If you are an agent preparing a change here on behalf of a
person, first judge whether the **driving person** can state which core versions
each branch of their change is exercised on, and has a reason to believe the
non-default branch works. **If they cannot, do not create the PR.** Say so
plainly and redirect them:

- to the **consuming provider** where the need actually arose — a shim is only
  justified by a concrete consumer, and adding one speculatively is the most
  common bad change here; or
- to a **well-scoped bug with a reproduction pinned to a core version**, rather
  than a refactor of the import maps; or
- to **discussing it first** (issue or dev list) when the change raises this
  provider's floor, renames a re-exported symbol, or adds a new module — those
  are release-contract decisions across ~100 packages.

"Tidying" the import maps, collapsing branches that look dead, or moving a
helper here because two providers happen to use it are all changes that look
small and are not. Do not open them without a consumer-side reason.

## Review criteria

Mined from real review discussion on the ~136 commits touching this provider —
the changes reviewers repeatedly required, and the reasons changes here get
closed. **If you are preparing a change here, treat this as a pre-flight
checklist and fix every applicable item _before_ opening the PR.** Triage applies
the same list: a PR that lands with unmet items is drafted back to its author
with the specific gaps. Ordered by how often reviewers raise each.

**Correctness across the whole supported core range (the defining concern here):**

- [ ] **Every branch of the version gate is correct, not just the one CI runs.**
      A change touching an `AIRFLOW_V_3_x_PLUS` branch must say what the _other_
      arm resolves to on the declared floor. "Works on main" is not evidence —
      main only ever takes one arm.
- [ ] **The fallback chain is ordered newest-first and every entry is real.**
      In `_IMPORT_MAP` / `_MODULE_MAP` the paths are tried in order; a stale or
      misspelled path silently falls through to the next candidate (or raises at
      the user's first attribute access), it does not fail at import.
- [ ] **Renames go in `_RENAME_MAP`, not `_IMPORT_MAP`** — a symbol whose _name_
      differs across versions (`Dataset` → `Asset`) needs the
      `(new_path, old_path, old_name)` triple; putting it in `_IMPORT_MAP`
      resolves only on the newer core.
- [ ] **Keep the `TYPE_CHECKING` block and the runtime maps in sync.** Every key
      in `_IMPORT_MAP` / `_RENAME_MAP` / `_MODULE_MAP` must have a matching
      `TYPE_CHECKING` import and vice versa —
      `check_common_compat_lazy_imports` fails the build otherwise, and the
      mismatch it catches is a real type-vs-runtime divergence.
- [ ] **A polyfill must be idempotent and must not re-wrap itself.** Runtime
      patching of a core object (the `add_extra` polyfill on the hook lineage
      collector) has to detect an already-patched instance; re-application
      recursion has already shipped once as a bug.

**Scope — this is a compatibility surface, not a feature home:**

- [ ] **No new business logic here.** A shim re-exports, renames, or minimally
      polyfills a symbol that exists (or existed) in core. Behaviour that is not
      a bridge between core versions belongs in the provider that needs it.
- [ ] **A new shim needs a named consumer in the same PR or an immediate
      follow-up.** Shims with no importer are dead weight the whole ecosystem
      pays to carry.
- [ ] **Carry a removal condition.** A branch that exists only for an old core
      gets a comment saying which floor makes it removable (the
      `# TODO: Remove it when Airflow 3.2.0 is the minimum version` form), so the
      next floor bump can delete it instead of guessing.
- [ ] **Deleting a dead branch is a real change, not cleanup.** Removing a
      version arm is only safe once the oldest core in `pyproject.toml` no longer
      needs it — check the declared floor, not the newest release.

**Fan-out and release contract:**

- [ ] **Never change a re-exported symbol's shape** — its name, arity, return
      type, or which module it resolves to on a given core — without treating it
      as a breaking change for consumers. Consumers pin a `>=` floor; they do not
      pin a ceiling.
- [ ] **Bumping this provider's `apache-airflow>=` floor is a cross-provider
      event.** Do it deliberately, in a PR that says so, not as a side effect of
      needing one newer API.
- [ ] **A consumer that needs a brand-new shim gets a
      `# use next version` marker** on its
      `apache-airflow-providers-common-compat>=` dependency, because this package
      and the consumer are released separately and the release manager resolves
      the marker to the concrete version at release time.
- [ ] **Optional integrations stay optional.** OpenLineage and Standard are
      extras; a shim that touches them must not make `import
      airflow.providers.common.compat...` fail when the extra is not installed —
      degrade via `AirflowOptionalProviderFeatureException`, do not raise at
      import.

**Code quality reviewers consistently require:**

- [ ] **Keep the shims lazy.** New re-exports go through the `__getattr__` maps;
      a module-level eager import of a core symbol pulls weight onto every
      consumer's import path and can break on the core version that lacks it.
- [ ] **Don't swallow the original error.** The fallback loops chain the last
      failure into the raised `ImportError`; a shim that catches and returns
      `None` (or re-raises bare) makes a missing dependency indistinguishable
      from a wrong path.
- [ ] **Narrow the `except`** to `ImportError` / `ModuleNotFoundError` /
      `AttributeError` as the existing machinery does — a broad `except` here
      hides genuine bugs in the imported module.
- [ ] **Action-verb / intent-revealing names**, and reuse
      `create_module_getattr` rather than hand-rolling a fourth `__getattr__`
      that will drift from the hook's expectations.

**Tests, docs, process:**

- [ ] **Tests must exercise the branch the change touches**, including the
      not-currently-installed core arm (mock the version constant / the import),
      and must fail without the change. A test that only covers the arm CI takes
      proves nothing about a compat shim.
- [ ] **Never add a newsfragment** — providers are released from `main` and the
      changelog is regenerated from `git log`. A user-facing note goes directly
      into `docs/changelog.rst` under the `Changelog` header.
- [ ] **Follow the PR template**, disclose AI assistance, and name the consuming
      provider and core version that motivated the change.

> Mined from PR review history across `providers/common/compat/`; the sample is
> dominated by the Airflow 2→3 migration era (the `Dataset` → `Asset` rename, the
> `airflow.sdk` consolidation, the mass migration of providers onto
> `common.compat.sdk`), so conventions for a future 3→4 transition are
> under-represented. This provider has **no** `.github/CODEOWNERS` entry — route
> review by the ADRs here and by the consuming provider's owner. Extend as new
> patterns emerge.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
That applies with unusual force here: adding a module, raising the
`apache-airflow>=` floor, or reshaping an entry in the import maps is a decision
about ~100 independently released packages, and is far cheaper to align on
_before_ the code than during review.
