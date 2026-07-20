<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# 1. Core-version branching lives here once, so providers do not branch

Date: 2026-07-20

## Status

Accepted

## Context

Every provider must work across a range of Airflow core versions — that is the
parent area's decision, recorded in `providers/adr/0002-…`, and it is not
restated here. What this provider exists to answer is the question that decision
leaves open: *where does the branching go?*

Without a shared answer, each provider writes its own. The natural move, when a
symbol moved between Airflow 2.x and 3.x, is a local `try: from airflow.sdk
import X / except ImportError: from airflow.models import X`, or an
`if AIRFLOW_V_3_0_PLUS:` ladder next to the import. Multiply that by ~100
independently released provider distributions and the same rename is encoded
~100 times, each copy free to get the fallback order wrong, to miss the *rename*
(`Dataset` → `Asset` is not just a moved path — the name changed too), or to
resolve correctly on the maintainer's `main` checkout and incorrectly on the
declared floor. When core moves the symbol again, ~100 places have to be found
and fixed.

`common.compat` centralises that branching. `sdk.py` carries the maps —
`_IMPORT_MAP` (a tuple of module paths tried newest-first), `_MODULE_MAP` (whole
modules such as `timezone` and `io`), and `_RENAME_MAP` (the
`(new_path, old_path, old_name)` triple for symbols that were *renamed*, not
merely moved) — and `_compat_utils.create_module_getattr` turns them into a lazy
module-level `__getattr__`. A provider writes `from
airflow.providers.common.compat.sdk import BaseHook` and the resolution order is
somebody else's problem, resolved in exactly one place. The same shape covers
the non-`sdk` surfaces: `assets/`, `notifier/`, `security/permissions.py`,
`module_loading/`, `standard/`, `sqlalchemy/orm.py`, `openlineage/`.

Roughly 100 provider `pyproject.toml` files now declare
`apache-airflow-providers-common-compat` as a runtime dependency. That fan-out is
the point — it is the measure of how much duplicated branching this provider
absorbs — and it is also why the surface has to be treated as an interface
rather than as a convenience.

## Decision

Cross-core-version resolution of a symbol is expressed **once**, here, and
consumed by providers as a plain import.

- **Providers import the symbol from `common.compat`**, not from core with a
  local `try`/`except ImportError` or an `AIRFLOW_V_X_PLUS` ladder around the
  import. If a provider needs a symbol whose location or name differs across the
  supported core range, the branching belongs in this provider.
- **New re-exports go through the lazy `__getattr__` maps.** Add the entry to
  `_IMPORT_MAP` / `_MODULE_MAP` / `_RENAME_MAP` and reuse
  `create_module_getattr`; do not hand-roll another `__getattr__`, and do not add
  a module-level eager import that every consumer then pays for on every import.
- **Renamed symbols use `_RENAME_MAP`.** A symbol that changed *name* between
  core versions needs the new-path/old-path/old-name triple; an `_IMPORT_MAP`
  entry only ever looks for the new name and therefore resolves on new core only.
- **Fallback order is newest-first, and every candidate path is real.** The maps
  are tried in order and the last failure is chained into the raised
  `ImportError`; a stale path silently falls through rather than failing loudly,
  so each entry has to be verified against the version it claims to serve.
- **The `TYPE_CHECKING` block mirrors the runtime maps exactly.** Every map key
  has a matching `TYPE_CHECKING` import and vice versa — the
  `check_common_compat_lazy_imports` prek hook fails the build on a mismatch,
  because a divergence there means type checkers and runtime disagree about what
  the shim resolves to.
- **`version_compat.py` is still copied, not imported, by other providers.**
  This provider holds the canonical copy and uses its own constants internally;
  it is not an import target for `AIRFLOW_V_3_x_PLUS` in other providers, per the
  parent decision and `run_check_imports_in_providers.py`.

## Consequences

- A core rename is handled in one diff instead of ~100, and a provider author
  needs no knowledge of which Airflow release moved what.
- Consumers' import lines stay stable across core versions, which is what makes
  a single provider wheel installable on both sides of the 2→3 boundary.
- The cost is concentration of risk: a wrong entry in the maps is wrong for every
  consumer at once, on whichever core version the entry got wrong. That is the
  trade this provider deliberately makes, and the reason review here is held to a
  higher bar than an ordinary provider.
- Because resolution is lazy, a mistake surfaces at the consumer's first
  attribute access rather than at import — tests must exercise the *specific*
  branch a change touches, including the arm the CI core version does not take.

A change **violates** this decision when it:

- adds a `try: … except ImportError:` fallback or an `AIRFLOW_V_X_PLUS` import
  ladder inside a consuming provider for a symbol that could be re-exported here;
- adds a re-export as a module-level eager import instead of an entry in the
  lazy `__getattr__` maps, putting the import cost and the version risk on every
  consumer;
- registers a *renamed* symbol in `_IMPORT_MAP` instead of `_RENAME_MAP`, so the
  old-core arm can never resolve;
- leaves the `TYPE_CHECKING` block and the runtime maps disagreeing, or silences
  `check_common_compat_lazy_imports` instead of fixing the divergence;
- adds a fallback path that is not verified to exist on the core version it
  claims to serve, relying on the silent fall-through to the next candidate;
- imports `version_compat` constants from this provider into another provider
  rather than copying the file, re-creating the release coupling the copy exists
  to avoid.

## Evidence

- #56790 — "Add comprehensive compatibility imports for Airflow 2 to 3
  migration": the bulk of the `sdk.py` map, establishing this provider as the
  single place the 2→3 symbol moves are encoded.
- #56884 — "Common.Compat: Extract reusable compat utilities and rename to sdk":
  factoring the resolution machinery into `create_module_getattr` so every shim
  module shares one implementation instead of hand-rolled `__getattr__`s.
- #56793 and #56867 — "Simplify version-specific imports in the Google provider"
  / "… in the Standard provider": the consumer side of the decision — local
  version ladders deleted in favour of a `common.compat` import.
- #57016 — "Migrate Apache providers & Elasticsearch to `common.compat`": the
  same migration applied across a family of providers.
- #62776 — "Consolidate `SkipMixin` imports through `common-compat` layer" and
  #61812 — "Route providers to consume `Stats` from common compat provider":
  individual symbols pulled out of per-provider branching into the shared map.
- #57183 — "Extract prek hooks for Common.Compat provider": the
  `check_common_compat_lazy_imports` guard that keeps the `TYPE_CHECKING` block
  and the runtime maps from drifting apart.
- #64933 — "Fix `RESOURCE_ASSET` compatibility with Airflow 2.x in
  common-compat": the failure mode this decision concentrates — one wrong arm of
  a shim, wrong for every consumer on that core version.
