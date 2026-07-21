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

Every provider must work across a range of Airflow core versions
(`providers/adr/0002-…`). What this provider answers is *where the branching
goes*. Without a shared answer, each provider writes its own — a local `try: from
airflow.sdk import X / except ImportError: from airflow.models import X`, or an
`if AIRFLOW_V_3_0_PLUS:` ladder. Multiply by ~100 independently released
distributions and the same rename is encoded ~100 times, each free to get the
fallback order wrong, to miss a *rename* (`Dataset` → `Asset` changed the name, not
just the path), or to resolve on `main` and break on the declared floor.

`common.compat` centralises that. `sdk.py` carries the maps — `_IMPORT_MAP` (module
paths tried newest-first), `_MODULE_MAP` (whole modules like `timezone`, `io`), and
`_RENAME_MAP` (the `(new_path, old_path, old_name)` triple for *renamed* symbols) —
and `_compat_utils.create_module_getattr` turns them into a lazy module-level
`__getattr__`. A provider writes `from airflow.providers.common.compat.sdk import
BaseHook` and the resolution order is resolved in exactly one place. The same shape
covers `assets/`, `notifier/`, `security/permissions.py`, `module_loading/`,
`standard/`, `sqlalchemy/orm.py`, `openlineage/`. Roughly 100 provider
`pyproject.toml` files declare `apache-airflow-providers-common-compat` as a
runtime dependency — that fan-out is the point, and why the surface is an interface
rather than a convenience.

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
- Consumers' import lines stay stable across core versions — what makes a single
  provider wheel installable on both sides of the 2→3 boundary.
- The cost is concentration of risk: a wrong map entry is wrong for every consumer
  at once, on whichever core version it got wrong — the reason review here is held
  to a higher bar than an ordinary provider.
- Because resolution is lazy, a mistake surfaces at the consumer's first attribute
  access, not at import — tests must exercise the *specific* branch a change
  touches, including the arm the CI core version does not take.

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

- #56790 — the bulk of the `sdk.py` map, establishing this provider as the single
  place the 2→3 symbol moves are encoded.
- #56884 — factoring the resolution machinery into `create_module_getattr`.
- #56793, #56867 — the consumer side: local version ladders deleted for a
  `common.compat` import (Google, Standard).
- #57016 — the same migration across a family of Apache providers and Elasticsearch.
- #62776, #61812 — individual symbols (`SkipMixin`, `Stats`) pulled into the shared map.
- #57183 — the `check_common_compat_lazy_imports` guard against drift.
- #64933 — one wrong arm of a shim, wrong for every consumer on that core version.
