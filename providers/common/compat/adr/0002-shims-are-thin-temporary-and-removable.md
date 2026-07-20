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

# 2. Shims are thin, temporary, and removable — this is not a feature home

Date: 2026-07-20

## Status

Accepted

## Context

A package that ~100 other packages already depend on is an attractive place to
put things. Any helper two providers happen to share, any utility that "feels
common", any behaviour that is awkward to place elsewhere — all of it has a
plausible-sounding argument for landing in `common.compat`, because the
dependency edge already exists and adding it costs the author nothing.

That pressure has to be resisted, for two reasons specific to this provider.
First, everything here is carried by every consumer forever: a helper added for
one provider is import surface, maintenance burden, and review risk for the
other ninety-nine. Second, and more fundamentally, a compatibility shim is
supposed to *end*. Its whole justification is that some Airflow core version in
the supported range lacks the symbol or has it under a different name; once that
version drops out of the range, the shim is dead code that still ships. The
provider's value depends on it shrinking as the floor rises, not on it
accumulating.

The codebase already encodes the discipline in places. `module_loading/`
fabricates `is_valid_dotpath` behind a `# TODO: Remove it when Airflow 3.2.0 is
the minimum version` marker — the removal condition is written down at the site,
so the next floor bump can delete it rather than reason about it.
`sqlalchemy/orm.py` fakes `mapped_column` from `Column` for SQLAlchemy < 2.0 in
six lines and nothing more. `security/permissions.py` is a handful of string
constants and one aliased import. `assets/__init__.py` is a version gate around
two import blocks. These are the right size.

The pressure in the other direction is visible too: `lineage/hook.py` carries a
genuine runtime polyfill that attaches `add_extra` to a core collector instance,
and that polyfill has already shipped a `RecursionError` from being re-applied to
an object it had already patched. That is what happens when a shim stops being a
re-export and starts being logic — the bug is not in the provider that reported
it, and it is not in core either.

## Decision

Treat everything in this provider as a temporary bridge with a known expiry, and
keep it as thin as the bridge allows.

- **A shim re-exports, renames, or minimally polyfills.** It does not implement
  provider behaviour, hold shared business logic, or become the place a
  cross-provider utility lives because the dependency edge was convenient. If it
  is not bridging a difference between supported Airflow core versions, it does
  not belong here.
- **Every version-conditional branch carries its removal condition** as a comment
  at the site, naming the core version that makes it deletable — the
  `# TODO: Remove it when Airflow 3.2.0 is the minimum version` form. A branch
  without a stated expiry cannot be safely retired later.
- **A floor bump is a deletion opportunity, and taking it is part of the bump.**
  When the oldest supported core no longer needs an arm, remove the arm rather
  than leaving it as harmless-looking dead code. Dead arms are the reason nobody
  can tell which branches are still load-bearing.
- **Deleting a branch requires checking the declared floor, not the newest
  release.** "Nobody runs Airflow 2 any more" is not the test; the
  `apache-airflow>=` pin in `pyproject.toml` is.
- **A new shim needs a named consumer.** The PR adding it points at the provider
  and the core version that motivated it. Speculative shims with no importer are
  rejected — they are permanent cost for hypothetical benefit.
- **Runtime polyfills must be idempotent and self-detecting.** A polyfill that
  patches a core object has to recognise an already-patched instance and leave
  it alone; re-application must never re-wrap, recurse, or discard state the
  earlier application collected.

## Consequences

- The provider stays small enough to review, and its import surface stays cheap
  for the ~100 packages that pay for it.
- The 2.x arms actually disappear when the floor moves, so the remaining branches
  are the ones that are still doing work — which is what makes reviewing a change
  here tractable at all.
- Contributors are pushed to put shared behaviour in the provider that owns it,
  or into the Task SDK, rather than in the compatibility layer. That is more
  work in the moment and is the intended outcome.
- The removal comments are a maintenance obligation of their own: they have to be
  accurate, and a floor bump that ignores them leaves the codebase worse than one
  that never had them.

A change **violates** this decision when it:

- adds business logic, a shared utility, or provider behaviour here because the
  dependency edge already exists, rather than because it bridges core versions;
- adds a version-conditional branch or a polyfill with no comment stating which
  core floor makes it removable;
- removes a compatibility branch that the *declared* `apache-airflow>=` floor
  still needs, on the assumption that old core versions are no longer in use;
- raises the floor and leaves the branches that the new floor made dead in place;
- adds a shim with no consuming provider in the same PR or an immediate
  follow-up;
- writes a runtime polyfill that cannot detect its own prior application, or that
  re-wraps a method it already wrapped.

## Evidence

- #62927 — "refactor: remove modules that are supposed to be removed in Airflow
  3.2": shims deleted at the point the floor made them dead — the removal half of
  the lifecycle this decision describes.
- #49877 — "Remove `AIRFLOW_2_10_PLUS` conditions", following #49843 — "Bump min
  Airflow version in providers to 2.10": the floor bump and the branch deletion
  treated as one piece of work.
- #58612 — "Bump minimum Airflow version in providers to Airflow 2.11.0": the
  floor move that defines which arms are currently load-bearing.
- #68735 — "Fix `RecursionError` in `common.compat` hook lineage `add_extra`
  polyfill": the concrete cost of a shim that grew into runtime logic without
  being idempotent.
- #60663 — "Fix `BaseAsyncOperator` in common-compat provider for Airflow 2.x":
  a shim whose non-default arm was wrong — the failure mode thin shims are meant
  to make rare.
- #56880 — "Add SQLA's `mapped_column` to common-compat": the right shape for an
  addition here — a few lines bridging a dependency version, nothing more.
- #69208 — "Added `get_async_hook` in `common.compat` provider" and #69140 —
  "Honor async hook subclass overrides in `get_async_connection`": a capability
  added on the shared surface for concrete consumers, then corrected when the
  thin wrapper turned out to bypass subclass behaviour.
