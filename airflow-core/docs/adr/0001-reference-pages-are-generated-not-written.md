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

# 1. Reference pages are generated from source, not written by hand

Date: 2026-07-20

## Status

Accepted

## Context

A large fraction of this documentation tree is not prose but a projection of the
code: every config option, CLI command and flag, REST endpoint and its permissions,
Alembic migration, the ER diagram, the operator/hook cross-reference. All of it
exists in the source and changes on a cadence no editor can track, so the project
generates it in two ways.

Some pages are **rendered at build time** from a directive — the config sections in
`configurations-ref.rst`, the OpenAPI spec in `stable-rest-api-ref.rst` via
`swagger-plugin`, the whole CLI in `cli-and-env-variables-ref.rst` via an `argparse`
directive on `airflow.cli.cli_parser`, the diagram in `database-erd-ref.rst` from
`generate_erd`, and `operators-and-hooks-ref.rst` assembled by
`operators_and_hooks_ref.py` — holding editable prose *around* content that does not
exist in the repo until the build runs. Others are **written into the tree by a
script or prek hook** and mark themselves: `security/api_permissions_ref.rst` opens
with `.. THIS FILE IS AUTO-GENERATED. DO NOT EDIT MANUALLY.` (via
`extract_permissions.py` / `generate-api-permissions-doc`), and `migrations-ref.rst`
and `installation/supported-versions.rst` carry `auto-generated table` markers with
editable prose outside them.

The property this buys is that reference documentation cannot drift — a config
option renamed in the code is renamed on the page in the same commit — and that is
the only reason a reference tree this size is maintainable. It is fragile in two
directions. A hand-edit to a generated page looks like an improvement until the
generator next runs and the change vanishes, or the hook fails in CI on someone
else's PR; the tempting cases (a typo, a missing option, a heading) are all changes
to the wrong file. The inverse mistake is changing a config default, flag, or
permission and not regenerating — leaving a page that confidently states something
the code no longer does, worse than a stale page because readers trust reference
material literally.

## Decision

Generated documentation is treated as generated:

- **Generated content is never edited by hand.** That means the whole of
  `security/api_permissions_ref.rst`; everything between the
  `auto-generated table` markers in `migrations-ref.rst` and
  `installation/supported-versions.rst`; and the directive-rendered reference in
  `configurations-ref.rst`, `cli-and-env-variables-ref.rst`,
  `stable-rest-api-ref.rst`, `database-erd-ref.rst`, and
  `operators-and-hooks-ref.rst` — where the surrounding prose *is* editable.
  Before editing any reference page, check for an auto-generated banner or
  marker and for a prek hook or Sphinx extension that owns it.
- **Fix the source, then regenerate.** A wrong option description is fixed in the
  config definition; a wrong endpoint permission in the route; a wrong migration
  summary in the migration. The page follows.
- **A change to a generated page's inputs regenerates the page in the same PR.**
  Run the relevant prek hook rather than leaving it to CI or to the next
  contributor.
- **New reference material that can be derived is derived.** If a table can be
  produced from the source of truth, add the generation rather than the table.

## Consequences

- Reference documentation stays correct without depending on anyone's memory.
- Contributors hit a confusing wall first: the obvious fix to an obviously wrong
  page is the wrong fix, and the right one is in the code. The area's `AGENTS.md`
  names the generated pages so this is discoverable before the edit.
- Improving *how* a reference page reads means changing a generator or extension — a
  bigger job than editing prose — so some presentation problems persist longer.
- Reviewers can reject a diff to a generated page mechanically.

A change **violates** this decision when it:

- edits generated content directly — a file carrying the auto-generated banner,
  or the region between the `auto-generated table` markers — including to fix a
  typo, add a missing entry, or adjust formatting;
- changes a config option, CLI command, REST route, permission, migration, or
  supported-version set without regenerating the page that projects it;
- adds a hand-maintained table or list duplicating information a generator
  already produces, instead of extending the generator;
- silences a failing generation hook, or commits the page without running the
  hook, to get an unrelated change through CI.

## Evidence

- The prek hooks themselves: `generate-api-permissions-doc` regenerates
  `security/api_permissions_ref.rst` when a public route or the security module
  changes; the supported-versions hook regenerates
  `installation/supported-versions.rst`.
- #67606 — the REST API permission reference moved from hand-maintained to generated
  because it had drifted.
- #69341 — the routine regeneration that automation produces, landed on its own.
- #69535 (closed) — a docs-link cleanup stalled on failing static checks: the
  generation and lint hooks are the gate a docs PR must pass.
- #68364 — a hand-carried reference entry that outlived the option it described: the
  defect this decision prevents.
- #62502 (closed) — a reference fix from a contributor who had not built the docs
  and asked reviewers to verify the rendering; reference changes are checked by
  running the build.
