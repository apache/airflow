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

A large fraction of this documentation tree is not prose at all. It is a
projection of the code: every configuration option, every CLI command and flag,
every REST endpoint and the permissions it requires, every Alembic migration,
the database ER diagram, and the operator/hook cross-reference. All of it exists
somewhere in the source already, and all of it changes on a cadence no human
editor can track.

So the project generates it, in two distinct ways.

Some pages are **rendered at build time** from a directive: the configuration
sections in `configurations-ref.rst` come from an included, extension-produced
fragment; `stable-rest-api-ref.rst` renders the generated OpenAPI spec through
`swagger-plugin`; `cli-and-env-variables-ref.rst` renders the whole CLI through
an `argparse` directive pointed at `airflow.cli.cli_parser`;
`database-erd-ref.rst` embeds a diagram produced by the `generate_erd` Sphinx
extension; `operators-and-hooks-ref.rst` is assembled by
`operators_and_hooks_ref.py` from provider metadata. These files hold real,
editable prose *around* content that does not exist in the repository at all
until the build runs.

Others are **written into the tree by a script or a prek hook**, and mark
themselves as such. `security/api_permissions_ref.rst` opens with
`.. THIS FILE IS AUTO-GENERATED. DO NOT EDIT MANUALLY.` and is produced by
`scripts/ci/prek/extract_permissions.py` via the `generate-api-permissions-doc`
hook. `migrations-ref.rst` and `installation/supported-versions.rst` carry the
`.. Beginning of auto-generated table` / `.. End of auto-generated table`
markers, written respectively by
`scripts/in_container/run_migration_reference.py` from the Alembic history and
by `scripts/ci/prek/supported_versions.py` — with editable prose outside the
markers.

The property this buys is that reference documentation cannot drift. A config
option renamed in the code is renamed on the page in the same commit, without
anyone remembering to do it. That property is the only reason a reference tree
of this size is maintainable by a project this size.

It is also fragile in a specific way. A hand-edit to a generated page *looks*
like a documentation improvement and behaves like one right up until the
generator next runs, at which point the change vanishes — or, worse, the hook
fails in CI on someone else's unrelated PR, and the cost of the edit is paid by
a contributor who did not make it. The tempting cases are exactly the ones that
feel harmless: correcting a typo in a generated table, adding a missing option
someone noticed, tidying a heading. Each of them is a change to the wrong file.

The inverse mistake happens too. A contributor changes a config default, a CLI
flag, or an endpoint's permission requirement, and does not regenerate — leaving
a reference page that confidently states something the code no longer does. That
is worse than a stale page, because reference material is what readers trust
literally.

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

- Reference documentation stays correct without depending on anyone's memory,
  which is what makes the tree's size sustainable.
- Contributors hit a confusing wall the first time: the obvious fix to an
  obviously wrong page is the wrong fix, and the right one is somewhere in the
  code. The area's `AGENTS.md` names the generated pages so this is discoverable
  before the edit rather than after.
- Improving *how* a reference page reads is a change to a generator or a Sphinx
  extension, which is a bigger job than editing prose. Some presentation
  problems therefore persist longer than they would otherwise.
- Reviewers can reject a diff to a generated page mechanically, without judging
  its content.

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
  `security/api_permissions_ref.rst` whenever a public route or the security
  module changes, and the supported-versions hook regenerates
  `installation/supported-versions.rst` from a script.
- #67606 — "Automate stable REST API permission reference doc generation": the
  page moved from hand-maintained to generated precisely because it had drifted.
- #69341 — "Regenerate REST API permission reference doc": the routine
  consequence of that automation, landed as its own change.
- #69535 (closed unmerged) — a documentation-link cleanup that stalled on
  failing static checks; the generation and lint hooks are the gate a docs PR
  has to pass, not an optional extra.
- #68364 — "Docs: Remove stale `reload_on_plugin_change` from API config
  reference": the shape of the defect this decision prevents, where a
  hand-carried reference entry outlived the option it described.
- #62502 (closed unmerged) — a reference-page fix from a contributor who had not
  built the docs and asked reviewers to verify the rendering instead; reference
  changes are checked by running the build, not by pattern-matching neighbouring
  entries.
