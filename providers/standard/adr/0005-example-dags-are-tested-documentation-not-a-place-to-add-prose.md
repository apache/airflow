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

# 5. Example Dags are tested documentation, not a place to add prose

Date: 2026-07-20

## Status

Accepted

## Context

`example_dags/` in this provider is not a folder of samples. Its files are
imported and parsed by the test suite, are pulled into the published
documentation region by region — `docs/operators/*.rst` uses
`exampleinclude` with `[START …]` / `[END …]` markers, so the *line ranges*
inside these files are load-bearing — and are the first Airflow code most new
users read. Because this provider owns `PythonOperator`, `BashOperator` and the
branch and sensor operators, these are the canonical examples for the whole
project.

This is also the single most common target of low-value change in the area.
A steady stream of PRs adds `doc_md=` to a Dag, converts a comment to a module
docstring, "standardises" docstrings across every example file, or adds
descriptive text that restates the line below it. They are easy to write, easy to
generate, touch many files, and look like documentation work.

They are rejected for consistent reasons. A comment that describes what the next
line does is duplication: it is obvious from the code, it adds nothing a reader
did not have, and it drifts out of date the moment the code changes — the DRY
argument applies to prose about code, not only to code. Text added *inside* a
region an `exampleinclude` extracts changes the published page without the author
seeing the rendered result. And a sweep across every example file is expensive to
review in proportion to the value: reviewers must read every hunk to find the one
that broke a marker or a parse.

Comments in examples earn their place by supplying *context* — why this operator
rather than another, what the surrounding pipeline assumes, what the reader should
notice — which is precisely what is not visible from the code.

## Decision

**Changes to `example_dags/` must make the example demonstrably more useful to a
reader, and must keep it runnable.**

- **Add context, never content.** A comment or docstring explains why something
  is done, what the example assumes, or what a reader should take away. If it
  restates the line beneath it, it is noise and will be removed.
- **The Dag must still parse and run.** These files are imported by tests and by
  Dag-loading tooling; they keep valid `dag_id`s, tags, and imports, and they do
  not acquire dependencies the provider does not declare.
- **Do not disturb `exampleinclude` regions.** Inserting or reflowing lines
  between `[START x]` and `[END x]` changes published documentation. If the
  region changes, say which docs page it feeds and what the rendered result looks
  like.
- **No cosmetic sweeps.** A PR that applies the same formatting, docstring, or
  `doc_md` treatment across many example files is not accepted on grounds of
  consistency alone. Change the examples the reader is actually failing on.
- **Prefer the prose page over the Dag file.** Narrative explanation belongs in
  `docs/operators/*.rst`, where it renders as prose, gets reviewed as prose, and
  cannot break a parse.
- **A new example carries its own reason.** It illustrates a capability the
  existing examples do not, and it is wired into a documentation page.

## Consequences

- The examples stay short, executable, and worth reading, and the docs build
  keeps working because nobody moved a marker by accident.
- Reviewer attention is spent on operator behaviour rather than on prose diffs
  across twenty files.
- The cost falls on newcomers, and it is a genuine one: "improve the docs" is
  standard first-contribution advice, this folder is the most inviting target in
  the repo, and a well-intentioned first PR here is very likely to be closed. The
  mitigation is to say so up front — in this provider's `AGENTS.md` and in this
  ADR — rather than after the work is done.
- Some examples remain thinner than they could be, because "this could use more
  explanation" is not on its own sufficient reason to change them.

A change **violates** this decision when it:

- adds `doc_md`, a module docstring, or comments to example Dags without
  identifying what a reader was unable to understand;
- adds text that restates the adjacent code rather than explaining context;
- edits many example files with the same mechanical treatment for consistency;
- inserts or reflows lines inside an `exampleinclude` region without saying which
  documentation page is affected;
- leaves an example that no longer parses, loses its tags, or depends on
  something the provider does not declare;
- adds a new example that duplicates the capability an existing one already
  shows.

## Evidence

- #60119 — "Add `doc_md` documentation to python decorator example Dag": closed.
  The review found no meaningful documentation improvement, and set out the
  general rule — comments that literally describe what is in the code are useless
  when the code already shows it, they duplicate and so drift, and comments in
  examples should add context, not content.
- #60563 — "Add `doc_md` to `example_bash_decorator` Dag", #66443 — "Set `doc_md`
  on TaskFlow decorator example Dags": the same change proposed again, closed.
- #60657 and #61481 — "Standardize example Dag docs via module docstring", twice:
  both closed. The mechanical sweep across the folder is the shape this decision
  names explicitly.
- #60149 — "Add meaningful Dag-level documentation to example Dags": closed after
  review found broken documentation links introduced by the added prose — the
  concrete cost of adding text nobody rendered.
- #61953 — "Examples: add measurement correction storyline example Dags": closed;
  new examples need a capability to illustrate that the existing set does not.
- #60665 — a docs PR closed in favour of a broader one already in flight for the
  same issue: this folder attracts duplicate work, and checking for an existing
  PR first is part of the criteria.
