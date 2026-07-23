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

`example_dags/` is not a folder of samples. Its files are imported and parsed by
the test suite, pulled into the published docs region by region (`docs/operators/*.rst`
uses `exampleinclude` with `[START …]` / `[END …]` markers, so the *line ranges*
are load-bearing), and are the first Airflow code most new users read — the
canonical examples for the whole project, since this provider owns `PythonOperator`,
`BashOperator` and the branch and sensor operators.

It is also the most common target of low-value change here: a steady stream of PRs
adding `doc_md=`, converting a comment to a module docstring, "standardising"
docstrings across every file, or adding text that restates the line below.
They are rejected consistently — a comment describing the next line is duplication
that drifts out of date (DRY applies to prose about code too); text *inside* an
`exampleinclude` region changes the published page without the author seeing the
render; and a folder-wide sweep is expensive to review for the one hunk that broke
a marker or a parse. Comments earn their place by supplying *context* — why this
operator, what the pipeline assumes, what to notice — which is exactly what is not
visible from the code.

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

- The examples stay short, executable, and worth reading, and the docs build keeps
  working because nobody moved a marker by accident.
- Reviewer attention goes to operator behaviour, not prose diffs across twenty files.
- The cost falls on newcomers and is genuine: "improve the docs" is standard
  first-contribution advice and this folder is the most inviting target, so a
  well-intentioned first PR here is likely closed. The mitigation is saying so up
  front, in this provider's `AGENTS.md` and this ADR.
- Some examples stay thinner than they could be — "this could use more explanation"
  is not on its own a reason to change them.

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

- #60119 — `doc_md` on the python decorator example, closed: no meaningful
  improvement, and set out the rule — comments restating the code duplicate and
  drift; examples add context, not content.
- #60563, #66443 — the same `doc_md` change proposed again, closed.
- #60657, #61481 — "standardize docs via module docstring" twice, both closed: the
  mechanical folder-wide sweep this decision names.
- #60149 — Dag-level docs closed after review found broken documentation links from
  the added prose: the cost of adding text nobody rendered.
- #61953 — new storyline example Dags closed: new examples need a capability the
  existing set does not show.
- #60665 — a docs PR closed for a broader one already in flight: this folder
  attracts duplicate work, so checking for an existing PR first is in the criteria.
