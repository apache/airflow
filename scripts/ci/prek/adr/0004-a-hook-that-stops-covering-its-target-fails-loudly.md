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

# 4. A hook that stops covering its target fails loudly

Date: 2026-07-20

## Status

Accepted

## Context

ADR 2 covers how a hook behaves when it runs. This ADR covers the case one step earlier
— the hook that no longer runs over anything at all, and therefore passes.

Prek selects a hook's inputs from its `files:` / `exclude:` patterns, and many scripts
additionally resolve a directory or metadata file themselves. Those references are plain
strings no type checker, no test of the hook's logic, and no reviewer of an unrelated PR
will notice going stale. When the repository is reorganised — providers moved into
`providers/`, a distribution renamed, a generated file relocated — the pattern silently
stops matching, prek reports the hook as passed, CI is green, and the rule is no longer
enforced. This is the worst failure mode a hook has, worse than a noisily broken one:
**the signal is absence** so nobody investigates, **the violations accumulate underneath**
so the eventual fix is a path correction plus a backlog of real violations, and **it
undermines every other hook** because one hook that passes vacuously makes "static checks
passed" a weaker claim across the board.

Zero matched files is occasionally legitimate — a hook scoped to a distribution a given
commit does not touch. The distinction is between *this run has no inputs* (normal, prek
handles it) and *this hook can no longer find the thing it was written to check* (a
defect).

## Decision

- **A hook that resolves a path, directory, config file or metadata file
  verifies that it exists and exits non-zero with a clear message when it does
  not.** "Expected `<path>`, not found — this hook's target has moved" is a
  correct outcome; silently succeeding is not.
- **A hook whose entire scope has vanished is a failure, not a pass.** When the
  hook's own `files:` pattern can match nothing in the repository at all — as
  opposed to nothing in the current commit — that is a defect to surface.
- **Paths come from the constants in `common_prek_utils.py`**, so a directory
  move breaks one shared definition rather than silently degrading several
  hooks independently.
- **Restructuring pull requests re-run the affected hooks with `--all-files`**;
  a hook is only known to still work when it has been made to run over the moved
  tree.
- **When a dead hook is found, the fix is the hook plus the violations it
  missed**, and the pull request says how long it had been inert.

## Consequences

- Static-check green means the rule was actually evaluated, the only reason to have the
  suite.
- Hooks carry a little extra defensive code — existence checks and explicit error paths
  that are dead weight while everything is in place.
- Reorganisation PRs get more expensive: moving a directory means running the hooks that
  reference it, not only the ones the changed files trigger.
- Occasional false failures when a hook's target is legitimately absent in an unusual
  checkout — the accepted trade against silent inertness.

A change **violates** this decision when it:

- resolves a directory or file and continues quietly when it is missing, or
  iterates an empty result and exits zero;
- hardcodes a repository path instead of deriving it from the shared constants
  in `common_prek_utils.py`;
- moves, renames or removes a directory referenced by a hook without updating
  that hook and re-running it with `--all-files`;
- restores a previously inert hook while leaving the violations that
  accumulated while it was inert unmentioned;
- adds a hook whose passing case is indistinguishable from its no-inputs case.

## Evidence

- #57276 — a provider yaml hook pointing at a location that no longer existed after
  providers moved into `providers/`; passing vacuously since the restructure, closed in
  favour of the complete fix in #57283.
- #63973 — an execution-API version hook that assumed a git remote name varying between
  checkouts, producing a result that depended on local state rather than the tree.
- #67966 — the breeze command-image hook running against a stale `uvx` cache rather than
  current sources, checking something other than the tree in front of it.
- #65611 — `mypy-scripts` and `check-distribution-gitignore` failing on correct trees
  after their scope drifted — the noisy counterpart of the same path-staleness problem.
