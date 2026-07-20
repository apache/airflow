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

# 1. Hooks are fast and precisely scoped

Date: 2026-07-20

## Status

Accepted

## Decision

- Every hook declares the narrowest `files:` pattern that still catches the rule
  it enforces, and uses `exclude:` to carve out subtrees it must not inspect.
- A hook prefers `pass_filenames: true` and acts only on the files it is handed.
  `pass_filenames: false` — a whole-tree scan on every commit — needs a reason,
  and where the scan is unavoidable the expensive part is gated behind an
  "inputs unchanged" short-circuit.
- Hooks that need the Breeze CI image, or that are otherwise slow, are
  registered at the *end* of the relevant `.pre-commit-config.yaml`, so fast
  local checks fail first. A hook too slow to run on every commit moves to
  `stages: ['manual']` rather than staying in the default set.
- Cost is measured against the whole repository (`prek run <id> --all-files`),
  not against the author's two touched files.

## Context

The hooks in this directory are not runtime code and ship to no user, so the
cost of a defect here is easy to underestimate. But every hook registered in a
`.pre-commit-config.yaml` runs on **every contributor's machine, on every
commit**, and again in CI. There are over a hundred hooks in the repo-wide
config alone, plus per-distribution configs for `airflow-core`, `providers`,
`task-sdk`, `chart`, `airflow-ctl`, and others.

That multiplier turns small inefficiencies into a project-wide tax. A hook that
takes two seconds longer than it needs to, multiplied across every commit by
every contributor, costs the project far more time than the hook itself was
worth. A hook scoped with a bare `files: \.py$` when it only inspects
`providers/` makes every core, SDK, and dev-tooling commit pay for a check that
can never fire on it.

The failure mode is also social, not just mechanical: when the commit hooks feel
slow, contributors reach for `--no-verify`, and the whole static-check layer
quietly stops protecting the repository. Keeping the default set fast is what
keeps it running at all.

## Consequences

Contributors get fast feedback on the checks that can actually apply to their
change, and the default hook set stays cheap enough that bypassing it is not
tempting. Slow-but-valuable checks remain available behind the manual stage or
at the tail of the config, where they cost nothing on a routine commit.

The cost is that adding a hook is slightly more work: the author has to think
about the pattern rather than reaching for `\.py$`, and has to measure the
all-files run before opening the PR.

A change **violates** this decision when it:

- registers a hook whose `files:` pattern is materially broader than the paths
  the check can act on — for example a repo-wide `\.py$` for a check that only
  ever inspects one distribution;
- adds or converts a hook to `pass_filenames: false` with a whole-tree scan on
  every commit, without a short-circuit and without stating why the per-file
  form is impossible;
- registers a Breeze-image-dependent or otherwise slow hook in the middle of the
  config rather than at its end, or leaves it in the default stages when it is
  too slow for a per-commit run;
- introduces a new hook without having run it across the whole repository, so
  its real cost and false-positive rate are unknown at review time;
- shells out to `breeze` directly instead of using `run_command_via_breeze_run`
  from `common_prek_utils.py`, bypassing the shared image-invocation path.

## Evidence

- #64131 — speed up the "Generate the FastAPI API spec" prek hook from ~2 minutes to ~25 seconds.
- #64124 — skip the pnpm install in the UI pre-commit hook when dependencies are unchanged.
- #55154 — replace the translations freeze check with a simpler and faster implementation.
- #58439 — limit Python prek hooks to a single pinned Python version so hook setup is not re-paid per interpreter.
