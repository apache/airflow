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

# 2. Hooks fail actionably and deterministically

Date: 2026-07-20

## Status

Accepted

## Decision

- A failing hook prints the hook that failed, the offending file (and line,
  where the check is per-file), and what to change. The contributor must not
  have to open the hook script to learn what it wants.
- Where the fix is unambiguous — formatting, regenerating a generated file,
  sorting a list — the hook applies the fix in place and then fails, rather than
  reporting a diff for the contributor to transcribe by hand.
- Hooks in the default stages do not use the network. A check that genuinely
  requires it lives in `stages: ['manual']` or in CI.
- Hooks resolve paths from the constants in `common_prek_utils.py`
  (`AIRFLOW_ROOT_PATH` and friends), never from the current working directory,
  and never from a cache that can go stale relative to the working tree.
- Versions that affect hook behaviour — `additional_dependencies`,
  `language_version`, third-party repo `rev:` — are pinned, so a hook cannot
  change what it accepts without a commit that says so.

## Context

A static check is a conversation with a contributor in the middle of something else: they
ran `git commit`, it did not happen, and whether that is a thirty-second correction or a
thirty-minute detour is decided entirely by what the hook printed.

The determinism half is a distinct and worse failure. A hook that depends on network
reachability, a stale cache, the directory the contributor ran from, or an unpinned
upstream tool passes locally and fails in CI — or passes for one contributor and fails for
another on the same commit. That destroys trust in the static-check layer faster than a
missing check would, because the contributor cannot tell whether their change is wrong or
the tooling is. The repository has repeatedly had to fix hooks that read a stale `uvx`
cache instead of local sources, or resolved a git remote by an assumed name. Contributors
who cannot trust the hooks start bypassing them, at which point the checks protect nothing.

## Consequences

A failing hook becomes a short instruction rather than an investigation, and a green local
run is a real signal about CI; auto-fixing hooks convert a class of failures into a
`git add` and a re-commit. The cost is an error-message and input-hygiene burden on hook
authors: the check has to explain itself and depend only on the working tree and pinned
tooling.

A change **violates** this decision when it:

- makes a hook exit non-zero with no message, a bare traceback, or a message
  that does not identify the hook and the file that caused it;
- reports a mechanical, unambiguous correction as a diff or a description
  instead of applying it and failing;
- adds a network call — an API request, a remote fetch, a registry lookup — to a
  hook that runs in the default stages;
- derives a repository path from `os.getcwd()`, a relative path, or an assumed
  git remote name instead of the shared constants and resolution helpers in
  `common_prek_utils.py`;
- reads a cached or previously-generated artifact as the source of truth when
  the working tree is the source of truth;
- leaves `additional_dependencies`, `language_version`, or a third-party
  `rev:` unpinned, so the hook's behaviour can drift under contributors without
  any commit to the repository.

## Evidence

- #61712 — include the failing mypy hook's name in the error message so the contributor knows which one to re-run.
- #67966 — run the breeze command-image prek hook against local sources rather than a stale `uvx` cache.
- #63973 — fix the execution-API version hook's remote reference, which assumed a git remote name that varies between checkouts.
- #63606 — add a cooldown period to the upgrade-important-versions check so it stops producing environment-timing-dependent results.
- #65611 — fix the `mypy-scripts` and `check-distribution-gitignore` hooks after they began failing on correct trees.
