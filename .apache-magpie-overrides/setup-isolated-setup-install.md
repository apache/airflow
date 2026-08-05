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

# Override: setup-isolated-setup-install — seed the breeze permission baseline

## What this overrides

Augments the **settings merge** the `setup-isolated-setup-install` skill
performs (its "Golden rules → Do not overwrite an existing settings file
silently" desired-merge, and the project-local settings write in **Step P**).
When wiring the isolated / secure agent setup for this repo, add the
project-specific Bash permission allow-list below to the adopter's
**project-local** settings file (`<repo>/.claude/settings.local.json`) —
merged alongside the sandbox `allowRead` / `allowWrite` entries Step P already
writes there, surfaced in the same diff, and applied with the same approval.

This seeds a per-machine baseline at install time; it does **not** introduce a
committed project-scope `.claude/settings.json` (that path stays gitignored in
this repo).

## Permissions to add

Add each of these to `permissions.allow` (create the key if absent). Never
remove or reorder pre-existing entries — this is an additive merge, and an
entry already present is a no-op:

```text
Bash(breeze *)
Bash(ANSWER=yes breeze *)
Bash(SKIP_BREEZE_SELF_UPGRADE_CHECK=1 breeze *)
Bash(SKIP_BREEZE_SELF_UPGRADE_CHECK=true breeze *)
Bash(SKIP_BREEZE_SELF_UPGRADE_CHECK= breeze *)
Bash(uvx *dev/breeze*)
Bash(uv run *dev/breeze*)
Bash(docker ps *)
Bash(docker info *)
```

## Why

`breeze` is the mandated entrypoint for all local development in Apache
Airflow — the contributor instructions forbid running `pytest` / `python` /
`airflow` directly on the host (see this repo's `AGENTS.md` / `CLAUDE.md`).
Without these allows, every breeze invocation prompts for Bash permission,
which is pure friction for a command contributors run constantly.

- The `uvx` / `uv run *dev/breeze*` forms cover the breeze shim that runs
  breeze from `dev/breeze` (see [ADR 0017](../dev/breeze/doc/adr/0017-use-uvx-to-run-breeze-from-local-sources.md)).
- The env-prefixed forms cover the common `ANSWER=yes` and
  `SKIP_BREEZE_SELF_UPGRADE_CHECK` wrappers used throughout the dev/CI scripts.

Only **non-destructive** commands are allowed: breeze itself (which drives
docker internally, so its docker usage is covered by the single breeze call)
plus read-only `docker ps` / `docker info`. No `docker rm`, no
`docker network prune`, nothing that writes outside the breeze workflow —
those stay prompt-gated.

## Scope notes

- These land in the gitignored, per-machine `.claude/settings.local.json`,
  applied when a contributor sets up the isolated build — there is no shared
  committed settings file to review or maintain.
- A contributor's own additional per-machine allows live in the same
  `settings.local.json`; this override only seeds the breeze baseline and
  leaves everything else untouched.
