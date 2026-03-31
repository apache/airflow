<!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- markdownlint-disable MD022 -->
---
name: stage-changes
description: Stage changed files for commit using git add. This is a host-only operation — it must never run inside a Breeze container.
---
<!-- markdownlint-enable MD022 -->

Stage Changes
=============

Stage specific files or directories before running static checks or committing.

Source of truth for contribution workflow order is `contributing-docs`; this skill
enforces the host/container guardrails when applying that workflow.

Host-Only Guardrail
-------------------

This skill operates on the **host** working tree. Running `git` commands inside a
Breeze container will silently operate on container-local paths that differ from your
actual working tree.

**Before running:** Confirm you are on the host by checking:

- `/.dockerenv` does **not** exist
- `/opt/airflow` does **not** exist as a directory
- `BREEZE_HOME` environment variable is **not** set

If any of those are true, exit Breeze first (`exit` or `Ctrl-D`), then stage changes.

Commands
--------

```bash
# Stage a single file
git add path/to/file.py

# Stage multiple files
git add path/to/file1.py path/to/file2.py

# Stage all changes in a directory
git add providers/amazon/

# Stage all tracked changes
git add -u
```

Workflow Context
----------------

This is **Scenario 1, Step 1** of the standard Airflow contributor workflow:

1. → **stage-changes** (this skill)
2. run-static-checks
3. run-unit-tests

Always stage changes *before* running `prek` (pre-commit) so the hooks operate on
the correct set of files.

Prerequisites
-------------

- You must be on the **host** (not inside Breeze). See guardrail above.
- Files must already exist and be tracked by git (or explicitly added with `git add`).

Success Criteria
----------------

`git status` shows staged changes under "Changes to be committed".
