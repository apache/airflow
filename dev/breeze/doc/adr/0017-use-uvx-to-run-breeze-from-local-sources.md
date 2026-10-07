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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [17. Run breeze from the current worktree's locked sources](#17-run-breeze-from-the-current-worktrees-locked-sources)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 17. Run breeze from the current worktree's locked sources

Date: 2026-04-26 (amended 2026-08-26: dispatch moved from ``uvx`` to ``uv run --locked``)

## Status

Accepted

Supersedes [16. Use uv tool to install breeze](0016-use-uv-tool-to-install-breeze.md)

## Context

ADR 0016 recommended installing breeze once globally with ``uv tool install -e ./dev/breeze``.
That model assumes a single working copy of Airflow per machine: the editable install points
at one specific ``dev/breeze`` folder, and the resulting ``breeze`` binary on ``PATH`` is shared
by every shell, every directory, every checkout.

Two patterns have made that single-install model awkward:

1. **Multiple checkouts / git worktrees.** Maintainers and contributors increasingly keep more
   than one working copy of Airflow open at the same time — separate clones for parallel
   feature work, ``v3-1-test`` backports, release verification, or just a clean tree to
   reproduce a bug. Each worktree may have a different version of breeze itself (different
   dependencies, different commands, different bugfixes). With a single ``uv tool`` install,
   only one of those worktrees is "live"; calling ``breeze`` from any other worktree silently
   runs the wrong code, and switching requires a ``uv tool install --force`` round-trip that
   breaks the other worktree.

2. **Agentic workflows.** Coding agents (Claude Code, Cursor, etc.) routinely create
   short-lived git worktrees so multiple agents can work in parallel without stepping on
   each other's branches. Those worktrees are created and destroyed automatically, and
   each one needs its own working ``breeze`` immediately, without a manual reinstall step.
   A single global install actively breaks this: agents in different worktrees fight over
   the same ``~/.local/bin/breeze`` symlink, and an agent that does ``uv tool install
   --force`` to "fix" itself silently sabotages every other worktree on the machine.

``uv`` ships a way to run a command from a project directory without installing anything
globally: ``uv run --project ./dev/breeze --locked breeze ...`` syncs that project's own
environment (``dev/breeze/.venv``) to exactly what ``dev/breeze/uv.lock`` pins, then runs the
command in it. The first call in a fresh worktree pays for the sync; every call after that
reuses the environment.

The ``--locked`` part matters as much as the per-worktree part. The two alternatives that
install from a path — ``uvx --from ./dev/breeze`` and ``uv tool install -e ./dev/breeze`` —
re-resolve breeze's requirements against the package index on every fresh environment and read
neither ``dev/breeze/uv.lock`` nor the ``[tool.uv] exclude-newer`` buffer declared next to it
(that setting governs project operations such as ``uv lock`` and ``uv sync`` only). Under those,
the committed lock recorded nothing and an unrelated upstream release could change breeze with
no commit in this repository. click 8.5.0 showed the cost: released on 2026-08-26, it added a
``help`` field to ``click.Argument.to_info_dict()`` — the dict breeze hashes to detect command
drift — so within the hour every CI job resolved the new click, every command taking a
positional argument hashed differently from its committed value, and static checks went red on
every open PR regardless of what it touched. The lock said click 8.4.2 throughout.

That gives us a way to make ``breeze`` always run from the *current* worktree's source
without ever touching a shared global install — but the dispatch mechanism has to be
something subprocesses can see. A shell function would not do: the codebase has many
sites (``scripts/ci/prek/breeze_cmd_line.py``, CI scripts, dev tools) that invoke
``breeze`` via ``subprocess.run(["breeze", ...])``, and subprocesses do not inherit
shell functions. The dispatcher has to be a real file on ``PATH``.

## Decision

The recommended way to run breeze is via a small **shim script** at
``~/.local/bin/breeze``, which delegates to ``uvx`` against the current git worktree:

```shell
#!/usr/bin/env bash
# Apache Airflow breeze shim — managed by scripts/tools/setup_breeze (ADR 0017).
# Runs breeze from the dev/breeze folder of the current git worktree via 'uv run',
# so each worktree (e.g. parallel agentic runs) gets its own environment tied to
# that worktree's source, with dependencies resolved from dev/breeze/uv.lock.
#
# Resolution order for the Airflow sources breeze runs from:
#   1. the current git worktree (per-worktree isolation — see above);
#   2. $AIRFLOW_REPO_ROOT, if exported and pointing at an Airflow worktree — the
#      release docs export this, so breeze resolves the same way across every
#      release process regardless of where the shim was installed from;
#   3. the install-time fallback baked in below (the worktree setup_breeze ran from).
# Steps 2 and 3 apply only when the current directory is not an Airflow worktree,
# so the fallbacks never override a real worktree and isolation is preserved.
set -e
# Install-time fallback: the Airflow sources 'scripts/tools/setup_breeze' was run
# from. Used only when the current directory is not an Airflow worktree.
fallback_root="/abs/path/to/airflow"   # baked in by setup_breeze (= AIRFLOW_SOURCES)
repo_root=$(git rev-parse --show-toplevel 2>/dev/null) || repo_root=""
if [ -n "${repo_root}" ] && [ -d "${repo_root}/dev/breeze" ]; then
    breeze_root="${repo_root}"
elif [ -n "${AIRFLOW_REPO_ROOT:-}" ] && [ -d "${AIRFLOW_REPO_ROOT}/dev/breeze" ]; then
    breeze_root="${AIRFLOW_REPO_ROOT}"
elif [ -d "${fallback_root}/dev/breeze" ]; then
    breeze_root="${fallback_root}"
else
    echo "breeze: not inside an Airflow worktree, AIRFLOW_REPO_ROOT is unset or not an Airflow worktree, and the install-time fallback '${fallback_root}/dev/breeze' is missing — re-run scripts/tools/setup_breeze" >&2
    exit 1
fi
exec env AIRFLOW_ROOT_PATH="${breeze_root}" SKIP_BREEZE_SELF_UPGRADE_CHECK=1 \
    uv run --project "${breeze_root}/dev/breeze" --locked --quiet breeze "$@"
```

``scripts/tools/setup_breeze`` writes this file (replacing any previous
``uv tool install`` of breeze) and marks it executable. The location
(``~/.local/bin``) matches where ``uv tool install`` would have created
``breeze``, so the file naturally lives on ``PATH`` for users who already had
the ``uv tool`` install working.

The user-facing command stays the same — they still type ``breeze`` — but each
invocation:

* resolves ``$(git rev-parse --show-toplevel)`` from the current working directory,
* dispatches to ``uv run --project <that-worktree>/dev/breeze --locked breeze``,
* and therefore always runs the breeze code that belongs to that worktree, with the
  dependencies that worktree's ``uv.lock`` pins.

Because the shim is a real file on ``PATH`` (not a shell function), it is also
visible to subprocesses — pre-commit hooks, CI scripts, dev tools, and anything
else that does ``subprocess.run(["breeze", ...])`` will pick it up exactly like
they picked up the old ``uv tool``-installed binary.

The two ``env`` variables matter: ``AIRFLOW_ROOT_PATH`` short-circuits breeze's
installation-source detection (which walks up from ``__file__`` and would otherwise
misfire on installs that do not live in the source tree), and
``SKIP_BREEZE_SELF_UPGRADE_CHECK=1`` disables the "your install is older than your
sources" nag — moot here, since ``uv run`` re-syncs the environment whenever
``pyproject.toml`` / ``uv.lock`` change and installs the sources as editable.

CI installs breeze the same way: ``scripts/ci/install_breeze.sh`` runs
``uv sync --project ./dev/breeze/ --locked`` and puts ``dev/breeze/.venv/bin`` on ``PATH``
rather than installing a global ``uv tool``. Dependency upgrades reach breeze only through a
change to ``dev/breeze/uv.lock`` — in practice the scheduled ``breeze ci upgrade`` PR, which
regenerates the lock and the command-output files together, as one reviewable commit.

``uv tool install -e ./dev/breeze`` and ``pipx install -e ./dev/breeze`` remain
supported as alternatives for users who explicitly want the old single-install
behaviour, but they are no longer the recommended path.

## Consequences

**Wins**

* **Per-worktree isolation.** Each git worktree (and each clone) gets its own
  breeze, transparently. No more ``uv tool install --force`` ping-pong when
  switching between trees, and agents working in parallel worktrees never
  clobber each other.
* **No stale installs.** The breeze that runs is always the breeze that's
  checked out — not whatever was current the last time someone reinstalled.
  The "your installed breeze is older than your sources" warning class largely
  goes away.
* **Reproducible dependencies.** Two checkouts of the same commit run breeze with the
  same dependency versions, whatever the index served that day, so the command hashes
  under ``dev/breeze/doc/images/`` are a property of the repository rather than of the
  calendar — and the ``exclude-newer`` buffer around lock upgrades finally applies.
* **Cheap setup in fresh worktrees.** Spinning up a new worktree (manually or
  via an agent) needs no extra install step; ``breeze`` works the moment
  ``cd`` lands in the tree.
* **Subprocess-safe.** The shim is a real binary on ``PATH``, so anything that
  shells out to ``breeze`` — pre-commit hooks, CI helpers, dev scripts —
  resolves it exactly like a ``uv tool`` install did.
* **Self-detecting staleness.** The shim carries a ``# breeze-shim-version: N``
  marker that ``setup_breeze`` bumps whenever the shim body changes. On startup
  breeze compares the installed shim's version against the version the current
  sources would install and warns the user to re-run ``setup_breeze`` if the
  installed shim is older (or predates versioning). The same startup check also
  detects a leftover legacy global ``uv tool`` / ``pipx`` install and nudges the
  user to migrate to the shim.

**Costs**

* **First call in a new worktree is slow.** ``uv run`` has to populate
  ``dev/breeze/.venv`` (~275 MB, mostly hardlinked into the uv cache; ignored by both
  ``.gitignore`` and ``.dockerignore``) the first time. Subsequent calls reuse it.
* **A stale lock blocks breeze.** Editing ``dev/breeze/pyproject.toml`` without re-running
  ``uv lock`` makes every breeze call fail until the lock is refreshed. The error names the
  fix, and the alternative — silently running dependencies nobody recorded — is the failure
  mode this dispatch removes.
* **Adds a small bash startup overhead.** The shim is a tiny bash script that
  runs ``git rev-parse`` and ``uv run`` for every invocation. Negligible at the
  command line, but noticeable inside tight loops or shell completion that
  re-invokes ``breeze`` many times.
* **Resolution is current-worktree-first, with two fallbacks.** ``breeze``
  invoked from inside an Airflow worktree runs that worktree's breeze. Invoked
  from anywhere else (a non-Airflow git tree, or no git tree at all — e.g. an
  ``asf-dist`` SVN release checkout), it falls back to, in order: the worktree
  pointed at by ``$AIRFLOW_REPO_ROOT`` (which the release docs export to the
  repo root, so breeze resolves the same way across every release process), then
  the ``dev/breeze`` of the worktree ``setup_breeze`` was last run from, baked
  into the shim at install time. This keeps release commands such as
  ``breeze release-management clean-old-provider-artifacts --directory <asf-dist>``
  working from the SVN tree. Only if the current worktree, ``$AIRFLOW_REPO_ROOT``,
  and the baked-in fallback are all missing ``dev/breeze`` does the shim error
  out with a clear message. The fallbacks never override a real worktree, so
  per-worktree isolation is preserved wherever it matters.
* **One-time migration.** Users who previously installed breeze with
  ``uv tool install`` need to ``uv tool uninstall apache-airflow-breeze``
  before installing the shim, otherwise both write to ``~/.local/bin/breeze``
  and conflict. ``scripts/tools/setup_breeze`` detects the legacy install and
  refuses to proceed until it is removed.
