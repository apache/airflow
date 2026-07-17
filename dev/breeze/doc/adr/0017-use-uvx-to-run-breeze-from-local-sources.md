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

- [17. Use `uvx` to run breeze from local sources](#17-use-uvx-to-run-breeze-from-local-sources)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 17. Use `uvx` to run breeze from local sources

Date: 2026-04-26

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

``uv`` ships a tool — ``uvx`` — that runs a command from a project directory in an
ephemeral, cached environment without installing anything globally. ``uvx --from
./dev/breeze breeze ...`` resolves dependencies once per ``pyproject.toml`` /``uv.lock``
hash, caches the resulting environment, and reuses it on subsequent calls. The first
call in a fresh worktree is slow (one resolve + install); every call after that is
fast.

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
# Runs breeze from the dev/breeze folder of the current git worktree via 'uvx',
# so each worktree (e.g. parallel agentic runs) gets its own ephemerally-installed
# breeze tied to that worktree's source.
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
    uvx --from "${breeze_root}/dev/breeze" --quiet breeze "$@"
```

``scripts/tools/setup_breeze`` writes this file (replacing any previous
``uv tool install`` of breeze) and marks it executable. The location
(``~/.local/bin``) matches where ``uv tool install`` would have created
``breeze``, so the file naturally lives on ``PATH`` for users who already had
the ``uv tool`` install working.

The user-facing command stays the same — they still type ``breeze`` — but each
invocation:

* resolves ``$(git rev-parse --show-toplevel)`` from the current working directory,
* dispatches to ``uvx --from <that-worktree>/dev/breeze breeze``,
* and therefore always runs the breeze code that belongs to that worktree.

Because the shim is a real file on ``PATH`` (not a shell function), it is also
visible to subprocesses — pre-commit hooks, CI scripts, dev tools, and anything
else that does ``subprocess.run(["breeze", ...])`` will pick it up exactly like
they picked up the old ``uv tool``-installed binary.

The two ``env`` variables matter: ``AIRFLOW_ROOT_PATH`` short-circuits breeze's
installation-source detection (which walks up from ``__file__`` and would
otherwise misfire because ``__file__`` lives inside the uvx cache, not the
source tree), and ``SKIP_BREEZE_SELF_UPGRADE_CHECK=1`` disables the "your
install is older than your sources" nag — moot under uvx, which auto-rebuilds
the env when ``pyproject.toml`` / ``uv.lock`` change.

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

* **First call in a new worktree is slow.** ``uvx`` has to resolve and install
  breeze's dependencies the first time it sees a given ``pyproject.toml`` /
  ``uv.lock``. Subsequent calls hit the cache and are fast.
* **Adds a small bash startup overhead.** The shim is a tiny bash script that
  runs ``git rev-parse`` and ``uvx`` for every invocation. Negligible at the
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
