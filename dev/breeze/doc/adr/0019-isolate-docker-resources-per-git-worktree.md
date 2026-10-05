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

- [19. Isolate Docker resources per git worktree](#19-isolate-docker-resources-per-git-worktree)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 19. Isolate Docker resources per git worktree

Date: 2026-10-05

## Status

Accepted

Builds on [17. Run breeze from the current worktree's locked sources](0017-use-uvx-to-run-breeze-from-local-sources.md)

## Context

ADR 0017 made each git worktree run its own Breeze. The Docker resources Breeze creates were still
shared: every checkout used the ``breeze`` Compose project, the same containers and the same database
volume. Two worktrees running ``breeze testing`` at the same time stopped each other's containers
and wrote into the same database.

Worktrees are also short-lived. Coding agents (``claude -w`` and similar) create a worktree for a
task, start Breeze to run tests in it, and delete it when the task is done, often without anyone
running ``breeze down`` first. Breeze keeps its containers and volumes around between commands to
save start-up time, so every deleted worktree used to leave running containers (CPU, memory) and
volumes (disk) behind, with nothing pointing back to them.

Git has no hook for worktree removal. Agent-specific hooks are not a general mechanism either, and the
ones that exist are unsuitable (a removal hook replaces git's own removal, and a session-end hook has
a time budget of about a second), so cleanup has to work without them.

## Decision

Each linked git worktree gets its own Docker Compose project, and Breeze removes the project's
resources once the worktree is deleted.

### Ownership is recorded in labels

Every container, volume and network that Breeze's Compose files create carries:

* ``org.apache.airflow.breeze=true`` - Breeze owns the resource;
* ``org.apache.airflow.breeze.worktree=<absolute path>`` - the isolated worktree that owns it (empty
  for the main checkout and for shared resources);
* ``org.apache.airflow.breeze.host=<host name>`` - the machine whose filesystem decides whether that
  path still exists.

Labels are applied when Docker creates a resource, so a resource is marked for cleanup from the
moment it exists, and Docker can be queried by label without parsing project names. A registry file
listing worktrees was considered and rejected: it can drift from what Docker actually holds, and it
needs its own locking and cleanup.

### Naming

The default Compose project of an isolated worktree is ``breeze-<directory name>-<path hash>``,
where the hash is the first six hex digits of the SHA-256 of the worktree's absolute path. The
directory name keeps the project recognisable in ``docker ps``; the hash keeps worktrees with the
same directory name in different locations or clones apart. The main checkout keeps ``breeze``.
Explicit ``--project-name`` values are used as given.

### What stays shared

Some resources are deliberately shared between worktrees, because they are expensive to rebuild and
safe to share:

* the CI and PROD images, which are keyed by Python version and branch rather than by checkout;
* the MyPy cache volume;
* host ports. Forwarded ports are fixed, so two worktrees cannot both forward them at the same time;
  ``breeze testing`` does not forward ports, so concurrent test runs are not affected.

Other resources that still have fixed names (for example the ``breeze-docs`` and ``breeze-db``
projects, fixed ``container_name`` values in integration Compose files, kind cluster names and the
bytecode cache volume) are scoped per worktree in a follow-up change.

### Cleanup triggers

* **Watcher.** The first Docker-backed Breeze command in an isolated worktree starts a detached
  watcher process. It checks the worktree directory every 30 seconds and, once the directory is
  gone, removes the containers, volumes and networks labelled with that worktree. It uses only the
  standard library and imports everything when it starts, because deleting the worktree can delete
  the virtualenv it runs from. A file lock in the worktree's ``.build`` directory keeps one watcher
  per worktree. It exits after an hour without any of the worktree's containers; the next
  Docker-backed command starts it again.
* **Sweep before Docker commands.** Before a Docker-backed command runs, Breeze removes resources
  whose worktree no longer exists. This covers watchers that were not running (for example after a
  reboot) and volumes left behind once the containers were gone.
* **Explicit commands.** ``breeze down`` removes the current checkout's projects and resources of
  deleted worktrees, and lists other worktrees that still hold resources. ``--all-worktrees`` removes
  everything Breeze owns, and ``breeze cleanup`` removes containers of deleted worktrees and unused
  Breeze volumes.

### Safety rules

* **Missing means not found.** A worktree counts as deleted only when its path does not exist. Any
  other error while checking the path keeps the resources.
* **Host scoping.** Resources labelled with another host are never treated as stale, because that
  host's paths cannot be checked here. This keeps a Docker daemon shared between machines from losing
  another machine's resources. Resources created before the host label existed have no host and are
  checked against the local filesystem.
* **Best effort.** The automatic sweep and the watcher never fail the command that triggered them;
  Docker errors produce a warning, and the next Docker-backed command retries.
* **Not in CI.** The watcher is not started in CI or with ``--dry-run``.

### Opt-out

Isolation is enabled by default. ``breeze setup config --no-worktree-isolation`` disables it for
every worktree of the checkout; the setting is stored in the main checkout's ``.build`` directory so
that new worktrees pick it up. The ``BREEZE_WORKTREE_ISOLATION`` environment variable (``true`` or
``false``) overrides it, for example for one shell. With isolation disabled, worktrees use the shared
``breeze`` project and their resources are not tied to the worktree.

## Consequences

* Worktrees can run Breeze, including database-backed tests, at the same time without interfering.
* Deleting a worktree releases its containers within about 30 seconds while the watcher runs, and in
  any case on the next Docker-backed Breeze command, without anyone running ``breeze down``.
* Each isolated worktree pays for its own database volume and container start-up.
* Adding the path hash renamed existing worktree projects, so a worktree that already had a database
  volume starts with a fresh one once. ``breeze down`` in that worktree still removes the old project,
  because it is selected by its worktree label.
* Compose files must keep the three labels on every new service and volume they define.
