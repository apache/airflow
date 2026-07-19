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

# 2. The task runs in a supervised subprocess and talks to the supervisor over a defined message protocol

Date: 2026-07-19

## Status

Accepted

## Context

Enforcing the isolation from ADR 1 requires a physical split: the untrusted task
code cannot run in the same process that holds the Execution-API credentials.
So the runtime is two processes. The **supervisor** (`supervisor.py`) forks a
child, which runs `task_runner.py` and executes the operator; the supervisor
keeps the token and the API client and never runs author code itself. The two
processes are separated by a socket, and everything the child needs from the
outside world — every DB read, state update, log line, heartbeat — crosses that
socket.

Because arbitrary user code sits on one end, that channel is a genuine protocol,
not an internal function call. It is length-prefixed (not line-based, so a task
printing a newline can't desync it), it has a defined request/response **message
union** in `comms.py`, and it is decoded by a `CommsDecoder` that has to survive
EOF at any point, out-of-order reads, transient network errors, force-closed
sockets, and a child that dies mid-message. It also carries signal and lifecycle
semantics: SIGTERM must warm-shut-down the running task rather than hard-kill it,
termination signals must be forwarded to the child, and on macOS the child is
started with `fork`+`exec` to avoid a `SIGSEGV`.

Crucially, this protocol is the *client half* of the Execution API. A new
supervisor↔runner interaction is not a private detail — it is one leg of an
end-to-end feature that also touches the Execution-API datamodels, routes, Cadwyn
version files, the SDK client, and the regenerated worker models, and it is
shared (via `InProcessExecutionAPI`) with the Dag processor and triggerer. The
history here is a long tail of subtle IPC bugs — missed EOF stranding
supervisors, decoder deadlocks, dropped terminal states — each of which shipped
because the protocol edge case wasn't obvious in the diff.

## Decision

The task executes in a supervisor-managed subprocess, and all supervisor↔runner
interaction goes through the defined `comms.py` message protocol — kept in sync
with the Execution-API server side. Concretely:

- The operator body runs only in the forked `task_runner.py` child; the
  supervisor forks/oversees it, owns the API client + token, and relays requests.
  No state crosses the boundary except as a `comms.py` message.
- Messages use the length-prefixed framing and the typed request/response union;
  new interactions are added to that union and handled in the supervisor, and
  wired end-to-end (datamodel → route → Cadwyn version → `comms.py` → SDK client
  → supervisor handler → regenerated `_generated.py` → per-version tests). A new
  message type is also added to the Dag-processor / triggerer handler-or-exclusion
  unions that share `InProcessExecutionAPI`.
- The IPC loop and lifecycle handling **fail closed**: EOF, transient errors, and
  a non-success terminal-state call must not be silently treated as success;
  signals warm-shut-down and are forwarded to the child; the macOS `fork`+`exec`
  startup is preserved.

## Consequences

- The isolation boundary is physical and auditable: because every cross-boundary
  need is a message, a reviewer can see exactly what authority the task process
  is exercising.
- Adding a runtime capability is more work — it is an end-to-end, versioned
  change, not a local method call — and that cost is the price of a protocol that
  two independently-deployed processes (and the Dag-processor / triggerer reuse)
  can rely on.
- The protocol's edge cases (framing, EOF, deadlock, signal timing) are
  load-bearing; changes to the loop need tests that drive the real socket, not an
  in-process shortcut.

A change **violates** this decision when, in this runtime, it:

- runs operator / author code in the supervisor process, or moves the
  Execution-API client/token into the task subprocess;
- passes state across the supervisor↔runner boundary by a side channel (shared
  globals, files, extra pipes) instead of a `comms.py` message;
- adds or changes a supervisor↔runner interaction without adding its type to the
  message union and the supervisor handler (and, where shared, the
  Dag-processor / triggerer handler-or-exclusion unions);
- reverts the length-prefixed framing to line-based, or handles EOF / transient
  IPC errors / a failed terminal-state call by treating the task as succeeded;
- hard-kills on SIGTERM instead of warm-shutting-down, drops signal forwarding to
  the child, or breaks the macOS `fork`+`exec` startup.

A reviewer should reject any change that smuggles work across the process
boundary outside the message protocol, or that lets an IPC edge case fail open.

## Evidence

- #51699 — "Switch the Supervisor/task process from line-based to
  length-prefixed": established the framing so task output can't desync the
  channel.
- #66573 — "Fail closed when supervisor IPC fails on a non-success terminal
  state" (with #66574, "Recover stuck TIs when direct terminal-state API call
  fails"): the loop must not treat an IPC/terminal-state failure as success.
- #48880 — "Change Trigger<->Supervisor communication to avoid stalls", #66572 —
  "Don't crash supervisor IPC loop on transient network errors", #64894 — "Fix
  read out-of-order issue with send method in CommsDecoder", #68377 — "Added
  deadlock detection in CommsDecoder", #67115 — "Fix ValueError when supervisor
  force-closes stuck sockets after timeout": the recurring IPC edge cases the
  protocol has to survive.
- #69034 — "Warm-shutdown supervisor on SIGTERM instead of killing the running
  task", #61627 — "Forward termination signals from supervisor to task
  subprocess", #64874 / #69008 — "Fix macOS `SIGSEGV` … via `fork`+`exec`": the
  signal and process-lifecycle semantics of the boundary.
- #66899 — "Enforce supervisor schema class name matches its `type` literal" and
  #67235 — "Add schema migration to supervisor-child comm": keep the protocol's
  message schemas versioned and internally consistent.
