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

# 2. Each Dag file is parsed in an isolated, timeout-bounded subprocess

Date: 2026-07-19

## Status

Accepted

## Context

A single `DagFileProcessorManager` keeps an entire deployment's serialized Dags
fresh, running a parse per Dag file that imports untrusted author code (see ADR 1).
Author code can hang, loop, leak, segfault a C extension, or crash. If parsing ran
**inside** the manager process, any one of those in any one file would take down
parsing for the whole deployment as a silent stall. The manager loop is also where
DB-lock contention, unbounded scans, or O(N²) bookkeeping quietly become "the
processor stopped making progress."

So the manager stays thin and delegates the dangerous work: a separate,
short-lived, timeout-bounded subprocess per file, treated as expendable. Platform
quirks matter — on macOS a bare `fork` without `exec` can `SIGSEGV`, so the launch
uses `fork+exec` — and the manager loop must stay responsive at all times.

## Decision

Dag parsing runs in isolated, timeout-bounded subprocesses managed by
`DagFileProcessorManager`; the manager process itself never imports author code
and never blocks on a single file. Specifically:

- One parse runs per subprocess, launched via `fork+exec`, bounded by a timeout;
  a hang, crash, or segfault in one file is contained to that file.
- The manager loop stays responsive: per-file/per-handler failures are guarded so
  one bad file (or a stale file handle) degrades that file, not the whole
  processor.
- Work done in the loop itself is bounded and cheap — no unbounded or O(N²)
  scanning, no blocking DB call that can stall progress under lock contention.

## Consequences

- One hostile or broken Dag file cannot stop the deployment's parsing; it fails
  its own slot and the manager moves on.
- The manager loop is a latency-sensitive hot path: anything added to it is paid
  on every cycle, so new per-file work belongs in the subprocess, and new
  bookkeeping must be bounded and indexed.
- Cross-platform process launch is part of the contract, not an incidental
  detail — changing how subprocesses are started must preserve the isolation and
  the platform-safety it buys.

A change **violates** this decision when it:

- moves Dag parsing (import of author files) into the manager process, or shares
  a long-lived interpreter across files instead of an expendable subprocess;
- lets a single file block the manager loop — an unbounded wait, a missing
  timeout, or a blocking call on the hot path;
- introduces unbounded or O(N²) work per cycle, or a DB access that can stall the
  loop under lock contention;
- lets a per-file/handler exception propagate far enough to crash the manager
  instead of degrading just that file;
- changes subprocess launch in a way that reintroduces the platform crash modes
  the `fork+exec` approach avoids.

## Evidence

- #69008 — macOS `SIGSEGV` fixed via `fork+exec`: the launch mechanism is load-bearing for isolation.
- #68118 — silent hang under DB lock contention: exactly the failure this guards against.
- #69324 — a stale file handler must degrade, not crash the manager.
- #67750 — file-queue dedup reduced O(N²)→O(N) to keep per-cycle bookkeeping bounded.
- #69452 — trimming per-file/per-run work on the hot path.
