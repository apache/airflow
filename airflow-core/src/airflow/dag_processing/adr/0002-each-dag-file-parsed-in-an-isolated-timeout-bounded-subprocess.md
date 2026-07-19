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

A single `DagFileProcessorManager` is responsible for keeping the serialized
Dags of an entire deployment fresh. It walks the set of Dag files, and for each
one runs a parse that imports untrusted author code (see ADR 1). Author code can
do anything: hang on a network call, spin in a loop, leak memory, segfault a C
extension, or crash outright.

If parsing ran **inside** the manager process, any one of those failure modes in
any one Dag file would take down parsing for the whole deployment — new Dag
versions would stop appearing, and the failure would present as a silent stall,
not a clean error. The manager loop is also the place where DB-lock contention,
unbounded scans, or O(N²) bookkeeping quietly convert into "the processor stopped
making progress," which operators experience as Dags mysteriously not updating.

So the manager keeps itself thin and delegates the dangerous work: it launches a
separate, short-lived subprocess per file, bounds it with a timeout, and treats
that subprocess as expendable. Platform quirks matter here too — on macOS a bare
`fork` without `exec` can `SIGSEGV`, so the launch uses `fork+exec`. The manager's
own loop must stay responsive at all times.

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

- #69008 — "Fix macOS `SIGSEGV` in triggerer and DAG processor via `fork+exec`":
  the launch mechanism itself is load-bearing for process isolation safety.
- #68118 — "Fix DagFileProcessorManager silent hang on DB lock contention":
  a loop that stalled under a DB lock is exactly the failure this decision guards
  against.
- #69324 — "Make sure stale file handler doesn't crash the DagFileProcessorManager":
  a per-handler failure must degrade, not kill the manager.
- #67750 — "Dag processor: reduce file-queue dedup from O(N²) to O(N) with
  OrderedDict": keeping per-cycle bookkeeping bounded so the loop stays
  responsive at scale.
- #69452 — "Reduce Dag processor log noise from per-Dag run lookups": trimming
  per-file/per-run work on the hot path.
