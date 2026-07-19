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

# 1. The Dag File Processor runs untrusted user code, isolated from the metadata DB

Date: 2026-07-19

## Status

Accepted

## Context

The Dag File Processor (DFP) is the one core component whose job is to **import
and execute Dag-author Python**. To turn a Dag file into a serialized Dag it
must run the module top level — arbitrary author code, custom operators,
timetable construction, `params` builders — none of which Airflow controls or
trusts.

Airflow's security model (see the project security model doc) treats that author
code as untrusted. The scheduler avoids the problem by never importing it
(see the scheduler's own ADR on this). The DFP cannot avoid it — running the
code *is* its purpose — so the boundary is drawn a different way: the DFP is
**isolated from the server's privileged database session**. Each parse runs in a
subprocess launched with `_AIRFLOW_PROCESS_CONTEXT=client`, and its interactions
with the metadata database go through the Execution API — the same restricted,
authenticated surface workers use — via an in-process API server
(`InProcessExecutionAPI`) rather than a direct ORM session.

These are **software guards that steer** the DFP onto the client path; the
project security model is explicit that they do not defend against *intentional*
bypass by malicious or misconfigured code. That makes it all the more important
that changes here do not casually widen the DFP's database reach, because every
such widening is a new place author code sits next to server credentials.

The recurring pressure is convenience: parsing already has the file in front of
it, so "just read this one row from the DB here" or "callbacks need this context,
let's query it directly" look harmless locally. The decision below is what keeps
routing those through the API instead.

## Decision

The DFP runs user code, but the parsing process does not hold a direct,
privileged server database session. Concretely:

- Parse subprocesses run with `_AIRFLOW_PROCESS_CONTEXT=client` and reach the
  metadata DB **only through the Execution API** (`InProcessExecutionAPI`), never
  by opening an `airflow.models` ORM session for parsing-side work.
- Context that the DFP or its callbacks need at runtime is **fetched through the
  Execution API**, not by adding a metadata-DB query inside the isolated process.
- New Execution-API message types the DFP relies on are wired into its explicit
  handler / exclusion union in `processor.py`; the DFP consumes a restricted set
  of message types by design.

## Consequences

- The DFP's blast radius stays bounded: author code runs, but it is not handed
  the server's DB credentials or an unrestricted query surface.
- New DFP features that need data must express that need as an Execution-API
  call, which keeps the isolation seam visible and reviewable.
- There is friction — going through the API is more work than a direct query —
  and that friction is intentional.

A change **violates** this decision when, in `dag_processing/` code reachable
from the parse subprocess, it:

- opens a direct `airflow.models` / ORM session (or a raw DB connection) to read
  or write metadata during parsing, instead of using the Execution API;
- removes, weakens, or bypasses the `_AIRFLOW_PROCESS_CONTEXT=client` guard, or
  runs parsing under the server process context;
- fetches new callback/parse context by querying the metadata DB from the
  isolated process rather than through the Execution API;
- adds an Execution-API message type the DFP handles without adding it to the
  processor's handler/exclusion union (so the restricted surface silently grows).

A reviewer should reject any change that gives the parsing path a wider,
directer route to the metadata database than the Execution API.

## Evidence

- #66608 — "Fetch deadline callback context via Execution API at runtime":
  moved callback context retrieval onto the Execution API rather than a direct
  DB read from the processing side (later reverted in #68909 and reworked —
  showing how load-bearing and delicate this seam is).
- #68569 — "Defer `InProcessExecutionAPI` import": kept the in-process API as the
  DFP's DB path while deferring its import for worker-isolation/startup reasons.
- #67772 — "Fix exceptions of positional session use in airflow-core
  dag_processing": tightened session discipline in this module, consistent with
  keeping DB access explicit and controlled rather than incidental.
