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
and execute Dag-author Python** — running the module top level (custom operators,
timetables, `params` builders) is arbitrary code Airflow does not trust. The
scheduler avoids the problem by never importing author code; the DFP cannot, since
running it *is* its purpose. So the boundary is drawn differently: the DFP is
**isolated from the server's privileged database session**. Each parse runs in a
subprocess launched with `_AIRFLOW_PROCESS_CONTEXT=client`, and its DB interactions
go through the Execution API — the same restricted surface workers use — via an
in-process API server (`InProcessExecutionAPI`), not a direct ORM session.

These are **software guards that steer** the DFP onto the client path; the security
model is explicit they do not defend against *intentional* bypass. So changes here
must not casually widen the DFP's database reach — every widening is a new place
author code sits next to server credentials, and the recurring pressure is
convenience ("just read this one row here").

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

- #66608 — moved deadline callback context onto the Execution API, not a direct DB read (reverted in #68909 and reworked — a delicate seam).
- #68569 — kept `InProcessExecutionAPI` as the DFP's DB path while deferring its import.
- #67772 — tightened positional-session discipline in this module.
