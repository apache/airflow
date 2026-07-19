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

# 3. The load-bearing shared libraries must fail safe

Date: 2026-07-19

## Status

Accepted

## Context

The shared libraries are not equal in risk. A few of them run on *every* process,
on the hot path, and hold responsibilities where a subtle defect is a
security or availability incident rather than a bug in one feature:

- **`secrets_masker`** is the last line of defence against secrets reaching logs.
  It redacts known-sensitive values wherever they appear — including inside nested
  lists/dicts/tuples and after templates are rendered or truncated. If it *under*-
  masks, a credential leaks into logs that ship to operators and aggregators.
- **`logging` / `observability`** sit on every process's logging and metrics path.
  If they raise on a malformed log format, a missing field, or a metrics-emitter
  error, they can crash or wedge the host process — the scheduler, a worker, the
  triggerer — rather than the one line that was being logged.
- **`serialization`** primitives are round-tripped by different components running
  different versions; non-deterministic or version-fragile output corrupts what a
  consumer reads back.

Because these ship to every distribution at once (ADR 1), a regression here is not
contained — it degrades the whole deployment. The bias must therefore be toward
*failing safe*: mask more rather than less, degrade rather than crash, and keep
output stable across versions.

## Decision

Changes to the security- and reliability-critical shared libraries must preserve
their fail-safe posture:

- **`secrets_masker`**: redaction must keep walking nested/compound structures and
  surviving round-trips; a change may widen what is masked but must not narrow it
  or expose a value on a newly-added path.
- **`logging` / `observability`**: emitters stay defensive — a bad format, missing
  field, or downstream metrics error degrades that log/metric, it does not raise
  into and crash the host process; metric names stay sanitized.
- **`serialization`**: output stays deterministic and round-trips across consumer
  versions; a change must deserialize what an older or newer consumer serialized.

## Consequences

- A secret is far more likely to be over-masked than leaked; a logging/metrics
  edge case degrades observability rather than taking down a process; a serialized
  value read by a differently-versioned consumer still loads.
- Contributors pay some conservatism — masking broadly, guarding emitters, and
  keeping serialized shapes stable is more work than the happy path — and that
  conservatism is intentional for these libraries specifically.

A change **violates** this decision when it:

- narrows `secrets_masker` redaction, stops it from walking a nested/compound
  structure, or exposes a secret on a path it previously covered;
- lets `logging`/`observability` raise into the host process on a malformed format,
  a missing field, or a metrics-emitter error, or emits an unsanitized metric name;
- makes `serialization` output order- or environment-dependent, or breaks
  deserialization of a shape an older/newer consumer produced.

## Evidence

- #68624 — "Fix sensitive data leak in SparkSubmitOperator truncated templates" and
  #68422 — "Walk nested lists/tuples/sets in secrets masker key-name redaction":
  the masker must cover truncated and nested values, not let them slip through.
- #67122 — "Fix SecretsMasker merge round-trip for Kubernetes env vars": masking
  has to survive a round-trip rather than dropping coverage.
- #69402 — "Prevent scheduler crash when process/thread are missing from log
  format": a malformed/short log format must degrade, not crash the host.
- #68945 — "Remove stale metrics no longer emitted from the metrics registry" and
  #69202 — "Fix mypy failure on datadog timer wrapped by shared observability
  Timer": the observability layer stays consistent and defensive across consumers.
