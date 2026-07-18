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

# 2. New executor behaviour is gated behind explicit version guards, not bare try/except

Date: 2026-07-18

## Status

Accepted

## Context

Because provider executors version-mix with core (see ADR 0001), a provider that
wants to use a capability that only exists in a newer core must be able to run on
an older core that lacks it — and the reverse. Provider executors carry
back-compatibility shims for exactly this: the ECS and Celery executors, for
example, branch on whether the running core is new enough before calling a
newer-core API.

There are two ways to write such a branch:

1. A **bare `try/except ImportError` / `except AttributeError`** around the new
   call, falling back on failure.
2. An **explicit version guard** — a boolean like `AIRFLOW_V_3_1_PLUS` derived
   from the installed Airflow version — with the new path under the `True` branch
   and the legacy path under the `False` branch.

The bare `try/except` form is deceptively appealing but corrosive: it swallows
unrelated failures (a real `ImportError` deeper in the new path gets treated as
"old core"), it hides *which* version the shim targets, and — most importantly —
it is impossible to grep for and remove cleanly once the minimum supported
version rises. A shim that cannot be found is a shim that lives forever.

An explicit, named version constant makes each shim self-documenting: it states
the exact minimum version at which the legacy branch becomes dead code, so it can
be deleted mechanically when core drops support for that version.

## Decision

- Gate any new cross-version executor behaviour behind an **explicit
  `AIRFLOW_V_3_x_PLUS`-style version guard**, following the existing ECS / Celery
  back-compat pattern. Do not use a bare `try/except` as a stand-in for a version
  check.
- The **code guard and its explanatory comment must name the same version.** If
  the guard is `AIRFLOW_V_3_1_PLUS`, the comment must say the shim is removable
  when core's minimum supported version reaches 3.1 — not a different number.
- Each guard marks a removable seam: the legacy branch is dead code once the
  minimum supported Airflow version is at or above the guarded version, and is
  deleted then.
- `try/except ImportError` remains legitimate only for genuinely optional
  dependencies, not as a proxy for "is core new enough".

## Consequences

- Every version shim is greppable by its constant and carries an unambiguous
  removal trigger.
- Reviewers can verify at a glance that the guard, the comment, and the intended
  minimum version all agree.
- Removing shims at a version bump is a mechanical, low-risk sweep rather than an
  archaeology exercise.

**A violating change looks like:** wrapping a newer-core executor call in a bare
`try/except ImportError: <legacy path>` instead of an explicit
`AIRFLOW_V_3_x_PLUS` guard; or writing `if AIRFLOW_V_3_2_PLUS:` while the adjacent
comment claims the shim is for 3.1 (guard/comment version mismatch). Both are
rejected because they make the shim unfindable or misleading at removal time.

## Evidence

- #56187 — Move the traces and metrics code under a common observability package (cross-version import seam handled with an explicit guard).
- #65277 — Clean up redundant api server uri generation (removal of a shimmed cross-version path at a known version).
- #67449 — Kubernetes executor callback support (new provider-executor behaviour gated for version-mix compatibility).
