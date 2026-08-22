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

# 5. A migration fixes the root cause; it never widens the schema around it

Date: 2026-07-20

## Status

Accepted

## Context

When a value does not fit a column, the smallest patch is to make the column
bigger; when a table is in the way, the smallest patch is to drop it. Both arrive
here regularly and both are refused for the same reason: the migration is
permanent (ADR 0001) while the problem it papers over is not understood. The
recurring shapes are concrete: a column overflows because something is stored in
it that should never have been stored — widening freezes the wrong design into the
schema, including its security properties when the value is a credential-bearing
URL that expires within hours; a length limit is raised to a new number just as
arbitrary as the old one, when the real question is whether the value should be a
bounded identifier at all; a table is dropped on the assumption one consumer is the
only one, when a reviewer who knows the wider system points out the others and the
correct fix is several layers away at the source.

In each case the schema change was the *first* idea rather than the conclusion of a
diagnosis, and the schema is the worst place to record a guess: a column that ships
wide cannot be narrowed without a new revision coping with the rows written
meanwhile, and a table that ships dropped takes its data with it. So a migration is
judged against the design it encodes, not only its DDL correctness — a widening can
round-trip cleanly on all three backends and still be refused.

## Decision

**A revision under `versions/` records a decision that has already been made
elsewhere. It is never the vehicle for discovering one.**

- **State the root cause in the PR, then show that the schema is where it lives.**
  If the fix could plausibly be made in the code that writes the column, it belongs
  there.
- **Do not widen a column to accommodate data that should not be persisted.**
  Sensitive, short-lived, or externally-controlled values are removed from the
  write path, not accommodated by a bigger type.
- **Do not raise a limit to another arbitrary number.** Either justify the new bound
  from the producing system's actual contract, or change the type to one where no
  bound is implied — and say why the column takes part in no index, join, or key if
  you make it unbounded text.
- **Do not drop a table or model without enumerating every consumer**, including
  the ones outside the metadata database — file-path templates, log readers, remote
  log backends, and anything else that reads the value indirectly.
- **A schema change never lands as a workaround.** If a genuine workaround is
  unavoidable, the underlying fix gets a tracking issue linked from the revision, per
  the repository's deferred-work rule.

## Consequences

- The schema stays a statement of the data model rather than a record of past
  incidents.
- Some legitimate fixes take longer, because the discussion moves to where the value
  is produced. Contributors who arrive with a widening and leave with a redesign
  sometimes leave the work unfinished — an accepted cost, since an unfinished
  redesign is recoverable and a released migration is not.

A change **violates** this decision when it:

- alters a column's length or type to make an observed value fit, without
  establishing why that value is stored there at all;
- raises a bound to a number that is not derived from the producing system's
  contract;
- converts a column to unbounded text without stating that it participates in no
  index, join, or key;
- drops a table, column, or model on the assumption that one known consumer is the
  only consumer;
- describes itself in the PR as a workaround, mitigation, or stop-gap without a
  linked tracking issue for the real fix.

## Evidence

- #62224 — a `String(200)` column widened to `Text` for a signed bundle URL: refused because signed URLs should not be persisted (sensitive, expire within hours) and the title described the DDL not the reason. Closed for the wider bundle redesign.
- #59813 — raising `external_executor_id` length: reviewers questioned the missing linked issue and the new ceiling, noting the value is set by external executors and a column used in no join or key has no reason to carry a bound.
- #69520 — "Drop the LogTemplate DB model": declined — the model generates the log-file suffix passed to workers and is needed to read historical logs from remote storage. Author reoriented onto the root cause in a separate PR.
- #54511 — schema hardening for concurrent `rendered_task_instance_fields` insertions, withdrawn once the failure proved a single report rather than a systemic defect.
- #53820 — "Serialize Dags before making `TI.dag_version_id` non-nullable": deleting the serialized rows was rejected because it would lose real run history; closed in favour of #54366.
