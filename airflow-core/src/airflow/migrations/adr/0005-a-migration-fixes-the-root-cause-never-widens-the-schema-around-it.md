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

When a value does not fit a column, the smallest possible patch is to make the
column bigger. When a table is in the way, the smallest possible patch is to drop
it. Both arrive here regularly, and both are refused for the same reason: the
migration is permanent (ADR 0001) while the problem it papers over is not
understood.

The recurring shapes are concrete:

- A column overflows because something is being stored in it that should never
  have been stored at all. Widening the column makes the overflow stop and freezes
  the wrong design into the schema — including its security properties, when the
  value is a credential-bearing URL that also expires within hours and is therefore
  useless by the time it is read back.
- A length limit is raised to a new number that is just as arbitrary as the old
  one. The value is produced by an external system whose bounds Airflow does not
  control, so the next report is inevitable; the real question — should this be a
  bounded identifier at all, or unbounded text that participates in no join or key
  — goes unasked.
- A model or table is dropped because one consumer of it was believed to be the
  only one. The reviewer who knows the wider system points out the other consumers,
  and the correct change turns out to be at the source of the behaviour, several
  layers away from the schema.

In each case the schema change was the *first* idea rather than the conclusion of
a diagnosis, and the schema is the worst place in this codebase to record a guess:
a column that ships wide cannot be narrowed again without a new revision that must
cope with the rows written in the meantime, and a table that ships dropped takes
its data with it.

The corollary is that a migration is judged against the design it encodes, not
only against its DDL correctness. A reviewer can confirm that a widening runs
cleanly on all three backends and still be obliged to refuse it.

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
  is produced and that is a wider conversation than a two-line migration.
- Contributors who arrive with a widening and leave with a redesign sometimes leave
  the work unfinished. That is an accepted cost: an unfinished redesign is
  recoverable, a released migration is not.

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

- #62224 — a `String(200)` column widened to `Text` so a signed bundle URL would
  fit: refused because signed URLs should not be persisted at all — they are
  sensitive and expire within hours — and because the PR title described the DDL
  rather than the reason. Closed by the author in favour of the wider bundle
  redesign.
- #59813 — raising the `external_executor_id` length: reviewers questioned both the
  missing linked issue and the new ceiling itself, noting that the value is set by
  external executors and that a column used in no join or key has no reason to carry
  a bound at all.
- #69520 — "Drop the LogTemplate DB model": declined because the model is used well
  beyond the one consumer assumed — it generates the log-file suffix passed to
  workers and is needed to read historical task logs back from remote storage. The
  author reoriented onto the root cause in a separate PR.
- #54511 — schema hardening for concurrent `rendered_task_instance_fields`
  insertions, withdrawn once it became clear the failure was a single report rather
  than a systemic defect.
- #53820 — "Serialize Dags before making `TI.dag_version_id` non-nullable": a
  data-shape problem where deleting the serialized rows was considered and rejected
  because it would lose real run history; closed in favour of the approach taken in
  #54366.
