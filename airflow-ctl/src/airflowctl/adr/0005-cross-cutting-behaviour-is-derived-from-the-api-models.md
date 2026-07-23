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

# 5. Cross-cutting command behaviour is derived from the API models, not written per command

Date: 2026-07-20

## Status

Accepted

## Context

A missing required argument, a malformed input file, a server rejection — these are
not per-command problems. Every command that posts a body has required fields;
every command reading a file can be handed a broken one; every command can get a
4xx. The generated datamodels already describe which fields are required, because
the server's OpenAPI spec says so and `datamodel-codegen` carries it into the
Pydantic models. So the behaviour can be derived once from those models — validate
against `is_required()`, map the failure to a message and exit code, and every
present and future command inherits it — or written into handlers one
`if not args.file: …` at a time.

Review has repeatedly chosen the first, for specific reasons: per-command checks
cover only the commands someone got around to (the gap is invisible — one command
gives a stack trace, its neighbour a clean message), they drift as the spec
changes, and for generated commands they are lost at every regeneration. A PR
adding JSON-structure validation to variables-import was closed for the generic
solution; a `dag_id` validation for pause/unpause drew the response that it is
needed for all commands, generated from the same place — the generic
implementation deriving it from `is_required()` landed in #63388. The limit
matters too: a generic wrapper over operation calls was closed because, near a
release with no concrete behaviour riding on it, the abstraction bought nothing.
The rule applies to *behaviour that demonstrably belongs to every command*, not
speculative indirection.

## Decision

Behaviour that applies to more than one command is implemented once, derived
from the generated API models:

- **Required-field and input validation comes from the datamodels**
  (`is_required()` and the field types), not from hand-written checks in a
  command handler. If a field's requiredness changes server-side, regeneration
  is the only change needed on this side.
- **Error mapping and exit codes are shared.** A server rejection, an auth
  failure, and a transport error map to messages and exit codes in one place, so
  every command is scriptable the same way.
- **A per-command check is acceptable only for behaviour genuinely unique to
  that command** — and the PR says why it cannot be derived.
- **Do not add an abstraction with no behaviour behind it.** A generic layer is
  justified by the cross-cutting behaviour it carries, not by symmetry.

## Consequences

New commands arrive with validation, error reporting, and exit codes already
correct, and the generated layer can be regenerated freely without losing
hand-applied logic. The cost is that a contributor who found one broken command
cannot fix just that command: the smallest correct fix is a change to shared
machinery with correspondingly broader testing. Several well-intentioned
single-command fixes were closed for precisely this reason — a poor experience for
someone who reported a real defect.

A change **violates** this decision when it:

- adds required-argument, presence, or input-format validation inside a single
  command handler for a condition that applies to other commands too;
- hand-writes error-to-exit-code or error-to-message mapping in a command
  instead of using the shared handling;
- re-applies validation logic to a generated command body, where the next
  regeneration will drop it;
- introduces a generic wrapper or abstraction layer without cross-cutting
  behaviour that depends on it.

## Evidence

- #65047 — per-command JSON-structure validation for variables import, closed for
  the generic solution covering all input.
- #61942 — `dag_id` validation for pause/unpause; reviewers required a shared
  validator derived from the API model, noting the others are generated from the
  same place.
- #63388 — "Amend compatibility issues for airflowctl", merged 2026-03-12: the
  landed implementation. `fill_missing_fields()` in `api/operations.py` walks
  `model_fields` and consults `field_info.is_required()`, deriving the behaviour
  from the generated models exactly as this decision describes.
- #65092 — a later re-attempt at the same client-side `is_required()` validation,
  opened after #63388 landed; closed unmerged. Superseded, not a precedent.
- #67410 — a separate required-field-validation attempt, closed with a batch from
  the same author for lack of testing evidence and template compliance.
- #53323 — a generic abstraction over operation calls, closed as carrying no
  demonstrable value at that point: the counter-case that bounds this decision.
