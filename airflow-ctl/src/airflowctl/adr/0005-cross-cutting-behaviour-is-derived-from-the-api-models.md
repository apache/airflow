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

A missing required argument, a malformed input file, a server rejection — these
are not per-command problems. Every command that posts a body has required
fields; every command that reads a file can be handed a broken one; every
command can get a 4xx back. The generated datamodels already describe which
fields are required, because the server's OpenAPI spec says so and
`datamodel-codegen` carries it into the Pydantic models.

So there are two places the behaviour can live. It can be derived once from
those models — validate against `is_required()`, map the failure to a message
and an exit code, and every present and future command inherits it. Or it can be
written into individual handlers, one `if not args.file: …` at a time.

Review here has repeatedly chosen the first, and the reasons are specific rather
than aesthetic. Per-command checks cover only the commands someone got around
to; the coverage gap is invisible, because nothing fails — the user simply gets
a stack trace from one command and a clean message from its neighbour. They
drift as the spec changes, since a hand-written check does not know a field
became optional upstream. And for the generated commands they are actively
fragile: the handler is regenerated, and the local validation is either lost or
has to be re-applied by hand forever.

The pattern is visible in what got closed. A PR adding JSON-structure validation
to the variables-import command was closed in favour of the generic solution
covering all input. A PR validating `dag_id` for pause/unpause drew the response
that the validation is needed for all commands, not those two, and that the rest
are generated from the same place anyway. Both were correct bug reports and both
were closed; the generic implementation, deriving required-field validation from
the Pydantic models' `is_required()`, is what landed — in #63388.

The limit matters too. This is not a mandate to abstract for its own sake — a PR
proposing a generic wrapper over operation calls was closed because, near a
release and with no concrete behaviour riding on it, the abstraction bought
nothing. The rule applies to *behaviour that demonstrably belongs to every
command*, not to speculative indirection.

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
hand-applied logic.

The cost is that a contributor who has found one broken command cannot fix just
that command. The smallest correct fix is a change to shared machinery, which
touches every command's behaviour and needs correspondingly broader testing —
a much larger PR than the bug appeared to warrant. Several well-intentioned
single-command fixes were closed for precisely this reason, which is a poor
experience for the contributor who reported a real defect.

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

- #65047 — per-command JSON-structure validation for variables import, closed in
  favour of the generic solution covering all input.
- #61942 — `dag_id` validation for pause/unpause; reviewers required a shared
  validator derived from the command's API model, noting the other commands are
  generated from the same place.
- #63388 — "Amend compatibility issues for airflowctl", merged 2026-03-12: the
  landed implementation. `fill_missing_fields()` in
  `airflow-ctl/src/airflowctl/api/operations.py` walks `model_fields` and consults
  `field_info.is_required()`, deriving the behaviour from the generated models
  exactly as this decision describes.
- #65092 — a later re-attempt at the same client-side `is_required()` validation,
  opened two months after #63388 had already landed it; closed unmerged without
  approvals. Superseded, not a precedent.
- #67410 — a separate attempt at required-field validation, closed alongside a
  batch of PRs from the same author for lack of testing evidence and template
  compliance.
- #53323 — a generic abstraction over operation calls, closed as carrying no
  demonstrable value at that point: the counter-case that bounds this decision.
