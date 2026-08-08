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

# 5. One provider, one problem, one in-flight PR

Date: 2026-07-20

## Status

Accepted

## Context

`providers/` is the widest, most-forked part of the repository and the entry
point for most first contributions, which concentrates two failure modes that
consume review capacity without producing merged code.

**Duplicate work.** A visible "good first issue" against a small provider draws
many independent implementations of the same fix within days — one IMAP-hook bug
(identically-named attachments overwriting each other) drew ten PRs re-adding the
same `overwrite_file` parameter before one merged, and the pattern recurred for
Salesforce `template_fields`, the Azure Batch SDK migration, Alibaba OSS, and
Microsoft Graph host resolution. Reviewers cannot merge two fixes for one bug, so
the marginal PR is pure cost.

**Unscoped diffs.** A change that also carries a bad rebase, an unrelated agent
edit, formatting churn, or a second issue's work cannot be reviewed as a unit —
and because providers are independently released, the unrelated edit ships with
no changelog line and no reviewer having looked at it. "Please remove the
unrelated changes" is the most repeated sentence in this directory's closed-PR
record.

"Limited to one provider" is not "touches nothing outside `providers/`". Roughly
one in eight merged provider changes legitimately edits a file under
`airflow-core/` or `task-sdk/`, because a few files there are *registries of
provider content* (`test_project_structure.py` enumerates every provider module,
the Task SDK decorator stubs every `@task.*` decorator, the plugin-registry tests
the registered plugins). #69613, #69930, #70014 and #68082 are merged examples, so
treating any such file as proof of a bad rebase sends back correct work. The real
signal is an edit to core *source* the provider has no reason to change, or a diff
reverting unrelated commits.

Two honest exceptions in opposite directions. A *mechanical*, behaviour-preserving
sweep across many providers (e.g. an import migration to `common.compat`) is
better as one hook-verified PR than fifty — but applied to a provider that does
not exhibit the pattern it is wrong, and the fix is to skip it. And
over-fragmentation is a real failure too: #63042 (blanket HTTP timeouts) closed
"in favor of splitting" because the right timeout differs per provider, while #63370
got the opposite instruction — "I need a single PR that takes care of all". Both
reviewers were right, so split-vs-single is a per-change reviewer call, not a
triage-draftable ground.

## Decision

A provider pull request addresses one problem, in one provider, and does not
duplicate work already in flight.

- **Check for existing work before writing any.** Search open PRs and read the
  linked issue, including its comments, for someone who already claimed it. If a
  PR exists, review it or offer to help — a second implementation is closed.
- **Claim the issue before implementing it.** Comment on the issue and get it
  assigned; that is the mechanism the project uses to prevent this collision.
- **One PR fixes one problem.** No second issue's work, no drive-by refactors,
  no formatting churn beyond the lines the fix touches.
- **The diff is limited to the provider being changed**, plus its generated
  metadata and the enumerated core registries that list provider content. Those
  registries are: `airflow-core/tests/unit/always/test_project_structure.py`,
  the Task SDK decorator stubs under
  `task-sdk/src/airflow/sdk/definitions/decorators/`, the plugin-registry tests
  under `airflow-core/tests/unit/`, cross-referenced pages under
  `airflow-core/docs/`, and the provider-metadata machinery —
  `airflow-core/src/airflow/provider.yaml.schema.json`,
  `airflow-core/src/airflow/provider_info.schema.json` and
  `airflow-core/src/airflow/providers_manager.py` (adding a new provider module
  category *requires* the schema edit). Anything else under `airflow-core/` or
  `task-sdk/` — core source, unrelated core tests — needs the PR body to say why
  it is there, and is otherwise reviewed as a bad rebase.

  This test only applies to a **provider-primary** PR: one where the majority of
  changed files are under `providers/`. A core, Go SDK, Helm or Execution API
  change that happens to touch one provider file is a change in *that* area,
  reviewed against *that* area's rules; the provider file is the incidental edge,
  not the diff. Reading it the other way round is the single most common way this
  rule fires on correct work.
- **Whether a general behaviour change is one PR or one per provider is a
  reviewer's call, not a rule.** It is per-provider when each provider needs its
  own judgement on the value or the risk; it is one PR when the same reasoning
  holds everywhere and splitting it would make the set unreviewable. Review has
  asked for both, on comparable-looking changes.
- **A mechanical, behaviour-preserving sweep is one PR**, and it skips providers
  where the pattern does not occur rather than inventing an edit for them.
- **Superseding your own PR means closing it.** Reopening the same work in a
  fresh PR after a history rewrite is fine; leaving both open is not.

## Consequences

- Review capacity goes to distinct problems rather than the fourth copy of one,
  and contributors are far more likely to see their work merged.
- Contributors must do discovery before coding — search, read the issue, claim
  it. That friction is deliberate and cheaper than the alternative.
- Some genuinely parallel work is discouraged; where two approaches really differ,
  saying so in the issue first is what unlocks the second PR.
- Large uniform migrations stay reviewable, because they stay mechanical and are
  checked by tooling.

A change **violates** this decision when it:

- implements a fix for an issue that already has an open, active PR;
- bundles two unrelated fixes, or a fix plus an unrequested refactor or
  reformat, in one PR;
- touches another provider's sources incidentally, or — **in a provider-primary
  PR**, one where the majority of changed files are under `providers/` — edits
  `airflow-core/` / `task-sdk/` outside the enumerated registries above without
  the PR body saying why. That is the signature of a bad rebase or an unreviewed
  agent edit. In a PR whose majority is *outside* `providers/`, the core files
  are the change and the provider file is the incidental edge: this bullet does
  not apply, and the PR is reviewed against the area that owns the majority;
- applies a mechanical sweep to a provider that does not exhibit the pattern,
  rather than skipping it;
- leaves a superseded PR from the same author open alongside its replacement.

Duplicate detection itself works from PR data alone — searching open PRs for the
same file and the same issue link is enough to find the second implementation.
Two *qualifiers* on it do not: whether the author argued in the issue why a
different approach is needed, and whether the issue was assigned to or claimed
in-thread by someone else, both live in the linked issue's thread and are
invisible in the diff. Fetch the linked issue and read its comments before
calling either one. Without that fetch they are review **asks**, not violations:
ask the author whether they saw the open PR and whether the issue was claimed.

Review **asks**, rather than violations, because the record answers them both
ways and a diff does not decide them:

- was the duplicate opened after an argued disagreement in the issue, and was the
  issue already claimed by someone else? Both require reading the linked issue's
  thread — without it, the overlap is a question for the author, not a finding;
- should a general behaviour change spanning several providers be split, or
  stay one PR? Split it when the right value or the risk differs per provider
  (#63042); keep it single when the same reasoning holds everywhere and a split
  produces a set nobody can review as a whole (#63370).

## Evidence

- #67748, #67751, #67755, #67899, #67588, #67019, #66850, #68297, #69468,
  #62321 — ten independent PRs adding the same IMAP-hook overwrite handling, all
  closed for one merged fix (#68838). "There are too many PRs raised for this issue."
- #66066, #66577, #66578 — a second cluster on the same hook (non-ASCII filenames),
  closed once one merged.
- #62908, #62852, #62840, #62539 — four Salesforce `template_fields` PRs, resolved
  by #63109.
- #66455 / #66452, #66512 / #66479, #61158 / #61103, #62042 / #62043, #65934 /
  #66302, #53290 / #53214 — the same duplicate-work pattern across six more providers.
- #64027 and #55686 — closed on "please remove all unrelated changes"; both
  reopened clean and were reviewed.
- #60050 — a JDBC migration touching `airflow-core/` and `task-sdk/` *source*;
  sent back as a bad rebase.
- #69613, #69930, #70014, #68082 — merged provider changes editing the structure
  test, decorator stubs, or plugin-registry tests, because those enumerate provider
  content.
- #70122, #64941 — both edit `provider.yaml.schema.json` (and #64941 also
  `provider_info.schema.json` and `providers_manager.py`) because those define what
  provider metadata may contain. #64941 is provider-primary (13 of 18 under
  `providers/`), so the primacy gate alone would not clear it — the registries have
  to be enumerated.
- #69537 (1 of 23 under `providers/`), #69757 (2 of 39), #69896 (2 of 17), #68700
  (2 of 21), #69943 (1 of 11) — core-primary changes appearing in a provider corpus
  only because they touch one provider file; the provider-primacy gate keeps this
  rule off them.
- #61035 — a GCS hook helper mixing in another contributor's in-flight issue;
  closed "one PR should address only one issue".
- #59847, #61569 — UI and auth changes carrying unrelated provider edits an agent
  had made, unreviewed by the authors.
- #63042 — blanket HTTP timeouts, closed and split per-provider.
- #57066 — a `common.compat` migration for a provider that does not exhibit the
  pattern; closed.
- #63370 — the counterexample: a standardisation change split per connection type,
  where review asked for a *single* PR. Over-fragmentation is penalised as readily
  as over-bundling.
