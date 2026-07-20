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
point for most first contributions. That makes it the place where two failure
modes concentrate, both of which consume review capacity without producing
merged code.

The first is **duplicate work**. A visible "good first issue" against a small
provider attracts many independent implementations of the same fix within days.
One reported bug in the IMAP hook — attachments with identical names silently
overwriting each other — drew ten separate pull requests from ten authors, each
re-implementing the same `overwrite_file` parameter, before a single one was
merged. The same pattern recurred for `template_fields` on the Salesforce bulk
operator, for the Azure Batch SDK migration, for the Alibaba OSS endpoint
option, and for the Microsoft Graph host resolution. Each of those PRs was
individually reasonable. Collectively they cost more review than the fix was
worth, and nine of every ten authors ended up with nothing merged. Reviewers
cannot merge two fixes for one bug, so the marginal PR is pure cost — for the
project *and* for its author.

The second is **unscoped diffs**. A change that fixes one thing but also carries
a bad rebase, an unrelated file an agent decided to touch, formatting churn
across a package, or a second issue's work "while I was in there", cannot be
reviewed as a unit. Providers are independently released, so an unrelated edit
riding along in a provider PR is not merely noise: it ships in that provider's
next release with no changelog line and no reviewer having looked at it as a
change in its own right. Review of such PRs consistently stalls at "please
remove the unrelated changes", which is the most repeated single sentence in the
closed-PR record for this directory.

"Limited to one provider" is not the same as "touches nothing outside
`providers/`". Roughly one in eight merged provider changes legitimately edits a
file under `airflow-core/` or `task-sdk/`, because a small number of files there
are *registries of provider content* rather than core logic.
`airflow-core/tests/unit/always/test_project_structure.py` enumerates every
provider module, so adding an operator module requires editing it; the Task SDK
decorator stubs in `task-sdk/src/airflow/sdk/definitions/decorators/__init__.pyi`
enumerate every `@task.*` decorator a provider contributes; the plugin-registry
tests under `airflow-core/tests/unit/` list registered plugins. Merged examples
are PRs #69613 (which touches both the structure test and the decorator stub),
as well as #69930, #70014 and #68082 — so treating any such file as proof of a
bad rebase sends back correct work. The signal that actually distinguishes a bad
rebase is an edit to core *source* the provider has no reason to change, or a
diff that reverts unrelated commits.

There is one honest exception in the opposite direction. A *mechanical* sweep —
applying the identical, behaviour-preserving edit across many providers, such as
migrating an import to `common.compat` — is better as a single PR than as fifty,
because it is verified by a hook rather than by reading. The distinguishing
question is not "how many providers does this touch" but "does any provider need
its own judgement call". Blanket HTTP timeouts across many providers looked like
a sweep and was not: the right timeout for a metadata call is wrong for a
long-running job submission, so each provider needed its own reasoning, and the
combined PR was split. Conversely, applying a sweep to a provider that does not
exhibit the pattern at all is also wrong — the correct action there is to skip
that provider, not to invent a change for it.

Over-fragmentation is a real failure in the other direction, and the record does
not let this be stated as a rule. #63042 (blanket HTTP timeouts) was closed with
"Closing this PR in favor of splitting"; #63370, a connection-standardisation
change split per connection type, got the opposite instruction — "I need a
single PR that takes care of all, please". Both reviewers were right about their
own change. So the split-vs-single question is one a reviewer answers per
change, and triage does not draft a PR back on this ground alone.

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

- Review capacity goes to distinct problems rather than to the fourth copy of
  one, and contributors are far more likely to see their work merged.
- Contributors must do discovery work before coding — search, read the issue,
  claim it. That friction is deliberate and is cheaper than the alternative.
- Some genuinely parallel work is discouraged. The project accepts this: where
  two approaches really do differ, saying so in the issue first is what unlocks
  the second PR.
- Large uniform migrations remain reviewable, because they stay mechanical and
  are checked by tooling rather than read provider by provider.

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
  #62321 — ten independent PRs adding the same overwrite handling to the IMAP
  hook's attachment download, all closed in favour of one merged fix (#68838).
  Review response: "There are too many PRs raised for this issue. You are
  welcome to assist with review the open PR — we don't need more of the same."
- #66066, #66577, #66578 — a second cluster on the same hook, for non-ASCII
  attachment filenames, closed once one of them merged.
- #62908, #62852, #62840, #62539 — four PRs adding `template_fields` to the
  Salesforce bulk operator, resolved by #63109.
- #66455 / #66452 (Azure Batch upper bound), #66512 / #66479 (Alibaba OSS
  endpoint), #61158 / #61103 (Microsoft Graph host), #62042 / #62043 (Snowflake
  OAuth), #65934 / #66302 (Kafka trigger `super().__init__()`), #53290 / #53214
  (Databricks endpoint) — the same duplicate-work pattern across six more
  providers.
- #64027 and #55686 — closed on "please remove all unrelated changes"; both
  authors reopened clean, single-purpose PRs that were then reviewed.
- #60050 — a JDBC provider migration whose diff also touched `airflow-core/`
  and `task-sdk/` *source*; sent back as a bad rebase and reopened from a clean
  base.
- #69613, #69930, #70014, #68082 — merged provider changes that edit
  `airflow-core/tests/unit/always/test_project_structure.py`, the Task SDK
  decorator stubs, or the plugin-registry tests, because those files enumerate
  provider content. Files outside `providers/` are not on their own evidence of
  a bad rebase.
- #70122 ("Add `toolset` as a provider module category") and #64941 (decoupling
  the S3 object-storage provider from `common.sql`) — both edit
  `provider.yaml.schema.json`, and #64941 also `provider_info.schema.json` and
  `providers_manager.py`, because those files *define* what provider metadata may
  contain. #64941 is provider-primary (13 of 18 files under `providers/`), so the
  primacy gate alone would not clear it — the registries have to be enumerated.
- #69537 (a repo-wide Alembic migration lint, 1 of 23 files under `providers/`),
  #69757 (a Go SDK TaskFlow feature, 2 of 39), #69896 (an Execution API
  log-ID-template change, 2 of 17), #68700 (connection port validation, 2 of 21)
  and #69943 (multi-team metrics tags, 1 of 11) — core-primary changes that
  appear in a provider corpus only because they touch one provider file. None is
  a bad rebase, and the provider-primacy gate is what keeps this rule off them.
- #61035 — a GCS hook helper that mixed in work from another contributor's
  in-flight issue; closed with "one PR should address only one issue".
- #59847, #61569 — UI and auth changes carrying unrelated provider edits an
  agent had made, which the authors had not reviewed.
- #63042 — "Add missing HTTP timeouts across multiple providers": closed and
  split into per-provider PRs, because a timeout safe for a control-plane call
  breaks a long-running one.
- #57066 — a `common.compat` migration opened for the Drill provider, which
  does not exhibit the pattern; closed with "we only need to add that dep where
  there is the pattern usage described in the issue".
- #63370 — the counterexample that keeps this out of the violates list: a
  standardisation change split per connection type, where review asked for a
  *single* PR covering all of them ("I need a single PR that takes care of all,
  please"). Over-fragmentation is penalised as readily as over-bundling.
