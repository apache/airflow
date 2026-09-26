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

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Apache Airflow — committer-onboarding configuration](#apache-airflow--committer-onboarding-configuration)
  - [Intake model](#intake-model)
    - [Intake-model-specific keys](#intake-model-specific-keys)
  - [Governance model](#governance-model)
    - [Governance-model-specific keys](#governance-model-specific-keys)
  - [Cross-references](#cross-references)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — committer-onboarding configuration

**This file enumerates the capability-flag vocabulary for the
`committer-onboarding` skill.** It is the contributor-growth
counterpart to `release-management-config.md`'s backend-flag model:
an adopter declares the intake and governance model that suits their
community, and the skill emits onboarding steps shaped for that model,
without any skill-body edit.

**Currently the `committer-onboarding` skill defaults to the ASF-PMC
/ ICLA model** (the ASF default). This file establishes the flag
vocabulary so that a non-ASF adopter can declare their model here;
the skill will read these flags in a follow-on update. New adopters
should copy this file into their own
`<project-config>/committer-onboarding-config.md` and replace every
`TODO`.

Related scaffolds in the same adopter directory:

- `committer-readiness.md` — activity thresholds the
  `contributor-to-committer` readiness tracker compares against
  (added by the `contributor-to-committer` skill).
- [`contributor-nomination-config.md`](contributor-nomination-config.md)
  — nomination-brief thresholds and assessment window.
- [`pmc-roster.md`](pmc-roster.md) — PMC-member roster used by
  `release-vote-tally` to classify binding vs non-binding votes.

---

## Intake model

Declares how new committers are formally onboarded to the project's
IP policy after a vote passes.

```yaml
committer_intake:
  # Model under which new committers are formally onboarded to the
  # project's IP policy.
  # ASF default: icla — every new committer must file a signed ICLA
  # with the Apache Software Foundation before commit bits are
  # granted. The skill checks Whimsy and blocks the account-request
  # step if no ICLA is on file.
  # Override when:
  #   dco — Developer Certificate of Origin (Linux Foundation model).
  #     A per-commit `Signed-off-by:` line replaces the one-time CLA;
  #     the skill links the DCO guide and verifies that the candidate's
  #     recent merged PRs carry sign-off.
  #   no-cla — No formal contributor agreement required (open/trust-
  #     based model). The skill skips IP-check steps entirely.
  # Allowed values: icla, dco, no-cla
  # Consumed by: committer-onboarding (intake-check step).
  model: icla  # icla | dco | no-cla  (ASF project; COMMITTERS.rst requires a CLA on file)
```

### Intake-model-specific keys

Fill in only the block that matches your `model` above; leave the
others absent or blank.

#### `icla` model

```yaml
committer_intake_icla:
  # URL the skill links when asking the nominator to check the
  # candidate's ICLA status.
  # ASF default: Whimsy's committer lookup.
  # Non-ASF adopters using ICLA: point at your foundation's CLA
  # query endpoint.
  # Consumed by: committer-onboarding.
  lookup_url: https://whimsy.apache.org/roster/committer/

  # Whether ICLA filing is a hard prerequisite that the skill refuses
  # to bypass. When true, the skill blocks until the nominator
  # confirms the ICLA is on file; when false, the skill flags the
  # gap but allows the nominator to proceed with a warning.
  # ASF default: true.
  # Consumed by: committer-onboarding.
  mandatory: true
```

#### `dco` model

```yaml
committer_intake_dco:
  # Reference URL the skill links in onboarding communications
  # explaining what DCO sign-off means for contributors.
  # Linux Foundation / CNCF projects typically link
  # https://developercertificate.org/ plus their own CONTRIBUTING.md.
  # Consumed by: committer-onboarding.
  reference_url: null  # n/a — Airflow uses the icla model, not DCO

  # Minimum number of the candidate's recently merged PRs that must
  # carry a valid `Signed-off-by:` line before the skill considers
  # the DCO check passed.
  # Set to 0 to skip the check (honour-system model).
  # Consumed by: committer-onboarding.
  min_signed_off_prs: 1
```

#### `no-cla` model

```yaml
committer_intake_nocla:
  # Free-text explanation shown in the onboarding checklist in place
  # of an IP-agreement check. Leave null for a default message.
  # Consumed by: committer-onboarding.
  explanation: null
  # e.g. "This project uses an Apache-2.0 inbound/outbound model; no
  # CLA or DCO sign-off is required — contributors retain copyright."
```

---

## Governance model

Declares how committer and PMC (or equivalent) status is formally
tracked, voted on, and applied after a vote passes.

```yaml
committer_governance:
  # Model under which committer and committee status is granted and
  # tracked.
  # ASF default: asf-pmc — the PMC votes on the dev@ list, submits a
  # secretary account-creation request via Whimsy, and the new
  # committer appears on the Apache Whimsy committee/committer roster.
  # Override when:
  #   github-codeowners — CODEOWNERS file + GitHub maintainer-team
  #     membership drives merge permissions; no formal committee vote
  #     and no external account-creation request.
  #   maintainer-roster — an adopter-managed roster file voted on by
  #     existing maintainers, outside any foundation's governance
  #     machinery.
  # Allowed values: asf-pmc, github-codeowners, maintainer-roster
  # Consumed by: committer-onboarding (roster-management steps).
  model: asf-pmc  # asf-pmc | github-codeowners | maintainer-roster
```

### Governance-model-specific keys

Fill in only the block that matches your `model` above.

#### `asf-pmc` model

```yaml
committer_governance_asf_pmc:
  # Whether the project is an incubating podling or a graduated
  # top-level project. This switches between Whimsy's PPMC
  # self-service UI (podling) and `committee-info.txt` edits (TLP).
  # Allowed values: podling, tlp
  # Consumed by: committer-onboarding (roster-management path
  # selection).
  stage: tlp  # podling | tlp  (top-level project since January 2019, airflow-core/docs/project.rst)

  # Whimsy URL the skill links the nominator to for roster management.
  # TLP default: committee detail page.
  # Podling default: PPMC roster page.
  # Consumed by: committer-onboarding.
  whimsy_roster_url: https://whimsy.apache.org/roster/committee/airflow
  # TLP example:   https://whimsy.apache.org/roster/committee/<project>
  # Podling example: https://whimsy.apache.org/roster/ppmc/<project>

  # URL of the ASF's new-committer / new-PMC-member secretary request
  # form. The skill prints this and asks the nominator to fill it in.
  # ASF default: the standard secretary request page.
  # Consumed by: committer-onboarding.
  secretary_request_url: https://whimsy.apache.org/officers/acreq

  # Dev-list address the skill uses when drafting the welcome
  # announcement. Matches `dev_list` in project.md — kept here for
  # self-contained configuration.
  # Consumed by: committer-onboarding.
  dev_list: dev@airflow.apache.org
```

#### `github-codeowners` model

```yaml
committer_governance_github_codeowners:
  # GitHub team slug (in `org/team` form) that maps to the
  # committer/maintainer team. The skill invites the new committer
  # to this team after the vote passes.
  # Consumed by: committer-onboarding.
  maintainers_team: apache/airflow-committers

  # Path to the CODEOWNERS file in the upstream repo, relative to
  # the repo root. The skill optionally opens a PR adding the new
  # committer's handle to the file.
  # Leave null to skip the CODEOWNERS update.
  # Consumed by: committer-onboarding.
  codeowners_file: CODEOWNERS  # or null

  # Whether a GitHub-discussion or a GitHub-issue thread is the
  # canonical "vote thread" for this model.
  # Allowed values: github-discussion, github-issue, off-band
  # `off-band` = the vote happened in Slack / email / another channel
  # and the skill just records the outcome.
  # Consumed by: committer-onboarding (vote-validation step).
  vote_channel: off-band  # votes are held on the private PMC mailing list
```

#### `maintainer-roster` model

```yaml
committer_governance_maintainer_roster:
  # Path to the roster file (relative to <project-config>/) that
  # the skill updates when a vote passes.
  # Consumed by: committer-onboarding.
  roster_file: null  # n/a — Airflow uses the asf-pmc model (roster in Whimsy)

  # Minimum number of approvals required from existing listed
  # maintainers before the skill considers the vote passed.
  # Consumed by: committer-onboarding (vote-validation step).
  min_approvals: 3  # ASF rule: at least 3 binding +1 votes and no veto

  # Channel where votes are held — used by the skill when
  # summarising the vote result.
  # Allowed values: github-discussion, github-issue, mailing-list,
  #   slack-channel, off-band
  # Consumed by: committer-onboarding.
  vote_channel: mailing-list
```

---

## Cross-references

- `committer-readiness.md` — activity thresholds for the
  `contributor-to-committer` readiness tracker (added by the
  `contributor-to-committer` skill).
- [`contributor-nomination-config.md`](contributor-nomination-config.md)
  — nomination-brief thresholds and assessment window.
- [`pmc-roster.md`](pmc-roster.md) — PMC-member roster for vote tally.
- [`committer-onboarding`](../../.agents/skills/magpie-committer-onboarding/SKILL.md)
  — the skill that reads this configuration.
