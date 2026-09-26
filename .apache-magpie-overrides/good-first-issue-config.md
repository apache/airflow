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

- [Apache Airflow — good-first-issue authoring configuration](#apache-airflow--good-first-issue-authoring-configuration)
  - [Identifiers](#identifiers)
  - [Getting-started link](#getting-started-link)
  - [Out-of-scope topics](#out-of-scope-topics)
  - [AI-attribution footer](#ai-attribution-footer)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — good-first-issue authoring configuration

This file configures the
[`good-first-issue-author`](../../skills/good-first-issue-author/SKILL.md)
skill (Agentic Mentoring, `experimental`). Copy it into your own
`<project-config>/good-first-issue-config.md` and replace every
`<placeholder>` with your project's value. If a required key is missing,
the skill aborts and points back here rather than guessing.

## Identifiers

| Key | Value | Notes |
|---|---|---|
| `good_first_issue_label` | `good first issue` | The label proposed on the drafted issue. The skill proposes it; a maintainer applies it on confirmation. |
| `max_effort_hours` | `4` | Upper bound on the estimated effort a good first issue may carry. A candidate that clearly exceeds it is `scope-too-large`. |

## Getting-started link

A single link the drafted issue points a newcomer at. The skill links it
rather than paraphrasing. The link must resolve from a GitHub issue body
(not a repo-rendered file), so use an absolute URL: relative paths like
`CONTRIBUTING.md` 404 when rendered inside an issue. The link must
resolve before the skill drafts an issue; do not leave a placeholder URL
in this row.

| Trigger | Link | One-line label |
|---|---|---|
| Newcomer onboarding | `https://github.com/apache/airflow/blob/main/contributing-docs/03a_contributors_quick_start_beginners.rst` | Your first Airflow pull request (15-minute guide) |

Pick the section of the contributing guide that is genuinely
newcomer-shaped (a "Your first contribution" / "Getting started" section,
not the top of the file, which usually lands on a doctoc TOC).

## Out-of-scope topics

The skill always declines (decision `unsuitable`) when a candidate touches
one of these. Adjust for your project; the defaults below are typical of
an Apache project.

- Security-sensitive work (vulnerabilities, CVE-adjacent, embargoed)
- Deprecation or removal timing (which release drops X)
- Licensing questions (compatibility, header policy)
- Architectural taste on a project-specific subsystem

## AI-attribution footer

Appended verbatim to every drafted issue body, disclosing AI authorship.

```markdown
---

_This issue was drafted with the help of an AI-assisted tool and reviewed by an Apache Airflow maintainer before posting. If anything here is unclear or looks wrong, say so on the issue: a real person is reading._
```

Replace `<PROJECT>` with the project's display name (read from
[`<project-config>/project.md`](project.md)).
