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

# Apache Airflow — project manifest

The **project configuration** apache-magpie skills read to resolve
Airflow's identity, repository and mailing-list values. Skills load it
through the `<project-config>` lookup chain described in
[`docs/setup/agentic-overrides.md`](https://github.com/apache/magpie/blob/main/docs/setup/agentic-overrides.md).

This is the **public codebase** repository, so the manifest carries only
the blocks that make sense here. The security-tracker blocks of the
upstream template — CVE authority, tracker board node IDs, mail-source
backends, issue-template field mappings — are deliberately **not**
declared: they describe a private security tracker, not `apache/airflow`.
A skill that needs one of those resolves it from the ASF organization
defaults, or asks.

## Identity

| Key | Value |
|---|---|
| `organization` | `ASF` |
| `project_name` | `Apache Airflow` |
| `vendor` | `Apache Software Foundation` |
| `short_name` | `Airflow` |
| `product_family_url` | `https://airflow.apache.org/` |

## Repositories

| Key | Value | Purpose |
|---|---|---|
| `upstream_repo` | `apache/airflow` | The public codebase — this repository |
| `upstream_repo_url` | `https://github.com/apache/airflow` | |
| `upstream_default_branch` | `main` | What `<default-branch>` resolves to |
| `upstream_agents_md_url` | `https://github.com/apache/airflow/blob/main/AGENTS.md` | Contribution conventions every agent follows. `CLAUDE.md` is a symlink to it |
| `upstream_contributing_docs_url` | `https://github.com/apache/airflow/tree/main/contributing-docs` | |
| `upstream_genai_disclosure_anchor` | `https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#gen-ai-assisted-contributions` | Required in the body of any AI-assisted PR |
| `upstream_security_policy_url` | `https://github.com/apache/airflow/security/policy` | |

Airflow standardises on two git remote names: `upstream` is
`apache/airflow`, `origin` is the contributor's fork. See
[`AGENTS.md` § Git remote naming conventions](https://github.com/apache/airflow/blob/main/AGENTS.md).

## Mailing lists

| Key | Value | Notes |
|---|---|---|
| `security_list` | `security@airflow.apache.org` | Inbound vulnerability reports; **not** publicly archived |
| `private_list` | `private@airflow.apache.org` | PMC escalation; **not** publicly archived |
| `users_list` | `users@airflow.apache.org` | Publicly archived |
| `dev_list` | `dev@airflow.apache.org` | Release `[VOTE]` / `[RESULT][VOTE]` threads; publicly archived |
| `announce_list` | `announce@apache.org` | Foundation-wide announcements; publicly archived |
| `commits_list` | `commits@airflow.apache.org` | Publicly archived; also receives issue, PR and discussion notifications per `.asf.yaml` |

Only URLs on publicly archived lists may appear in a CVE record's
`references[]` as `vendor-advisory`.

## Pointers to sibling files

- [`pr-management-config.md`](pr-management-config.md) — identifiers, labels, grace windows and feedback delivery for the PR-management skills.
- [`pr-management-triage-ci-check-map.md`](pr-management-triage-ci-check-map.md) — CI-check pattern → category + doc URL.
- [`pr-management-triage-comment-templates.md`](pr-management-triage-comment-templates.md) — contributor-facing comment templates.
- [`setup-isolated-setup-install.md`](setup-isolated-setup-install.md) — Airflow-specific steps for the secure-agent setup.
- [`README.md`](README.md) — what this directory is and the rule that framework changes go via PR to `apache/magpie`.
