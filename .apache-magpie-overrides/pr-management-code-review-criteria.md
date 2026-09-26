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

<!-- SPDX-License-Identifier: Apache-2.0 -->
# Apache Airflow — code-review criteria sources

Generated from this repository. Ground truth is the
source files themselves; this file only points at them.

## repo_wide_source_files

| File | What it covers |
|---|---|
| `.github/instructions/code-review.instructions.md` | The review checklist every Airflow PR is checked against |
| `AGENTS.md` (`CLAUDE.md` is a symlink to it) | Architecture boundaries, security model, coding standards, testing standards, commit/PR conventions, newsfragment rules |
| `contributing-docs/05_pull_requests.rst` | PR conventions incl. Gen-AI disclosure |

## Per-area source files

| Subtree | File |
|---|---|
| `providers/` | `providers/AGENTS.md` |
| `providers/common/ai/` | `providers/common/ai/AGENTS.md` |
| `providers/elasticsearch/` | `providers/elasticsearch/AGENTS.md` |
| `providers/opensearch/` | `providers/opensearch/AGENTS.md` |
| `airflow-core/src/airflow/ui/` | `airflow-core/src/airflow/ui/AGENTS.md` |
| `airflow-core/src/airflow/api_fastapi/execution_api/` | `airflow-core/src/airflow/api_fastapi/execution_api/AGENTS.md` |
| `airflow-core/src/airflow/_shared/` | `airflow-core/src/airflow/_shared/AGENTS.md` |
| `task-sdk/src/airflow/sdk/_shared/` | `task-sdk/src/airflow/sdk/_shared/AGENTS.md` |
| `task-sdk/src/airflow/sdk/execution_time/schema/` | `task-sdk/src/airflow/sdk/execution_time/schema/AGENTS.md` |
| `registry/` | `registry/AGENTS.md` |
| `dev/` | `dev/AGENTS.md`, `dev/ide_setup/AGENTS.md` |
| `scripts/ci/prek/` | `scripts/ci/prek/AGENTS.md` |

Any other `AGENTS.md` under a touched path is auto-discovered via `git ls-files`.

## security_model_calibration

| Key | Value |
|---|---|
| `file` | `airflow-core/docs/security/security_model.rst` |
| `supplementary` | `airflow-core/docs/security/jwt_token_authentication.rst`, `AGENTS.md` § Security Model |

## Backports / version-specific PRs

| Key | Value |
|---|---|
| `backport_branch_pattern` | `v[0-9]-[0-9]-test` (e.g. `v3-1-test`, `v3-3-test`) |

## Section anchors

Base: `https://github.com/apache/airflow/blob/main/.github/instructions/code-review.instructions.md`

| Section | Anchor |
|---|---|
| Architecture boundaries | `#architecture-boundaries` |
| Database / query correctness | `#database-and-query-correctness` |
| Code quality | `#code-quality-rules` |
| Testing | `#testing-requirements` |
| API correctness | `#api-correctness` |
| UI (React/TypeScript) | `#ui-code-reacttypescript` |
| Generated files | `#generated-files` |
| AI-generated code signals | `#ai-generated-code-signals` |
| Quality signals to check | `#quality-signals-to-check` |
| Commits and PRs | `https://github.com/apache/airflow/blob/main/AGENTS.md#commits-and-prs` |
| Security model | `https://github.com/apache/airflow/blob/main/airflow-core/docs/security/security_model.rst` |
