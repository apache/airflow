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

# Apache Airflow — release-management configuration

Project configuration read by the `magpie-release-management` skills. Values
are taken from [`dev/README_RELEASE_PROVIDERS.md`](../dev/README_RELEASE_PROVIDERS.md),
which remains the source of truth for the process.

**Scope: the providers release train only.** Airflow ships several trains
(providers, Airflow core, Task SDK, airflowctl, Helm chart), each with its own
`dev/README_RELEASE_*.md`. Only the providers train is configured here so that
`release-verify-rc` can run; the other trains, `pmc-roster.md` and
`release-trains.md` are not configured yet. A skill asked to act on another
train must say so and stop rather than apply these values.

## Identifiers

| Key | Value |
|---|---|
| `project_dist_name` | `airflow` |
| `git_upstream_remote` | `upstream` |
| `release_branch_base` | `main` |
| `version_manifest_files` | `providers/<provider-path>/provider.yaml` (`versions:` head), `providers/<provider-path>/pyproject.toml` (`version`), `providers/<provider-path>/src/airflow/providers/<provider-path>/__init__.py` (`__version__`) |

A providers wave is identified by its **preparation date**, not a single
version: one `providers/<YYYY-MM-DD>` tag for the wave plus one
`providers-<provider-id>/<version>rcN` tag per provider in it, where
`<provider-id>` is the provider path with `/` replaced by `-`.

## Backends

| Key | Value |
|---|---|
| `release_dist_backend` | `svnpubsub` |
| `release_vote_backend` | `manual` |
| `release_approval_mechanism` | `dev-list-vote` |
| `release_announce_backend` | `announce-list` |

## Distribution URLs

| Key | Value |
|---|---|
| `release_dist_url_template` | `https://dist.apache.org/repos/dist/<bucket>/airflow/providers/` — RC staging is under `dev/airflow/providers/<YYYY-MM-DD>/` |
| `archive_url_template` | `https://archive.apache.org/dist/airflow/providers/` |

The RC distributions are also published to PyPI as `<version>rcN`, which is
where contributors install them from for testing.

## Signing

| Key | Value |
|---|---|
| `keys_file_url` | `https://dist.apache.org/repos/dist/release/airflow/KEYS` |
| `keyserver` | `keys.openpgp.org` |
| `automated_release_signing` | `off` |

## Vote

| Key | Value |
|---|---|
| `vote_dev_list` | `dev@airflow.apache.org` |
| `mail_archive` | `ponymail` |
| `mail_archive_url_template` | `https://lists.apache.org/list.html?dev@airflow.apache.org` |
| `vote_window_hours` | `72` |
| `vote_pass_rule_overrides` | *(none — ASF baseline)* |
| `vote_subject_template` | `[VOTE] Airflow Providers, release preparation date <YYYY-MM-DD>` |
| `result_subject_template` | `[RESULT][VOTE] Airflow Providers - release preparation date <YYYY-MM-DD>` |
| `vote_verification_doc_url` | `https://github.com/apache/airflow/blob/main/dev/README_RELEASE_PROVIDERS.md#verify-the-release-candidate-by-pmc-members` |
| `vote_verification_skill` | `magpie-release-management:verify-rc` |

Contributors verify their own changes per
[`README_RELEASE_PROVIDERS.md` § Verify the release candidate by Contributors](../dev/README_RELEASE_PROVIDERS.md#verify-the-release-candidate-by-contributors),
which [`release-verify-rc.md`](release-verify-rc.md) automates as an optional
step, and report in the "Status of testing Providers that were prepared on
<date>" GitHub issue.

## Announce

| Key | Value |
|---|---|
| `announce_list` | `announce@apache.org` |
| `announce_cc_lists` | `dev@airflow.apache.org`, `users@airflow.apache.org` |
| `site_repo` | `apache/airflow-site` |
