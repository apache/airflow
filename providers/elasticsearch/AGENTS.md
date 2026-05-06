<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Elasticsearch Provider — Agent Instructions

## Keep in sync with `providers/opensearch`

OpenSearch was forked from Elasticsearch and the two providers share most of
their task-log handler code and surface. File layouts mirror each other:

| Elasticsearch                                   | OpenSearch                              |
| ----------------------------------------------- | --------------------------------------- |
| `log/es_task_handler.py`                        | `log/os_task_handler.py`                |
| `log/es_response.py`                            | `log/os_response.py`                    |
| `log/es_json_formatter.py`                      | `log/os_json_formatter.py`              |
| `ElasticsearchTaskHandler`                      | `OpensearchTaskHandler`                 |
| `ElasticsearchRemoteLogIO`                      | `OpensearchRemoteLogIO`                 |

**When fixing a bug or changing behaviour here, check whether the equivalent
change is needed in `providers/opensearch` (and vice-versa).** This applies
especially to: task-log handler logic, log grouping / formatting, connection
handling, URL/credential treatment, and response parsing. The two packages
ship on independent release cadences, so the fix should usually land as two
separate PRs on the same day.

Legitimate reasons to diverge: upstream client API differences (`elasticsearch`
vs `opensearchpy`), provider-specific features that only one side has, or
changes gated on config that only exists in one provider.
