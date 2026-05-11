<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Apache Airflow — pr-management-triage CI-check to doc-URL map](#apache-airflow--pr-management-triage-ci-check-to-doc-url-map)
  - [Table](#table)
  - [Notes](#notes)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — pr-management-triage CI-check to doc-URL map

This file is the **CI-check categorisation table** for the
[`pr-management-triage`](../../.claude/skills/pr-management-triage/SKILL.md) skill's
violations comments.  It contains the concrete mapping for the
Apache Airflow project.  New adopters should copy this file into
their own `<project-config>/pr-management-triage-ci-check-map.md`
and replace every Airflow-specific pattern and URL with their
project's equivalents.

## Table

Each row maps a **GitHub check name pattern** (case-insensitive
substring match) to a **human-readable category name** the skill
prints in the violations comment, plus a **doc URL** the skill
links to.

| Pattern | Category | Doc URL |
|---|---|---|
| `static checks`, `pre-commit`, `prek` | Pre-commit / static checks | `https://github.com/apache/airflow/blob/main/contributing-docs/08_static_code_checks.rst` |
| `ruff` | Ruff (linting / formatting) | `https://github.com/apache/airflow/blob/main/contributing-docs/08_static_code_checks.rst` |
| `mypy-` | mypy (type checking) | `https://github.com/apache/airflow/blob/main/contributing-docs/08_static_code_checks.rst` |
| `unit test`, `test-` | Unit tests | `https://github.com/apache/airflow/blob/main/contributing-docs/09_testing.rst` |
| `docs`, `spellcheck-docs`, `build-docs` | Build docs | `https://github.com/apache/airflow/blob/main/contributing-docs/11_documentation_building.rst` |
| `helm` | Helm tests | `https://github.com/apache/airflow/blob/main/contributing-docs/testing/helm_unit_tests.rst` |
| `k8s`, `kubernetes` | Kubernetes tests | `https://github.com/apache/airflow/blob/main/contributing-docs/testing/k8s_tests.rst` |
| `build ci image`, `build prod image`, `ci-image`, `prod-image` | Image build | `https://github.com/apache/airflow/blob/main/contributing-docs/08_static_code_checks.rst` |
| `provider` | Provider tests | `https://github.com/apache/airflow/blob/main/contributing-docs/12_provider_distributions.rst` |
| `*` (catch-all) | Other failing CI checks | `https://github.com/apache/airflow/blob/main/contributing-docs/08_static_code_checks.rst` |

## Notes

- **Order matters.** The skill matches first-found; more-specific
  patterns are listed above broader ones (e.g. `mypy-airflow-core`
  matches the `mypy-` row).
- **Mergeability fallback.** If the PR has `mergeable ==
  CONFLICTING`, the skill emits a separate "Merge conflicts"
  category linking to the project's git/rebase doc:

| Concept | Doc URL |
|---|---|
| Merge conflicts (rebase guide) | `https://github.com/apache/airflow/blob/main/contributing-docs/10_working_with_git.rst` |

- **Failing-CI fallback.** If `checks_state == FAILURE` but no
  failed check names are extractable, the skill emits a generic
  "Failing CI checks" entry pointing at the same doc URL as the
  catch-all row above.
