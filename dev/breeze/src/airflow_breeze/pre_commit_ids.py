# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# NOTE! THIS FILE IS AUTOMATICALLY GENERATED AND WILL BE
# OVERWRITTEN WHEN RUNNING PRE_COMMIT CHECKS.
#
# IF YOU WANT TO MODIFY IT, YOU SHOULD MODIFY THE TEMPLATE
# `pre_commit_ids_TEMPLATE.py.jinja2` IN the `dev/breeze/src/airflow_breeze` DIRECTORY
from __future__ import annotations

PRE_COMMIT_LIST = [
    "all",
    "bandit",
    "blacken-docs",
    "check-aiobotocore-optional",
    "check-airflow-k8s-not-used",
    "check-airflow-provider-compatibility",
    "check-airflow-providers-bug-report-template",
    "check-apache-license-rat",
    "check-base-operator-partial-arguments",
    "check-base-operator-usage",
    "check-boring-cyborg-configuration",
    "check-breeze-top-dependencies-limited",
    "check-builtin-literals",
    "check-changelog-format",
    "check-changelog-has-no-duplicates",
    "check-cncf-k8s-only-for-executors",
    "check-code-deprecations",
    "check-common-compat-used-for-openlineage",
    "check-core-deprecation-classes",
    "check-daysago-import-from-utils",
    "check-decorated-operator-implements-custom-name",
    "check-deferrable-default",
    "check-docstring-param-types",
    "check-example-dags-urls",
    "check-executables-have-shebangs",
    "check-extra-packages-references",
    "check-extras-order",
    "check-fab-migrations",
    "check-for-inclusive-language",
    "check-get-lineage-collector-providers",
    "check-google-re2-as-dependency",
    "check-hatch-build-order",
    "check-hooks-apply",
    "check-incorrect-use-of-LoggingMixin",
    "check-init-decorator-arguments",
    "check-integrations-list-consistent",
    "check-lazy-logging",
    "check-links-to-example-dags-do-not-use-hardcoded-versions",
    "check-merge-conflict",
    "check-newsfragments-are-valid",
    "check-no-airflow-deprecation-in-providers",
    "check-no-providers-in-core-examples",
    "check-only-new-session-with-provide-session",
    "check-persist-credentials-disabled-in-github-workflows",
    "check-pre-commit-information-consistent",
    "check-provide-create-sessions-imports",
    "check-provider-docs-valid",
    "check-provider-yaml-valid",
    "check-providers-subpackages-init-file-exist",
    "check-pydevd-left-in-code",
    "check-revision-heads-map",
    "check-safe-filter-usage-in-html",
    "check-sql-dependency-common-data-structure",
    "check-start-date-not-used-in-defaults",
    "check-system-tests-present",
    "check-system-tests-tocs",
    "check-taskinstance-tis-attrs",
    "check-template-context-variable-in-sync",
    "check-template-fields-valid",
    "check-tests-in-the-right-folders",
    "check-tests-unittest-testcase",
    "check-urlparse-usage-in-code",
    "check-usage-of-re2-over-re",
    "check-xml",
    "codespell",
    "compile-ui-assets",
    "compile-ui-assets-dev",
    "compile-www-assets",
    "compile-www-assets-dev",
    "create-missing-init-py-files-tests",
    "debug-statements",
    "detect-private-key",
    "doctoc",
    "end-of-file-fixer",
    "fix-encoding-pragma",
    "flynt",
    "generate-airflow-diagrams",
    "generate-openapi-spec",
    "generate-pypi-readme",
    "identity",
    "insert-license",
    "kubeconform",
    "lint-chart-schema",
    "lint-css",
    "lint-dockerfile",
    "lint-helm-chart",
    "lint-json-schema",
    "lint-markdown",
    "lint-openapi",
    "mixed-line-ending",
    "mypy-airflow",
    "mypy-dev",
    "mypy-docs",
    "mypy-providers",
    "pretty-format-json",
    "pylint",
    "python-no-log-warn",
    "replace-bad-characters",
    "rst-backticks",
    "ruff",
    "ruff-format",
    "shellcheck",
    "trailing-whitespace",
    "ts-compile-format-lint-ui",
    "ts-compile-format-lint-www",
    "update-black-version",
    "update-breeze-cmd-output",
    "update-breeze-readme-config-hash",
    "update-build-dependencies",
    "update-chart-dependencies",
    "update-common-sql-api-stubs",
    "update-er-diagram",
    "update-extras",
    "update-in-the-wild-to-be-sorted",
    "update-inlined-dockerfile-scripts",
    "update-installed-providers-to-be-sorted",
    "update-installers",
    "update-local-yml-file",
    "update-migration-references",
    "update-openapi-spec-tags-to-be-sorted",
    "update-providers-dependencies",
    "update-reproducible-source-date-epoch",
    "update-spelling-wordlist-to-be-sorted",
    "update-supported-versions",
    "update-vendored-in-k8s-json-schema",
    "update-version",
    "validate-operators-init",
    "yamllint",
]
