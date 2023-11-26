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
from __future__ import annotations

import json
import os
import re
import sys
from enum import Enum
from functools import cached_property, lru_cache
from typing import Any, Dict, List, TypeVar

from airflow_breeze.global_constants import (
    ALL_PYTHON_MAJOR_MINOR_VERSIONS,
    APACHE_AIRFLOW_GITHUB_REPOSITORY,
    COMMITTERS,
    CURRENT_KUBERNETES_VERSIONS,
    CURRENT_MSSQL_VERSIONS,
    CURRENT_MYSQL_VERSIONS,
    CURRENT_POSTGRES_VERSIONS,
    CURRENT_PYTHON_MAJOR_MINOR_VERSIONS,
    DEFAULT_KUBERNETES_VERSION,
    DEFAULT_MSSQL_VERSION,
    DEFAULT_MYSQL_VERSION,
    DEFAULT_POSTGRES_VERSION,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    HELM_VERSION,
    KIND_VERSION,
    RUNS_ON_PUBLIC_RUNNER,
    RUNS_ON_SELF_HOSTED_RUNNER,
    SELF_HOSTED_RUNNERS_CPU_COUNT,
    GithubEvents,
    SelectiveUnitTestTypes,
    all_helm_test_packages,
    all_selective_test_types,
)
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.exclude_from_matrix import excluded_combos
from airflow_breeze.utils.kubernetes_utils import get_kubernetes_python_combos
from airflow_breeze.utils.path_utils import (
    AIRFLOW_PROVIDERS_ROOT,
    AIRFLOW_SOURCES_ROOT,
    DOCS_DIR,
    SYSTEM_TESTS_PROVIDERS_ROOT,
    TESTS_PROVIDERS_ROOT,
)
from airflow_breeze.utils.provider_dependencies import DEPENDENCIES, get_related_providers

FULL_TESTS_NEEDED_LABEL = "full tests needed"
DEBUG_CI_RESOURCES_LABEL = "debug ci resources"
USE_PUBLIC_RUNNERS_LABEL = "use public runners"
UPGRADE_TO_NEWER_DEPENDENCIES_LABEL = "upgrade to newer dependencies"

ALL_CI_SELECTIVE_TEST_TYPES = (
    "API Always BranchExternalPython BranchPythonVenv "
    "CLI Core ExternalPython Operators Other PlainAsserts "
    "Providers[-amazon,google] Providers[amazon] Providers[google] "
    "PythonVenv Serialization WWW"
)


class FileGroupForCi(Enum):
    ENVIRONMENT_FILES = "environment_files"
    PYTHON_PRODUCTION_FILES = "python_scans"
    JAVASCRIPT_PRODUCTION_FILES = "javascript_scans"
    ALWAYS_TESTS_FILES = "always_test_files"
    API_TEST_FILES = "api_test_files"
    API_CODEGEN_FILES = "api_codegen_files"
    HELM_FILES = "helm_files"
    SETUP_FILES = "setup_files"
    DOC_FILES = "doc_files"
    WWW_FILES = "www_files"
    SYSTEM_TEST_FILES = "system_tests"
    KUBERNETES_FILES = "kubernetes_files"
    ALL_PYTHON_FILES = "all_python_files"
    ALL_SOURCE_FILES = "all_sources_for_tests"
    ALL_AIRFLOW_PYTHON_FILES = "all_airflow_python_files"
    ALL_PROVIDERS_PYTHON_FILES = "all_provider_python_files"
    ALL_DEV_PYTHON_FILES = "all_dev_python_files"
    ALL_PROVIDER_YAML_FILES = "all_provider_yaml_files"
    ALL_DOCS_PYTHON_FILES = "all_docs_python_files"


T = TypeVar("T", FileGroupForCi, SelectiveUnitTestTypes)


class HashableDict(Dict[T, List[str]]):
    def __hash__(self):
        return hash(frozenset(self))


CI_FILE_GROUP_MATCHES = HashableDict(
    {
        FileGroupForCi.ENVIRONMENT_FILES: [
            r"^.github/workflows",
            r"^dev/breeze",
            r"^dev/.*\.py$",
            r"^Dockerfile",
            r"^scripts",
            r"^setup.py",
            r"^setup.cfg",
            r"^generated/provider_dependencies.json$",
        ],
        FileGroupForCi.PYTHON_PRODUCTION_FILES: [
            r"^airflow/.*\.py",
            r"^setup.py",
        ],
        FileGroupForCi.JAVASCRIPT_PRODUCTION_FILES: [
            r"^airflow/.*\.[jt]sx?",
            r"^airflow/.*\.lock",
        ],
        FileGroupForCi.API_TEST_FILES: [
            r"^airflow/api/",
            r"^airflow/api_connexion/",
        ],
        FileGroupForCi.API_CODEGEN_FILES: [
            r"^airflow/api_connexion/openapi/v1\.yaml",
            r"^clients/gen",
        ],
        FileGroupForCi.HELM_FILES: [
            r"^chart",
            r"^airflow/kubernetes",
            r"^tests/kubernetes",
            r"^helm_tests",
        ],
        FileGroupForCi.SETUP_FILES: [
            r"^pyproject.toml",
            r"^setup.cfg",
            r"^setup.py",
            r"^generated/provider_dependencies.json$",
        ],
        FileGroupForCi.DOC_FILES: [
            r"^docs",
            r"^\.github/SECURITY\.rst$",
            r"^airflow/.*\.py$",
            r"^chart",
            r"^providers",
            r"^tests/system",
            r"^CHANGELOG\.txt",
            r"^airflow/config_templates/config\.yml",
            r"^chart/RELEASE_NOTES\.txt",
            r"^chart/values\.schema\.json",
            r"^chart/values\.json",
        ],
        FileGroupForCi.WWW_FILES: [
            r"^airflow/www/.*\.ts[x]?$",
            r"^airflow/www/.*\.js[x]?$",
            r"^airflow/www/[^/]+\.json$",
            r"^airflow/www/.*\.lock$",
        ],
        FileGroupForCi.KUBERNETES_FILES: [
            r"^chart",
            r"^kubernetes_tests",
            r"^airflow/providers/cncf/kubernetes/",
            r"^tests/providers/cncf/kubernetes/",
            r"^tests/system/providers/cncf/kubernetes/",
        ],
        FileGroupForCi.ALL_PYTHON_FILES: [
            r".*\.py$",
        ],
        FileGroupForCi.ALL_AIRFLOW_PYTHON_FILES: [
            r".*\.py$",
        ],
        FileGroupForCi.ALL_PROVIDERS_PYTHON_FILES: [
            r"^airflow/providers/.*\.py$",
            r"^tests/providers/.*\.py$",
            r"^tests/system/providers/.*\.py$",
        ],
        FileGroupForCi.ALL_DOCS_PYTHON_FILES: [
            r"^docs/.*\.py$",
        ],
        FileGroupForCi.ALL_DEV_PYTHON_FILES: [
            r"^dev/.*\.py$",
        ],
        FileGroupForCi.ALL_SOURCE_FILES: [
            r"^.pre-commit-config.yaml$",
            r"^airflow",
            r"^chart",
            r"^tests",
            r"^kubernetes_tests",
        ],
        FileGroupForCi.SYSTEM_TEST_FILES: [
            r"^tests/system/",
        ],
        FileGroupForCi.ALWAYS_TESTS_FILES: [
            r"^tests/always/",
        ],
        FileGroupForCi.ALL_PROVIDER_YAML_FILES: [
            r".*/provider\.yaml$",
        ],
    }
)

CI_FILE_GROUP_EXCLUDES = HashableDict(
    {
        FileGroupForCi.ALL_AIRFLOW_PYTHON_FILES: [
            r"^.*/.*_vendor/.*",
            r"^airflow/migrations/.*",
            r"^airflow/providers/.*",
            r"^dev/.*",
            r"^docs/.*",
            r"^provider_packages/.*",
            r"^tests/providers/.*",
            r"^tests/system/providers/.*",
            r"^tests/dags/test_imports.py",
        ]
    }
)

TEST_TYPE_MATCHES = HashableDict(
    {
        SelectiveUnitTestTypes.API: [
            r"^airflow/api",
            r"^airflow/api_connexion",
            r"^tests/api",
            r"^tests/api_connexion",
        ],
        SelectiveUnitTestTypes.CLI: [
            r"^airflow/cli",
            r"^tests/cli",
        ],
        SelectiveUnitTestTypes.OPERATORS: [
            r"^airflow/operators",
            r"^tests/operators",
        ],
        SelectiveUnitTestTypes.PROVIDERS: [
            r"^airflow/providers/",
            r"^tests/system/providers/",
            r"^tests/providers/",
        ],
        SelectiveUnitTestTypes.PYTHON_VENV: [
            r"^tests/operators/test_python.py",
        ],
        SelectiveUnitTestTypes.BRANCH_PYTHON_VENV: [
            r"^tests/operators/test_python.py",
        ],
        SelectiveUnitTestTypes.WWW: [r"^airflow/www", r"^tests/www"],
    }
)

TEST_TYPE_EXCLUDES = HashableDict({})


def find_provider_affected(changed_file: str, include_docs: bool) -> str | None:
    file_path = AIRFLOW_SOURCES_ROOT / changed_file
    # is_relative_to is only available in Python 3.9 - we should simplify this check when we are Python 3.9+
    for provider_root in (TESTS_PROVIDERS_ROOT, SYSTEM_TESTS_PROVIDERS_ROOT, AIRFLOW_PROVIDERS_ROOT):
        try:
            file_path.relative_to(provider_root)
            relative_base_path = provider_root
            break
        except ValueError:
            pass
    else:
        if include_docs:
            try:
                relative_path = file_path.relative_to(DOCS_DIR)
                if relative_path.parts[0].startswith("apache-airflow-providers-"):
                    return relative_path.parts[0].replace("apache-airflow-providers-", "").replace("-", ".")
            except ValueError:
                pass
        return None

    for parent_dir_path in file_path.parents:
        if parent_dir_path == relative_base_path:
            break
        relative_path = parent_dir_path.relative_to(relative_base_path)
        if (AIRFLOW_PROVIDERS_ROOT / relative_path / "provider.yaml").exists():
            return str(parent_dir_path.relative_to(relative_base_path)).replace(os.sep, ".")
    # If we got here it means that some "common" files were modified. so we need to test all Providers
    return "Providers"


def find_all_providers_affected(
    changed_files: tuple[str, ...], include_docs: bool, fail_if_suspended_providers_affected: bool
) -> list[str] | str | None:
    all_providers: set[str] = set()

    all_providers_affected = False
    suspended_providers: set[str] = set()
    for changed_file in changed_files:
        provider = find_provider_affected(changed_file, include_docs=include_docs)
        if provider == "Providers":
            all_providers_affected = True
        elif provider is not None:
            if provider not in DEPENDENCIES:
                suspended_providers.add(provider)
            else:
                all_providers.add(provider)
    if all_providers_affected:
        return "ALL_PROVIDERS"
    if suspended_providers:
        # We check for suspended providers only after we have checked if all providers are affected.
        # No matter if we found that we are modifying a suspended provider individually, if all providers are
        # affected, then it means that we are ok to proceed because likely we are running some kind of
        # global refactoring that affects multiple providers including the suspended one. This is a
        # potential escape hatch if someone would like to modify suspended provider,
        # but it can be found at the review time and is anyway harmless as the provider will not be
        # released nor tested nor used in CI anyway.
        get_console().print("[yellow]You are modifying suspended providers.\n")
        get_console().print(
            "[info]Some providers modified by this change have been suspended, "
            "and before attempting such changes you should fix the reason for suspension."
        )
        get_console().print(
            "[info]When fixing it, you should set suspended = false in provider.yaml "
            "to make changes to the provider."
        )
        get_console().print(f"Suspended providers: {suspended_providers}")
        if fail_if_suspended_providers_affected:
            get_console().print(
                "[error]This PR did not have `allow suspended provider changes` label set so it will fail."
            )
            sys.exit(1)
        else:
            get_console().print(
                "[info]This PR had `allow suspended provider changes` label set so it will continue"
            )
    if not all_providers:
        return None
    for provider in list(all_providers):
        all_providers.update(
            get_related_providers(provider, upstream_dependencies=True, downstream_dependencies=True)
        )
    return sorted(all_providers)


class SelectiveChecks:
    __HASHABLE_FIELDS = {"_files", "_default_branch", "_commit_ref", "_pr_labels", "_github_event"}

    def __init__(
        self,
        files: tuple[str, ...] = (),
        default_branch="main",
        default_constraints_branch="constraints-main",
        commit_ref: str | None = None,
        pr_labels: tuple[str, ...] = (),
        github_event: GithubEvents = GithubEvents.PULL_REQUEST,
        github_repository: str = APACHE_AIRFLOW_GITHUB_REPOSITORY,
        github_actor: str = "",
        github_context_dict: dict[str, Any] | None = None,
    ):
        self._files = files
        self._default_branch = default_branch
        self._default_constraints_branch = default_constraints_branch
        self._commit_ref = commit_ref
        self._pr_labels = pr_labels
        self._github_event = github_event
        self._github_repository = github_repository
        self._github_actor = github_actor
        self._github_context_dict = github_context_dict or {}

    def __important_attributes(self) -> tuple[Any, ...]:
        return tuple(getattr(self, f) for f in self.__HASHABLE_FIELDS)

    def __hash__(self):
        return hash(self.__important_attributes())

    def __eq__(self, other):
        return isinstance(other, SelectiveChecks) and all(
            [getattr(other, f) == getattr(self, f) for f in self.__HASHABLE_FIELDS]
        )

    def __str__(self) -> str:
        from airflow_breeze.utils.github import get_ga_output

        output = []
        for field_name in dir(self):
            if not field_name.startswith("_"):
                value = getattr(self, field_name)
                if value is not None:
                    output.append(get_ga_output(field_name, value))
        return "\n".join(output)

    default_python_version = DEFAULT_PYTHON_MAJOR_MINOR_VERSION
    default_postgres_version = DEFAULT_POSTGRES_VERSION
    default_mysql_version = DEFAULT_MYSQL_VERSION
    default_mssql_version = DEFAULT_MSSQL_VERSION

    default_kubernetes_version = DEFAULT_KUBERNETES_VERSION
    default_kind_version = KIND_VERSION
    default_helm_version = HELM_VERSION

    @cached_property
    def default_branch(self) -> str:
        return self._default_branch

    @cached_property
    def default_constraints_branch(self) -> str:
        return self._default_constraints_branch

    @cached_property
    def full_tests_needed(self) -> bool:
        if not self._commit_ref:
            get_console().print("[warning]Running everything as commit is missing[/]")
            return True
        if self._github_event in [GithubEvents.PUSH, GithubEvents.SCHEDULE, GithubEvents.WORKFLOW_DISPATCH]:
            get_console().print(f"[warning]Full tests needed because event is {self._github_event}[/]")
            return True
        if self._matching_files(
            FileGroupForCi.ENVIRONMENT_FILES, CI_FILE_GROUP_MATCHES, CI_FILE_GROUP_EXCLUDES
        ):
            get_console().print("[warning]Running everything because env files changed[/]")
            return True
        if FULL_TESTS_NEEDED_LABEL in self._pr_labels:
            get_console().print(
                "[warning]Full tests needed because "
                f"label '{FULL_TESTS_NEEDED_LABEL}' is in  {self._pr_labels}[/]"
            )
            return True
        return False

    @cached_property
    def python_versions(self) -> list[str]:
        return (
            CURRENT_PYTHON_MAJOR_MINOR_VERSIONS
            if self.full_tests_needed
            else [DEFAULT_PYTHON_MAJOR_MINOR_VERSION]
        )

    @cached_property
    def python_versions_list_as_string(self) -> str:
        return " ".join(self.python_versions)

    @cached_property
    def all_python_versions(self) -> list[str]:
        return (
            ALL_PYTHON_MAJOR_MINOR_VERSIONS
            if self.full_tests_needed
            else [DEFAULT_PYTHON_MAJOR_MINOR_VERSION]
        )

    @cached_property
    def all_python_versions_list_as_string(self) -> str:
        return " ".join(self.all_python_versions)

    @cached_property
    def postgres_versions(self) -> list[str]:
        return CURRENT_POSTGRES_VERSIONS if self.full_tests_needed else [DEFAULT_POSTGRES_VERSION]

    @cached_property
    def mysql_versions(self) -> list[str]:
        return CURRENT_MYSQL_VERSIONS if self.full_tests_needed else [DEFAULT_MYSQL_VERSION]

    @cached_property
    def mssql_versions(self) -> list[str]:
        return CURRENT_MSSQL_VERSIONS if self.full_tests_needed else [DEFAULT_MSSQL_VERSION]

    @cached_property
    def kind_version(self) -> str:
        return KIND_VERSION

    @cached_property
    def helm_version(self) -> str:
        return HELM_VERSION

    @cached_property
    def postgres_exclude(self) -> list[dict[str, str]]:
        if not self.full_tests_needed:
            # Only basic combination so we do not need to exclude anything
            return []
        return [
            # Exclude all combinations that are repeating python/postgres versions
            {"python-version": python_version, "postgres-version": postgres_version}
            for python_version, postgres_version in excluded_combos(
                CURRENT_PYTHON_MAJOR_MINOR_VERSIONS, CURRENT_POSTGRES_VERSIONS
            )
        ]

    @cached_property
    def mssql_exclude(self) -> list[dict[str, str]]:
        if not self.full_tests_needed:
            # Only basic combination so we do not need to exclude anything
            return []
        return [
            # Exclude all combinations that are repeating python/mssql versions
            {"python-version": python_version, "mssql-version": mssql_version}
            for python_version, mssql_version in excluded_combos(
                CURRENT_PYTHON_MAJOR_MINOR_VERSIONS, CURRENT_MSSQL_VERSIONS
            )
        ]

    @cached_property
    def mysql_exclude(self) -> list[dict[str, str]]:
        if not self.full_tests_needed:
            # Only basic combination so we do not need to exclude anything
            return []
        return [
            # Exclude all combinations that are repeating python/mysql versions
            {"python-version": python_version, "mysql-version": mysql_version}
            for python_version, mysql_version in excluded_combos(
                CURRENT_PYTHON_MAJOR_MINOR_VERSIONS, CURRENT_MYSQL_VERSIONS
            )
        ]

    @cached_property
    def sqlite_exclude(self) -> list[dict[str, str]]:
        return []

    @cached_property
    def kubernetes_versions(self) -> list[str]:
        return CURRENT_KUBERNETES_VERSIONS if self.full_tests_needed else [DEFAULT_KUBERNETES_VERSION]

    @cached_property
    def kubernetes_versions_list_as_string(self) -> str:
        return " ".join(self.kubernetes_versions)

    @cached_property
    def kubernetes_combos_list_as_string(self) -> str:
        python_version_array: list[str] = self.python_versions_list_as_string.split(" ")
        kubernetes_version_array: list[str] = self.kubernetes_versions_list_as_string.split(" ")
        combo_titles, short_combo_titles, combos = get_kubernetes_python_combos(
            kubernetes_version_array, python_version_array
        )
        return " ".join(short_combo_titles)

    def _match_files_with_regexps(self, matched_files, matching_regexps):
        for file in self._files:
            if any(re.match(regexp, file) for regexp in matching_regexps):
                matched_files.append(file)

    def _exclude_files_with_regexps(self, matched_files, exclude_regexps):
        for file in self._files:
            if any(re.match(regexp, file) for regexp in exclude_regexps):
                if file in matched_files:
                    matched_files.remove(file)

    @lru_cache(maxsize=None)
    def _matching_files(
        self, match_group: T, match_dict: dict[T, list[str]], exclude_dict: dict[T, list[str]]
    ) -> list[str]:
        matched_files: list[str] = []
        match_regexps = match_dict[match_group]
        excluded_regexps = exclude_dict.get(match_group)
        self._match_files_with_regexps(matched_files, match_regexps)
        if excluded_regexps:
            self._exclude_files_with_regexps(matched_files, excluded_regexps)
        count = len(matched_files)
        if count > 0:
            get_console().print(f"[warning]{match_group} matched {count} files.[/]")
            get_console().print(matched_files)
        else:
            get_console().print(f"[warning]{match_group} did not match any file.[/]")
        return matched_files

    def _should_be_run(self, source_area: FileGroupForCi) -> bool:
        if self.full_tests_needed:
            get_console().print(f"[warning]{source_area} enabled because we are running everything[/]")
            return True
        matched_files = self._matching_files(source_area, CI_FILE_GROUP_MATCHES, CI_FILE_GROUP_EXCLUDES)
        if matched_files:
            get_console().print(
                f"[warning]{source_area} enabled because it matched {len(matched_files)} changed files[/]"
            )
            return True
        else:
            get_console().print(
                f"[warning]{source_area} disabled because it did not match any changed files[/]"
            )
            return False

    @cached_property
    def needs_python_scans(self) -> bool:
        return self._should_be_run(FileGroupForCi.PYTHON_PRODUCTION_FILES)

    @cached_property
    def needs_javascript_scans(self) -> bool:
        return self._should_be_run(FileGroupForCi.JAVASCRIPT_PRODUCTION_FILES)

    @cached_property
    def needs_api_tests(self) -> bool:
        return self._should_be_run(FileGroupForCi.API_TEST_FILES)

    @cached_property
    def needs_api_codegen(self) -> bool:
        return self._should_be_run(FileGroupForCi.API_CODEGEN_FILES)

    @cached_property
    def run_www_tests(self) -> bool:
        return self._should_be_run(FileGroupForCi.WWW_FILES)

    @cached_property
    def run_amazon_tests(self) -> bool:
        if self.parallel_test_types_list_as_string is None:
            return False
        return (
            "amazon" in self.parallel_test_types_list_as_string
            or "Providers" in self.parallel_test_types_list_as_string.split(" ")
        )

    @cached_property
    def run_kubernetes_tests(self) -> bool:
        return self._should_be_run(FileGroupForCi.KUBERNETES_FILES)

    @cached_property
    def docs_build(self) -> bool:
        return self._should_be_run(FileGroupForCi.DOC_FILES)

    @cached_property
    def needs_helm_tests(self) -> bool:
        return self._should_be_run(FileGroupForCi.HELM_FILES) and self._default_branch == "main"

    @cached_property
    def run_tests(self) -> bool:
        return self._should_be_run(FileGroupForCi.ALL_SOURCE_FILES)

    @cached_property
    def ci_image_build(self) -> bool:
        return self.run_tests or self.docs_build or self.run_kubernetes_tests or self.needs_helm_tests

    @cached_property
    def prod_image_build(self) -> bool:
        return self.run_kubernetes_tests or self.needs_helm_tests

    def _select_test_type_if_matching(
        self, test_types: set[str], test_type: SelectiveUnitTestTypes
    ) -> list[str]:
        matched_files = self._matching_files(test_type, TEST_TYPE_MATCHES, TEST_TYPE_EXCLUDES)
        count = len(matched_files)
        if count > 0:
            test_types.add(test_type.value)
            get_console().print(f"[warning]{test_type} added because it matched {count} files[/]")
        return matched_files

    def _are_all_providers_affected(self) -> bool:
        # if "Providers" test is present in the list of tests, it means that we should run all providers tests
        # prepare all providers packages and build all providers documentation
        return "Providers" in self._get_test_types_to_run()

    def _fail_if_suspended_providers_affected(self):
        return "allow suspended provider changes" not in self._pr_labels

    def _get_test_types_to_run(self) -> list[str]:
        if self.full_tests_needed:
            return list(all_selective_test_types())

        candidate_test_types: set[str] = {"Always"}
        matched_files: set[str] = set()
        matched_files.update(
            self._select_test_type_if_matching(candidate_test_types, SelectiveUnitTestTypes.WWW)
        )
        matched_files.update(
            self._select_test_type_if_matching(candidate_test_types, SelectiveUnitTestTypes.PROVIDERS)
        )
        matched_files.update(
            self._select_test_type_if_matching(candidate_test_types, SelectiveUnitTestTypes.CLI)
        )
        matched_files.update(
            self._select_test_type_if_matching(candidate_test_types, SelectiveUnitTestTypes.OPERATORS)
        )
        matched_files.update(
            self._select_test_type_if_matching(candidate_test_types, SelectiveUnitTestTypes.API)
        )

        kubernetes_files = self._matching_files(
            FileGroupForCi.KUBERNETES_FILES, CI_FILE_GROUP_MATCHES, CI_FILE_GROUP_EXCLUDES
        )
        system_test_files = self._matching_files(
            FileGroupForCi.SYSTEM_TEST_FILES, CI_FILE_GROUP_MATCHES, CI_FILE_GROUP_EXCLUDES
        )
        all_source_files = self._matching_files(
            FileGroupForCi.ALL_SOURCE_FILES, CI_FILE_GROUP_MATCHES, CI_FILE_GROUP_EXCLUDES
        )
        test_always_files = self._matching_files(
            FileGroupForCi.ALWAYS_TESTS_FILES, CI_FILE_GROUP_MATCHES, CI_FILE_GROUP_EXCLUDES
        )
        remaining_files = (
            set(all_source_files)
            - set(matched_files)
            - set(kubernetes_files)
            - set(system_test_files)
            - set(test_always_files)
        )
        get_console().print(f"[warning]Remaining non test/always files: {len(remaining_files)}[/]")
        count_remaining_files = len(remaining_files)

        if count_remaining_files > 0:
            get_console().print(
                f"[warning]We should run all tests. There are {count_remaining_files} changed "
                "files that seems to fall into Core/Other category[/]"
            )
            get_console().print(remaining_files)
            candidate_test_types.update(all_selective_test_types())
        else:
            if "Providers" in candidate_test_types:
                affected_providers = find_all_providers_affected(
                    changed_files=self._files,
                    include_docs=False,
                    fail_if_suspended_providers_affected=self._fail_if_suspended_providers_affected(),
                )
                if affected_providers != "ALL_PROVIDERS" and affected_providers is not None:
                    candidate_test_types.remove("Providers")
                    candidate_test_types.add(f"Providers[{','.join(sorted(affected_providers))}]")
            get_console().print(
                "[warning]There are no core/other files. Only tests relevant to the changed files are run.[/]"
            )
        # sort according to predefined order
        sorted_candidate_test_types = sorted(candidate_test_types)
        get_console().print("[warning]Selected test type candidates to run:[/]")
        get_console().print(sorted_candidate_test_types)
        return sorted_candidate_test_types

    @staticmethod
    def _extract_long_provider_tests(current_test_types: set[str]):
        """
        In case there are Provider tests in the list of test to run - either in the form of
        Providers or Providers[...] we subtract them from the test type,
        and add them to the list of tests to run individually.

        In case of Providers, we need to replace it with Providers[-<list_of_long_tests>], but
        in case of Providers[list_of_tests] we need to remove the long tests from the list.

        """
        long_tests = ["amazon", "google"]
        for original_test_type in tuple(current_test_types):
            if original_test_type == "Providers":
                current_test_types.remove(original_test_type)
                for long_test in long_tests:
                    current_test_types.add(f"Providers[{long_test}]")
                current_test_types.add(f"Providers[-{','.join(long_tests)}]")
            elif original_test_type.startswith("Providers["):
                provider_tests_to_run = (
                    original_test_type.replace("Providers[", "").replace("]", "").split(",")
                )
                if any(long_test in provider_tests_to_run for long_test in long_tests):
                    current_test_types.remove(original_test_type)
                    for long_test in long_tests:
                        if long_test in provider_tests_to_run:
                            current_test_types.add(f"Providers[{long_test}]")
                            provider_tests_to_run.remove(long_test)
                    current_test_types.add(f"Providers[{','.join(provider_tests_to_run)}]")

    @cached_property
    def parallel_test_types_list_as_string(self) -> str | None:
        if not self.run_tests:
            return None
        current_test_types = set(self._get_test_types_to_run())
        if self._default_branch != "main":
            test_types_to_remove: set[str] = set()
            for test_type in current_test_types:
                if test_type.startswith("Providers"):
                    get_console().print(
                        f"[warning]Removing {test_type} because the target branch "
                        f"is {self._default_branch} and not main[/]"
                    )
                    test_types_to_remove.add(test_type)
            current_test_types = current_test_types - test_types_to_remove

        self._extract_long_provider_tests(current_test_types)
        return " ".join(sorted(current_test_types))

    @cached_property
    def basic_checks_only(self) -> bool:
        return not self.ci_image_build

    @cached_property
    def upgrade_to_newer_dependencies(self) -> bool:
        return (
            len(
                self._matching_files(
                    FileGroupForCi.SETUP_FILES, CI_FILE_GROUP_MATCHES, CI_FILE_GROUP_EXCLUDES
                )
            )
            > 0
            or self._github_event in [GithubEvents.PUSH, GithubEvents.SCHEDULE]
            or UPGRADE_TO_NEWER_DEPENDENCIES_LABEL in self._pr_labels
        )

    @cached_property
    def docs_list_as_string(self) -> str | None:
        _ALL_DOCS_LIST = ""
        if not self.docs_build:
            return None
        if self._default_branch != "main":
            return "apache-airflow docker-stack"
        if self.full_tests_needed:
            return _ALL_DOCS_LIST
        providers_affected = find_all_providers_affected(
            changed_files=self._files,
            include_docs=True,
            fail_if_suspended_providers_affected=self._fail_if_suspended_providers_affected(),
        )
        if (
            providers_affected == "ALL_PROVIDERS"
            or "docs/conf.py" in self._files
            or "docs/build_docs.py" in self._files
            or self._are_all_providers_affected()
        ):
            return _ALL_DOCS_LIST
        packages = []
        if any(file.startswith(("airflow/", "docs/apache-airflow/")) for file in self._files):
            packages.append("apache-airflow")
        if any(file.startswith("docs/apache-airflow-providers/") for file in self._files):
            packages.append("apache-airflow-providers")
        if any(file.startswith(("chart/", "docs/helm-chart")) for file in self._files):
            packages.append("helm-chart")
        if any(file.startswith("docs/docker-stack/") for file in self._files):
            packages.append("docker-stack")
        if providers_affected:
            for provider in providers_affected:
                packages.append(provider.replace("-", "."))
        return " ".join(packages)

    @cached_property
    def skip_pre_commits(self) -> str:
        pre_commits_to_skip = set()
        pre_commits_to_skip.add("identity")
        if self._default_branch != "main":
            # Skip those tests on all "release" branches
            pre_commits_to_skip.update(
                (
                    "check-airflow-provider-compatibility",
                    "check-extra-packages-references",
                    "check-provider-yaml-valid",
                    "lint-helm-chart",
                    "mypy-providers",
                )
            )
        if self.full_tests_needed:
            # when full tests are needed, we do not want to skip any checks and we should
            # run all the pre-commits just to be sure everything is ok when some structural changes occurred
            return ",".join(sorted(pre_commits_to_skip))
        if not self._matching_files(
            FileGroupForCi.ALL_PROVIDERS_PYTHON_FILES, CI_FILE_GROUP_MATCHES, CI_FILE_GROUP_EXCLUDES
        ):
            pre_commits_to_skip.add("mypy-providers")
        if not self._matching_files(
            FileGroupForCi.ALL_AIRFLOW_PYTHON_FILES, CI_FILE_GROUP_MATCHES, CI_FILE_GROUP_EXCLUDES
        ):
            pre_commits_to_skip.add("mypy-core")
        if not self._matching_files(
            FileGroupForCi.ALL_DOCS_PYTHON_FILES, CI_FILE_GROUP_MATCHES, CI_FILE_GROUP_EXCLUDES
        ):
            pre_commits_to_skip.add("mypy-docs")
        if not self._matching_files(
            FileGroupForCi.ALL_DEV_PYTHON_FILES, CI_FILE_GROUP_MATCHES, CI_FILE_GROUP_EXCLUDES
        ):
            pre_commits_to_skip.add("mypy-dev")
        if not self._matching_files(FileGroupForCi.WWW_FILES, CI_FILE_GROUP_MATCHES, CI_FILE_GROUP_EXCLUDES):
            pre_commits_to_skip.add("ts-compile-format-lint-www")
        if not self._matching_files(
            FileGroupForCi.ALL_PYTHON_FILES, CI_FILE_GROUP_MATCHES, CI_FILE_GROUP_EXCLUDES
        ):
            pre_commits_to_skip.add("flynt")
        if not self._matching_files(
            FileGroupForCi.HELM_FILES,
            CI_FILE_GROUP_MATCHES,
            CI_FILE_GROUP_EXCLUDES,
        ):
            pre_commits_to_skip.add("lint-helm-chart")
        if not (
            self._matching_files(
                FileGroupForCi.ALL_PROVIDER_YAML_FILES, CI_FILE_GROUP_MATCHES, CI_FILE_GROUP_EXCLUDES
            )
            or self._matching_files(
                FileGroupForCi.ALL_PROVIDERS_PYTHON_FILES, CI_FILE_GROUP_MATCHES, CI_FILE_GROUP_EXCLUDES
            )
        ):
            # only skip provider validation if none of the provider.yaml and provider
            # python files changed because validation also walks through all the provider python files
            pre_commits_to_skip.add("check-provider-yaml-valid")
        return ",".join(sorted(pre_commits_to_skip))

    @cached_property
    def skip_provider_tests(self) -> bool:
        if self._default_branch != "main":
            return True
        if self.full_tests_needed:
            return False
        if any(test_type.startswith("Providers") for test_type in self._get_test_types_to_run()):
            return False
        return True

    @cached_property
    def cache_directive(self) -> str:
        return "disabled" if self._github_event == GithubEvents.SCHEDULE else "registry"

    @cached_property
    def debug_resources(self) -> bool:
        return DEBUG_CI_RESOURCES_LABEL in self._pr_labels

    @cached_property
    def helm_test_packages(self) -> str:
        return json.dumps(all_helm_test_packages())

    @cached_property
    def affected_providers_list_as_string(self) -> str | None:
        _ALL_PROVIDERS_LIST = ""
        if self.full_tests_needed:
            return _ALL_PROVIDERS_LIST
        if self._are_all_providers_affected():
            return _ALL_PROVIDERS_LIST
        affected_providers = find_all_providers_affected(
            changed_files=self._files,
            include_docs=True,
            fail_if_suspended_providers_affected=self._fail_if_suspended_providers_affected(),
        )
        if not affected_providers:
            return None
        if affected_providers == "ALL_PROVIDERS":
            return _ALL_PROVIDERS_LIST
        return " ".join(sorted(affected_providers))

    @cached_property
    def runs_on(self) -> str:
        if self._github_repository == APACHE_AIRFLOW_GITHUB_REPOSITORY:
            if self._github_event in [GithubEvents.SCHEDULE, GithubEvents.PUSH]:
                return RUNS_ON_SELF_HOSTED_RUNNER
            actor = self._github_actor
            if self._github_event in (GithubEvents.PULL_REQUEST, GithubEvents.PULL_REQUEST_TARGET):
                try:
                    actor = self._github_context_dict["event"]["pull_request"]["user"]["login"]
                    get_console().print(
                        f"[warning]The actor: {actor} retrieved from GITHUB_CONTEXT's"
                        f" event.pull_request.user.login[/]"
                    )
                except Exception as e:
                    get_console().print(f"[warning]Exception when reading user login: {e}[/]")
                    get_console().print(
                        f"[info]Could not find the actor from pull request, "
                        f"falling back to the actor who triggered the PR: {actor}[/]"
                    )
            if actor in COMMITTERS and USE_PUBLIC_RUNNERS_LABEL not in self._pr_labels:
                return RUNS_ON_SELF_HOSTED_RUNNER
        return RUNS_ON_PUBLIC_RUNNER

    @cached_property
    def is_self_hosted_runner(self) -> bool:
        """
        True if the job has runs_on labels indicating It should run on "self-hosted" runner.

        All self-hosted runners have "self-hosted" label.
        """
        return "self-hosted" in json.loads(self.runs_on)

    @cached_property
    def is_airflow_runner(self) -> bool:
        """
        True if the job has runs_on labels indicating It should run on Airflow managed runner.

        All Airflow team-managed runners will have "airflow-runner" label.
        """
        # TODO: when we have it properly set-up with labels we should just check for
        #       "airflow-runner" presence in runs_on
        runs_on_array = json.loads(self.runs_on)
        return "Linux" in runs_on_array and "X64" in runs_on_array and "self-hosted" in runs_on_array

    @cached_property
    def is_amd_runner(self) -> bool:
        """
        True if the job has runs_on labels indicating AMD architecture.

        Matching amd label, asf-runner, and any ubuntu that does not contain arm
        The last case is just in case - currently there are no public runners that have ARM
        instances, but they can add them in the future. It might be that for compatibility
        they will just add arm in the runner name - because currently GitHub users use just
        one label "ubuntu-*" for all their work and depend on them being AMD ones.
        """
        return any(
            [
                "amd" == label.lower()
                or "amd64" == label.lower()
                or "x64" == label.lower()
                or "asf-runner" == label
                or ("ubuntu" in label and "arm" not in label.lower())
                for label in json.loads(self.runs_on)
            ]
        )

    @cached_property
    def is_arm_runner(self) -> bool:
        """
        True if the job has runs_on labels indicating ARM architecture.

        Matches any label containing arm - including ASF-specific "asf-arm" label.

        # See https://cwiki.apache.org/confluence/pages/viewpage.action?spaceKey=INFRA&title=ASF+Infra+provided+self-hosted+runners
        """
        return any(
            [
                "arm" == label.lower() or "arm64" == label.lower() or "asf-arm" == label
                for label in json.loads(self.runs_on)
            ]
        )

    @cached_property
    def is_vm_runner(self) -> bool:
        """Whether the runner is VM runner (managed by airflow)."""
        # TODO: when we have it properly set-up with labels we should just check for
        #       "airflow-runner" presence in runs_on
        return self.is_airflow_runner

    @cached_property
    def is_k8s_runner(self) -> bool:
        """Whether the runner is K8s runner (managed by airflow)."""
        # TODO: when we have it properly set-up with labels we should just check for
        #       "k8s-runner" presence in runs_on
        return False

    @cached_property
    def mssql_parallelism(self) -> int:
        # Limit parallelism for MSSQL to 1 for public runners due to race conditions generated there
        return SELF_HOSTED_RUNNERS_CPU_COUNT if self.runs_on == RUNS_ON_SELF_HOSTED_RUNNER else 1

    @cached_property
    def has_migrations(self) -> bool:
        return any([file.startswith("airflow/migrations/") for file in self._files])
