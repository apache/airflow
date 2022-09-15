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
import sys
from enum import Enum

from airflow_breeze.utils.github_actions import get_ga_output
from airflow_breeze.utils.kubernetes_utils import get_kubernetes_python_combos
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    # noinspection PyUnresolvedReferences
    from cached_property import cached_property

from functools import lru_cache
from re import match
from typing import Any, Dict, List, TypeVar

from airflow_breeze.global_constants import (
    ALL_PYTHON_MAJOR_MINOR_VERSIONS,
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
    GithubEvents,
    SelectiveUnitTestTypes,
    all_selective_test_types,
)
from airflow_breeze.utils.console import get_console

FULL_TESTS_NEEDED_LABEL = "full tests needed"


class FileGroupForCi(Enum):
    ENVIRONMENT_FILES = "environment_files"
    PYTHON_PRODUCTION_FILES = "python_scans"
    JAVASCRIPT_PRODUCTION_FILES = "javascript_scans"
    API_TEST_FILES = "api_test_files"
    API_CODEGEN_FILES = "api_codegen_files"
    HELM_FILES = "helm_files"
    SETUP_FILES = "setup_files"
    DOC_FILES = "doc_files"
    UI_FILES = "ui_files"
    WWW_FILES = "www_files"
    KUBERNETES_FILES = "kubernetes_files"
    ALL_PYTHON_FILES = "all_python_files"
    ALL_SOURCE_FILES = "all_sources_for_tests"


T = TypeVar('T', FileGroupForCi, SelectiveUnitTestTypes)


class HashableDict(Dict[T, List[str]]):
    def __hash__(self):
        return hash(frozenset(self))


CI_FILE_GROUP_MATCHES = HashableDict(
    {
        FileGroupForCi.ENVIRONMENT_FILES: [
            r"^.github/workflows",
            r"^dev/breeze",
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
            r"^airflow/api",
        ],
        FileGroupForCi.API_CODEGEN_FILES: [
            "^airflow/api_connexion/openapi/v1.yaml",
            "^clients/gen",
        ],
        FileGroupForCi.HELM_FILES: [
            "^chart",
            "^airflow/kubernetes",
            "^tests/kubernetes",
        ],
        FileGroupForCi.SETUP_FILES: [
            r"^pyproject.toml",
            r"^setup.cfg",
            r"^setup.py",
            r"^generated/provider_dependencies.json$",
            r"^airflow/providers/.*/provider.yaml$",
        ],
        FileGroupForCi.DOC_FILES: [
            r"^docs",
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
        FileGroupForCi.UI_FILES: [
            r"^airflow/www/.*\.[tj]sx?$",
            r"^airflow/www/[^/]+\.json$",
            r"^airflow/www/.*\.lock$",
        ],
        FileGroupForCi.WWW_FILES: [
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
            r"\.py$",
        ],
        FileGroupForCi.ALL_SOURCE_FILES: [
            "^.pre-commit-config.yaml$",
            "^airflow",
            "^chart",
            "^tests",
            "^kubernetes_tests",
        ],
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
        SelectiveUnitTestTypes.PROVIDERS: [
            "^airflow/providers/",
            "^tests/providers/",
            "^tests/system/",
        ],
        SelectiveUnitTestTypes.WWW: ["^airflow/www", "^tests/www"],
    }
)

TESTS_PROVIDERS_ROOT = AIRFLOW_SOURCES_ROOT / "tests" / "providers"
SYSTEM_TESTS_PROVIDERS_ROOT = AIRFLOW_SOURCES_ROOT / "tests" / "system" / "providers"
AIRFLOW_PROVIDERS_ROOT = AIRFLOW_SOURCES_ROOT / "airflow" / "providers"


def find_provider_affected(changed_file: str) -> str | None:
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
        return None

    for parent_dir_path in file_path.parents:
        if parent_dir_path == relative_base_path:
            break
        relative_path = parent_dir_path.relative_to(relative_base_path)
        if (AIRFLOW_PROVIDERS_ROOT / relative_path / "provider.yaml").exists():
            return str(parent_dir_path.relative_to(relative_base_path)).replace(os.sep, ".")
    # If we got here it means that some "common" files were modified. so we need to test all Providers
    return "Providers"


def add_dependent_providers(
    providers: set[str], provider_to_check: str, dependencies: dict[str, dict[str, list[str]]]
):
    for provider, provider_info in dependencies.items():
        if provider_to_check in provider_info['cross-providers-deps']:
            providers.add(provider)


def find_all_providers_affected(changed_files: tuple[str, ...]) -> set[str]:
    all_providers: set[str] = set()
    for changed_file in changed_files:
        provider = find_provider_affected(changed_file)
        if provider == "Providers":
            return set()
        if provider is not None:
            all_providers.add(provider)
    dependencies = json.loads((AIRFLOW_SOURCES_ROOT / "generated" / "provider_dependencies.json").read_text())
    for provider in list(all_providers):
        add_dependent_providers(all_providers, provider, dependencies)
    return all_providers


class SelectiveChecks:
    __HASHABLE_FIELDS = {'_files', '_default_branch', '_commit_ref', "_pr_labels", "_github_event"}

    def __init__(
        self,
        files: tuple[str, ...] = (),
        default_branch="main",
        default_constraints_branch="constraints-main",
        commit_ref: str | None = None,
        pr_labels: tuple[str, ...] = (),
        github_event: GithubEvents = GithubEvents.PULL_REQUEST,
    ):
        self._files = files
        self._default_branch = default_branch
        self._default_constraints_branch = default_constraints_branch
        self._commit_ref = commit_ref
        self._pr_labels = pr_labels
        self._github_event = github_event

    def __important_attributes(self) -> tuple[Any, ...]:
        return tuple(getattr(self, f) for f in self.__HASHABLE_FIELDS)

    def __hash__(self):
        return hash(self.__important_attributes())

    def __eq__(self, other):
        return isinstance(other, SelectiveChecks) and all(
            [getattr(other, f) == getattr(self, f) for f in self.__HASHABLE_FIELDS]
        )

    def __str__(self) -> str:
        output = []
        for field_name in dir(self):
            if not field_name.startswith('_'):
                output.append(get_ga_output(field_name, getattr(self, field_name)))
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
    def _full_tests_needed(self) -> bool:
        if self._github_event in [GithubEvents.PUSH, GithubEvents.SCHEDULE, GithubEvents.WORKFLOW_DISPATCH]:
            get_console().print(f"[warning]Full tests needed because event is {self._github_event}[/]")
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
            if self._full_tests_needed
            else [DEFAULT_PYTHON_MAJOR_MINOR_VERSION]
        )

    @cached_property
    def python_versions_list_as_string(self) -> str:
        return " ".join(self.python_versions)

    @cached_property
    def all_python_versions(self) -> list[str]:
        return (
            ALL_PYTHON_MAJOR_MINOR_VERSIONS
            if self._run_everything or self._full_tests_needed
            else [DEFAULT_PYTHON_MAJOR_MINOR_VERSION]
        )

    @cached_property
    def all_python_versions_list_as_string(self) -> str:
        return " ".join(self.all_python_versions)

    @cached_property
    def postgres_versions(self) -> list[str]:
        return CURRENT_POSTGRES_VERSIONS if self._full_tests_needed else [DEFAULT_POSTGRES_VERSION]

    @cached_property
    def mysql_versions(self) -> list[str]:
        return CURRENT_MYSQL_VERSIONS if self._full_tests_needed else [DEFAULT_MYSQL_VERSION]

    @cached_property
    def mssql_versions(self) -> list[str]:
        return CURRENT_MSSQL_VERSIONS if self._full_tests_needed else [DEFAULT_MSSQL_VERSION]

    @cached_property
    def kind_version(self) -> str:
        return KIND_VERSION

    @cached_property
    def helm_version(self) -> str:
        return HELM_VERSION

    @cached_property
    def postgres_exclude(self) -> list[dict[str, str]]:
        return [{"python-version": "3.7"}] if self._full_tests_needed else []

    @cached_property
    def mssql_exclude(self) -> list[dict[str, str]]:
        return [{"python-version": "3.8"}] if self._full_tests_needed else []

    @cached_property
    def mysql_exclude(self) -> list[dict[str, str]]:
        return [{"python-version": "3.10"}] if self._full_tests_needed else []

    @cached_property
    def sqlite_exclude(self) -> list[dict[str, str]]:
        return [{"python-version": "3.9"}] if self._full_tests_needed else []

    @cached_property
    def kubernetes_versions(self) -> list[str]:
        return CURRENT_KUBERNETES_VERSIONS if self._full_tests_needed else [DEFAULT_KUBERNETES_VERSION]

    @cached_property
    def kubernetes_versions_list_as_string(self) -> str:
        return " ".join(self.kubernetes_versions)

    @cached_property
    def kubernetes_combos(self) -> str:
        python_version_array: list[str] = self.python_versions_list_as_string.split(" ")
        kubernetes_version_array: list[str] = self.kubernetes_versions_list_as_string.split(" ")
        combo_titles, short_combo_titles, combos = get_kubernetes_python_combos(
            kubernetes_version_array, python_version_array
        )
        return " ".join(short_combo_titles)

    def _match_files_with_regexps(self, matched_files, regexps):
        for file in self._files:
            for regexp in regexps:
                if match(regexp, file):
                    matched_files.append(file)
                    break

    @lru_cache(maxsize=None)
    def _matching_files(self, match_group: T, match_dict: dict[T, list[str]]) -> list[str]:
        matched_files: list[str] = []
        regexps = match_dict[match_group]
        self._match_files_with_regexps(matched_files, regexps)
        count = len(matched_files)
        if count > 0:
            get_console().print(f"[warning]{match_group} matched {count} files.[/]")
            get_console().print(matched_files)
        else:
            get_console().print(f"[warning]{match_group} did not match any file.[/]")
        return matched_files

    @cached_property
    def _run_everything(self) -> bool:
        if not self._commit_ref:
            get_console().print("[warning]Running everything as commit is missing[/]")
            return True
        if self._full_tests_needed:
            get_console().print("[warning]Running everything as full tests are needed[/]")
            return True
        if len(self._matching_files(FileGroupForCi.ENVIRONMENT_FILES, CI_FILE_GROUP_MATCHES)) > 0:
            get_console().print("[warning]Running everything because env files changed[/]")
            return True
        return False

    def _should_be_run(self, source_area: FileGroupForCi) -> bool:
        if self._run_everything:
            get_console().print(f"[warning]{source_area} enabled because we are running everything[/]")
            return True
        matched_files = self._matching_files(source_area, CI_FILE_GROUP_MATCHES)
        if len(matched_files) > 0:
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
    def run_ui_tests(self) -> bool:
        return self._should_be_run(FileGroupForCi.UI_FILES)

    @cached_property
    def run_www_tests(self) -> bool:
        return self._should_be_run(FileGroupForCi.WWW_FILES)

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
    def image_build(self) -> bool:
        return self.run_tests or self.docs_build or self.run_kubernetes_tests

    def _select_test_type_if_matching(
        self, test_types: set[str], test_type: SelectiveUnitTestTypes
    ) -> list[str]:
        matched_files = self._matching_files(test_type, TEST_TYPE_MATCHES)
        count = len(matched_files)
        if count > 0:
            test_types.add(test_type.value)
            get_console().print(f"[warning]{test_type} added because it matched {count} files[/]")
        return matched_files

    def _get_test_types_to_run(self) -> list[str]:
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
            self._select_test_type_if_matching(candidate_test_types, SelectiveUnitTestTypes.API)
        )

        kubernetes_files = self._matching_files(FileGroupForCi.KUBERNETES_FILES, CI_FILE_GROUP_MATCHES)
        all_source_files = self._matching_files(FileGroupForCi.ALL_SOURCE_FILES, CI_FILE_GROUP_MATCHES)

        remaining_files = set(all_source_files) - set(matched_files) - set(kubernetes_files)
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
                affected_providers = find_all_providers_affected(changed_files=self._files)
                if len(affected_providers) != 0:
                    candidate_test_types.remove("Providers")
                    candidate_test_types.add(f"Providers[{','.join(sorted(affected_providers))}]")
            get_console().print(
                "[warning]There are no core/other files. Only tests relevant to the changed files are run.[/]"
            )
        sorted_candidate_test_types = list(sorted(candidate_test_types))
        get_console().print("[warning]Selected test type candidates to run:[/]")
        get_console().print(sorted_candidate_test_types)
        return sorted_candidate_test_types

    @cached_property
    def test_types(self) -> str:
        if not self.run_tests:
            return ""
        if self._run_everything:
            current_test_types = set(all_selective_test_types())
        else:
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
            if "Integration" in current_test_types:
                get_console().print(
                    "[warning]Removing 'Integration' because the target branch "
                    f"is {self._default_branch} and not main[/]"
                )
                test_types_to_remove.add("Integration")
            current_test_types = current_test_types - test_types_to_remove
        return " ".join(sorted(current_test_types))

    @cached_property
    def basic_checks_only(self) -> bool:
        return not self.image_build

    @cached_property
    def upgrade_to_newer_dependencies(self) -> bool:
        return len(
            self._matching_files(FileGroupForCi.SETUP_FILES, CI_FILE_GROUP_MATCHES)
        ) > 0 or self._github_event in [GithubEvents.PUSH, GithubEvents.SCHEDULE]

    @cached_property
    def docs_filter(self) -> str:
        return (
            ""
            if self._default_branch == 'main'
            else "--package-filter apache-airflow --package-filter docker-stack"
        )

    @cached_property
    def skip_pre_commits(self) -> str:
        return "identity" if self._default_branch == "main" else "identity,check-airflow-2-2-compatibility"

    @cached_property
    def cache_directive(self) -> str:
        return "disabled" if self._github_event == GithubEvents.SCHEDULE else "registry"
