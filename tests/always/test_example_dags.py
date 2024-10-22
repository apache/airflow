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

import os
import re
import sys
from glob import glob
from importlib import metadata as importlib_metadata
from pathlib import Path

import pytest
from packaging.specifiers import SpecifierSet
from packaging.version import Version

from airflow.models import DagBag
from airflow.utils import yaml

from tests_common.test_utils.asserts import assert_queries_count

AIRFLOW_SOURCES_ROOT = Path(__file__).resolve().parents[2]
AIRFLOW_PROVIDERS_ROOT = AIRFLOW_SOURCES_ROOT / "airflow" / "providers"
CURRENT_PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"
PROVIDERS_PREFIXES = ("providers/src/airflow/providers/", "providers/tests/system/")
OPTIONAL_PROVIDERS_DEPENDENCIES: dict[str, dict[str, str | None]] = {
    # Some examples or system tests may depend on additional packages
    # that are not included in certain CI checks.
    # The format of the dictionary is as follows:
    # key: the regexp matching the file to be excluded,
    # value: a dictionary containing package distributions with an optional version specifier, e.g., >=2.3.4
    ".*example_bedrock_retrieve_and_generate.py": {"opensearch-py": None},
    ".*example_opensearch.py": {"opensearch-py": None},
    r".*example_yandexcloud.*\.py": {"yandexcloud": None},
}
IGNORE_AIRFLOW_PROVIDER_DEPRECATION_WARNING: tuple[str, ...] = (
    # Certain examples or system tests may trigger AirflowProviderDeprecationWarnings.
    # Generally, these should be resolved as soon as a parameter or operator is deprecated.
    # If the deprecation is postponed, the item should be added to this tuple,
    # and a corresponding Issue should be created on GitHub.
    "providers/tests/system/google/cloud/bigquery/example_bigquery_operations.py",
    "providers/tests/system/google/cloud/dataflow/example_dataflow_sql.py",
    "providers/tests/system/google/cloud/dataproc/example_dataproc_gke.py",
    "providers/tests/system/google/cloud/datapipelines/example_datapipeline.py",
    "providers/tests/system/google/cloud/gcs/example_gcs_sensor.py",
    "providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine.py",
    "providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_async.py",
    "providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_job.py",
    "providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_kueue.py",
    "providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_resource.py",
    "providers/tests/system/google/cloud/life_sciences/example_life_sciences.py",
    # Deprecated Operators/Hooks, which replaced by common.sql Operators/Hooks
)


def match_optional_dependencies(distribution_name: str, specifier: str | None) -> tuple[bool, str]:
    try:
        package_version = Version(importlib_metadata.version(distribution_name))
    except ImportError:
        return False, f"{distribution_name!r} not installed."

    if specifier and package_version not in SpecifierSet(specifier):
        return False, f"{distribution_name!r} required {specifier}, but installed {package_version}."

    return True, ""


def get_suspended_providers_folders() -> list[str]:
    """
    Returns a list of suspended providers folders that should be
    skipped when running tests (without any prefix - for example apache/beam, yandex, google etc.).
    """
    suspended_providers = []
    for provider_path in AIRFLOW_PROVIDERS_ROOT.rglob("provider.yaml"):
        provider_yaml = yaml.safe_load(provider_path.read_text())
        if provider_yaml["state"] == "suspended":
            suspended_providers.append(
                provider_path.parent.relative_to(AIRFLOW_SOURCES_ROOT)
                .as_posix()
                .replace("providers/src/airflow/providers/", "")
            )
    return suspended_providers


def get_python_excluded_providers_folders() -> list[str]:
    """
    Returns a list of providers folders that should be excluded for current Python version and
    skipped when running tests (without any prefix - for example apache/beam, yandex, google etc.).
    """
    excluded_providers = []
    for provider_path in AIRFLOW_PROVIDERS_ROOT.rglob("provider.yaml"):
        provider_yaml = yaml.safe_load(provider_path.read_text())
        excluded_python_versions = provider_yaml.get("excluded-python-versions", [])
        if CURRENT_PYTHON_VERSION in excluded_python_versions:
            excluded_providers.append(
                provider_path.parent.relative_to(AIRFLOW_SOURCES_ROOT)
                .as_posix()
                .replace("providers/src/airflow/providers/", "")
            )
    return excluded_providers


def example_not_excluded_dags(xfail_db_exception: bool = False):
    example_dirs = ["airflow/**/example_dags/example_*.py", "tests/system/**/example_*.py"]

    suspended_providers_folders = get_suspended_providers_folders()
    current_python_excluded_providers_folders = get_python_excluded_providers_folders()
    suspended_providers_folders = [
        AIRFLOW_SOURCES_ROOT.joinpath(prefix, provider).as_posix()
        for prefix in PROVIDERS_PREFIXES
        for provider in suspended_providers_folders
    ]
    current_python_excluded_providers_folders = [
        AIRFLOW_SOURCES_ROOT.joinpath(prefix, provider).as_posix()
        for prefix in PROVIDERS_PREFIXES
        for provider in current_python_excluded_providers_folders
    ]
    providers_folders = tuple([AIRFLOW_SOURCES_ROOT.joinpath(pp).as_posix() for pp in PROVIDERS_PREFIXES])

    default_branch = os.environ.get("DEFAULT_BRANCH", "main")
    include_providers = default_branch == "main"

    for example_dir in example_dirs:
        if not include_providers and "providers/" in example_dir:
            print(f"Skipping {example_dir} because providers are not included for {default_branch} branch.")
            continue
        candidates = glob(f"{AIRFLOW_SOURCES_ROOT.as_posix()}/{example_dir}", recursive=True)
        for candidate in sorted(candidates):
            param_marks = []

            if candidate.startswith(tuple(suspended_providers_folders)):
                param_marks.append(pytest.mark.skip(reason="Suspended provider"))

            if candidate.startswith(tuple(current_python_excluded_providers_folders)):
                param_marks.append(
                    pytest.mark.skip(reason=f"Not supported for Python {CURRENT_PYTHON_VERSION}")
                )

            for optional, dependencies in OPTIONAL_PROVIDERS_DEPENDENCIES.items():
                if re.match(optional, candidate):
                    for distribution_name, specifier in dependencies.items():
                        result, reason = match_optional_dependencies(distribution_name, specifier)
                        if not result:
                            param_marks.append(pytest.mark.skip(reason=reason))

            if candidate.startswith(providers_folders):
                # Do not raise an error for airflow.exceptions.RemovedInAirflow3Warning.
                # We should not rush to enforce new syntax updates in providers
                # because a version of Airflow that deprecates certain features may not yet be released.
                # Instead, it is advisable to periodically review the warning reports and implement manual
                # updates as needed.
                param_marks.append(
                    pytest.mark.filterwarnings("default::airflow.exceptions.RemovedInAirflow3Warning")
                )
                if candidate.endswith(IGNORE_AIRFLOW_PROVIDER_DEPRECATION_WARNING):
                    param_marks.append(
                        pytest.mark.filterwarnings(
                            "default::airflow.exceptions.AirflowProviderDeprecationWarning"
                        )
                    )

            yield pytest.param(candidate, marks=tuple(param_marks), id=relative_path(candidate))


def relative_path(path):
    return os.path.relpath(path, AIRFLOW_SOURCES_ROOT.as_posix())


@pytest.mark.db_test
@pytest.mark.parametrize("example", example_not_excluded_dags())
def test_should_be_importable(example: str):
    dagbag = DagBag(
        dag_folder=example,
        include_examples=False,
    )
    assert len(dagbag.import_errors) == 0, f"import_errors={str(dagbag.import_errors)}"
    assert len(dagbag.dag_ids) >= 1


@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.db_test
@pytest.mark.parametrize("example", example_not_excluded_dags(xfail_db_exception=True))
def test_should_not_do_database_queries(example: str):
    with assert_queries_count(1, stacklevel_from_module=example.rsplit(os.sep, 1)[-1]):
        DagBag(
            dag_folder=example,
            include_examples=False,
        )
