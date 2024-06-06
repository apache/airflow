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
import sys
from glob import glob
from importlib import metadata as importlib_metadata
from pathlib import Path

import pytest
from packaging.specifiers import SpecifierSet
from packaging.version import Version

from airflow.models import DagBag
from airflow.utils import yaml
from tests.test_utils.asserts import assert_queries_count

AIRFLOW_SOURCES_ROOT = Path(__file__).resolve().parents[2]
AIRFLOW_PROVIDERS_ROOT = AIRFLOW_SOURCES_ROOT / "airflow" / "providers"
CURRENT_PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"
NO_DB_QUERY_EXCEPTION = ("/airflow/example_dags/example_subdag_operator.py",)
PROVIDERS_PREFIXES = ("airflow/providers/", "tests/system/providers/")
OPTIONAL_PROVIDERS_DEPENDENCIES: dict[str, dict[str, str | None]] = {
    # Some examples or system tests may depend on additional packages
    # that are not included in certain CI checks.
    # The format of the dictionary is as follows:
    # key: the prefix of the file to be excluded,
    # value: a dictionary containing package distributions with an optional version specifier, e.g., >=2.3.4
}
IGNORE_AIRFLOW_PROVIDER_DEPRECATION_WARNING: tuple[str, ...] = (
    # Certain examples or system tests may trigger AirflowProviderDeprecationWarnings.
    # Generally, these should be resolved as soon as a parameter or operator is deprecated.
    # If the deprecation is postponed, the item should be added to this tuple,
    # and a corresponding Issue should be created on GitHub.
    "tests/system/providers/google/cloud/bigquery/example_bigquery_operations.py",
    "tests/system/providers/google/cloud/dataproc/example_dataproc_gke.py",
    "tests/system/providers/google/cloud/gcs/example_gcs_sensor.py",
    "tests/system/providers/google/cloud/kubernetes_engine/example_kubernetes_engine.py",
    "tests/system/providers/google/cloud/kubernetes_engine/example_kubernetes_engine_async.py",
    "tests/system/providers/google/cloud/kubernetes_engine/example_kubernetes_engine_job.py",
    "tests/system/providers/google/cloud/kubernetes_engine/example_kubernetes_engine_kueue.py",
    "tests/system/providers/google/cloud/kubernetes_engine/example_kubernetes_engine_resource.py",
    "tests/system/providers/google/cloud/life_sciences/example_life_sciences.py",
    "tests/system/providers/google/marketing_platform/example_analytics.py",
    # Deprecated Operators/Hooks, which replaced by common.sql Operators/Hooks
    "tests/system/providers/jdbc/example_jdbc_queries.py",
    "tests/system/providers/snowflake/example_snowflake.py",
    "tests/system/providers/trino/example_trino.py",
)


if os.environ.get("PYDANTIC", "v2") != "v2":
    pytest.skip(
        "The test is skipped because we are running in limited Pydantic environment", allow_module_level=True
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
                .replace("airflow/providers/", "")
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
                .replace("airflow/providers/", "")
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

    for example_dir in example_dirs:
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
                if candidate.endswith(optional):
                    for distribution_name, specifier in dependencies.items():
                        result, reason = match_optional_dependencies(distribution_name, specifier)
                        if not result:
                            param_marks.append(pytest.mark.skip(reason=reason))

            if xfail_db_exception and candidate.endswith(NO_DB_QUERY_EXCEPTION):
                # Use strict XFAIL for excluded tests. So if it is not failed, we should remove from the list.
                param_marks.append(pytest.mark.xfail(reason="Expected DB call", strict=True))

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


@pytest.mark.db_test
@pytest.mark.parametrize("example", example_not_excluded_dags(xfail_db_exception=True))
def test_should_not_do_database_queries(example: str):
    with assert_queries_count(0, stacklevel_from_module=example.rsplit(os.sep, 1)[-1]):
        DagBag(
            dag_folder=example,
            include_examples=False,
        )
