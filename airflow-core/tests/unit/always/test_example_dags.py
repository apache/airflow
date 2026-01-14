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
from unittest.mock import patch

import pytest
from packaging.specifiers import SpecifierSet
from packaging.version import Version

from airflow.configuration import conf
from airflow.dag_processing.dagbag import DagBag
from airflow.models import Connection
from airflow.sdk import BaseHook
from airflow.utils import yaml

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker
from tests_common.test_utils.paths import AIRFLOW_PROVIDERS_ROOT_PATH, AIRFLOW_ROOT_PATH

CURRENT_PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"
PROVIDERS_PREFIXES = ["providers/"]
OPTIONAL_PROVIDERS_DEPENDENCIES: dict[str, dict[str, str | None]] = {
    # Some examples or system tests may depend on additional packages
    # that are not included in certain CI checks.
    # The format of the dictionary is as follows:
    # key: the regexp matching the file to be excluded,
    # value: a dictionary containing package distributions with an optional version specifier, e.g., >=2.3.4
    # yandexcloud is automatically removed in case botocore is upgraded to latest
    r".*example_yandexcloud.*\.py": {"yandexcloud": None},
}
IGNORE_AIRFLOW_PROVIDER_DEPRECATION_WARNING: tuple[str, ...] = (
    # Certain examples or system tests may trigger AirflowProviderDeprecationWarnings.
    # Generally, these should be resolved as soon as a parameter or operator is deprecated.
    # If the deprecation is postponed, the item should be added to this tuple,
    # and a corresponding Issue should be created on GitHub.
    "providers/google/tests/system/google/cloud/dataproc/example_dataproc_gke.py",
    "providers/google/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine.py",
    "providers/google/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_async.py",
    "providers/google/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_job.py",
    "providers/google/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_kueue.py",
    "providers/google/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_resource.py",
    # Deprecated Operators/Hooks, which replaced by common.sql Operators/Hooks
)

LONGER_IMPORT_TIMEOUTS: dict[str, float] = {
    "providers/google/tests/system/google/cloud/gen_ai/example_gen_ai_generative_model.py": 60
}


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
    for provider_path in AIRFLOW_PROVIDERS_ROOT_PATH.rglob("provider.yaml"):
        provider_yaml = yaml.safe_load(provider_path.read_text())
        if provider_yaml["state"] == "suspended":
            suspended_providers.append(provider_path.parent.resolve().as_posix())
    return suspended_providers


def get_python_excluded_providers_folders() -> list[str]:
    """
    Returns a list of providers folders that should be excluded for current Python version and
    skipped when running tests (without any prefix - for example apache/beam, yandex, google etc.).
    """
    excluded_providers = []
    for provider_path in AIRFLOW_PROVIDERS_ROOT_PATH.rglob("provider.yaml"):
        provider_yaml = yaml.safe_load(provider_path.read_text())
        excluded_python_versions = provider_yaml.get("excluded-python-versions", [])
        if CURRENT_PYTHON_VERSION in excluded_python_versions:
            excluded_providers.append(provider_path.parent.resolve().as_posix())
    return excluded_providers


def example_not_excluded_dags(xfail_db_exception: bool = False):
    example_dirs = [
        "airflow-core/**/example_dags/example_*.py",
        "tests/system/**/example_*.py",
        "providers/**/example_*.py",
    ]

    default_branch = os.environ.get("DEFAULT_BRANCH", "main")
    include_providers = default_branch == "main"

    suspended_providers_folders = get_suspended_providers_folders()
    current_python_excluded_providers_folders = get_python_excluded_providers_folders()
    providers_folders = tuple([AIRFLOW_ROOT_PATH.joinpath(pp).as_posix() for pp in PROVIDERS_PREFIXES])
    for example_dir in example_dirs:
        candidates = glob(f"{AIRFLOW_ROOT_PATH.as_posix()}/{example_dir}", recursive=True)
        for candidate in sorted(candidates):
            param_marks = []

            if candidate.startswith(tuple(suspended_providers_folders)):
                param_marks.append(pytest.mark.skip(reason="Suspended provider"))

            if candidate.startswith(tuple(current_python_excluded_providers_folders)):
                param_marks.append(
                    pytest.mark.skip(reason=f"Not supported for Python {CURRENT_PYTHON_VERSION}")
                )

            # TODO: remove when context serialization is implemented in AIP-72
            if "/example_python_context_" in candidate:
                param_marks.append(
                    pytest.mark.skip(reason="Temporary excluded until AIP-72 context serialization is done.")
                )

            for optional, dependencies in OPTIONAL_PROVIDERS_DEPENDENCIES.items():
                if re.match(optional, candidate):
                    for distribution_name, specifier in dependencies.items():
                        result, reason = match_optional_dependencies(distribution_name, specifier)
                        if not result:
                            param_marks.append(pytest.mark.skip(reason=reason))

            if candidate.startswith(providers_folders):
                if not include_providers:
                    print(
                        f"Skipping {candidate} because providers are not included for {default_branch} branch."
                    )
                    continue
                if candidate.endswith(IGNORE_AIRFLOW_PROVIDER_DEPRECATION_WARNING):
                    param_marks.append(
                        pytest.mark.filterwarnings(
                            "default::airflow.exceptions.AirflowProviderDeprecationWarning"
                        )
                    )

            yield pytest.param(candidate, marks=tuple(param_marks), id=relative_path(candidate))


def relative_path(path):
    return os.path.relpath(path, AIRFLOW_ROOT_PATH.as_posix())


def _get_dagbag_import_timeout_for_example(example_path: str) -> float:
    """
    Return the dagbag import timeout for the given example path.
    If the example path ends with one of the keys in LONGER_IMPORT_TIMEOUTS, use that value.
    Otherwise fall back to the configured default from [core] dagbag_import_timeout.
    """
    for key, val in LONGER_IMPORT_TIMEOUTS.items():
        if example_path.endswith(key):
            return val
    return conf.getfloat("core", "dagbag_import_timeout")


@pytest.fixture
def patch_get_dagbag_import_timeout():
    """Patch settings.get_dagbag_import_timeout to consult LONGER_IMPORT_TIMEOUTS or config.

    The patched function accepts the dag file path argument and returns the timeout.
    """
    with patch(
        "airflow.dag_processing.dagbag.settings.get_dagbag_import_timeout",
        side_effect=_get_dagbag_import_timeout_for_example,
    ):
        yield


@skip_if_force_lowest_dependencies_marker
@pytest.mark.db_test
@pytest.mark.parametrize("example", example_not_excluded_dags())
def test_should_be_importable(example: str, patch_get_dagbag_import_timeout):
    dagbag = DagBag(
        dag_folder=example,
        include_examples=False,
    )
    if len(dagbag.import_errors) == 1 and "AirflowOptionalProviderFeatureException" in str(
        dagbag.import_errors
    ):
        pytest.skip(
            f"Skipping {example} because it requires an optional provider feature that is not installed."
        )
    assert len(dagbag.import_errors) == 0, f"import_errors={str(dagbag.import_errors)}"
    assert len(dagbag.dag_ids) >= 1


@skip_if_force_lowest_dependencies_marker
@pytest.mark.db_test
@pytest.mark.parametrize("example", example_not_excluded_dags(xfail_db_exception=True))
def test_should_not_do_database_queries(example: str, patch_get_dagbag_import_timeout):
    with assert_queries_count(1, stacklevel_from_module=example.rsplit(os.sep, 1)[-1]):
        DagBag(
            dag_folder=example,
            include_examples=False,
        )


@pytest.mark.db_test
@pytest.mark.parametrize("example", example_not_excluded_dags(xfail_db_exception=True))
def test_should_not_run_hook_connections(example: str, patch_get_dagbag_import_timeout):
    # Example dags should never run BaseHook.get_connection() class method when parsed
    with patch.object(BaseHook, "get_connection") as mock_get_connection:
        mock_get_connection.return_value = Connection()
        DagBag(
            dag_folder=example,
            include_examples=False,
        )
    assert mock_get_connection.call_count == 0, (
        f"BaseHook.get_connection() should not be called during DAG parsing. "
        f"It was called {mock_get_connection.call_count} times. Please make sure that no "
        "connections are created during DAG parsing. NOTE! Do not set conn_id to None to avoid it, just make "
        "sure that you do not create connection object in the `__init__` method of your operator."
    )
