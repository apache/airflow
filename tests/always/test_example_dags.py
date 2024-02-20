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
from pathlib import Path

import pytest
from packaging.specifiers import SpecifierSet
from packaging.version import Version

if sys.version_info >= (3, 9):
    from importlib.metadata import version
else:
    from importlib_metadata import version

from airflow.models import DagBag
from airflow.utils import yaml
from tests.test_utils.asserts import assert_queries_count

AIRFLOW_SOURCES_ROOT = Path(__file__).resolve().parents[2]
AIRFLOW_PROVIDERS_ROOT = AIRFLOW_SOURCES_ROOT / "airflow" / "providers"

NO_DB_QUERY_EXCEPTION = ["/airflow/example_dags/example_subdag_operator.py"]

if os.environ.get("PYDANTIC", "v2") != "v2":
    pytest.skip(
        "The test is skipped because we are running in limited Pydantic environment", allow_module_level=True
    )

OPTIONAL_PROVIDERS_DEPENDENCIES = {
    # This test required to be installed `s3fs`, which are not installed into some CI checks
    "tests/system/providers/common/io/example_file_transfer_local_to_s3.py": {"s3fs": None}
}


def match_optional_dependencies(distribution_name: str, specifier: str | None) -> tuple[bool, str]:
    try:
        package_version = Version(version(distribution_name))
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


def example_not_suspended_dags(exclude_db_exception: bool = False):
    example_dirs = ["airflow/**/example_dags/example_*.py", "tests/system/providers/**/example_*.py"]
    suspended_providers_folders = get_suspended_providers_folders()
    possible_prefixes = ["airflow/providers/", "tests/system/providers/"]
    suspended_providers_folders = [
        AIRFLOW_SOURCES_ROOT.joinpath(prefix, provider).as_posix()
        for prefix in possible_prefixes
        for provider in suspended_providers_folders
    ]
    for example_dir in example_dirs:
        candidates = glob(f"{AIRFLOW_SOURCES_ROOT.as_posix()}/{example_dir}", recursive=True)
        for candidate in candidates:
            param_marks = []

            if candidate.startswith(tuple(suspended_providers_folders)):
                param_marks.append(pytest.mark.skip(reason="Suspended provider"))

            for optional, dependencies in OPTIONAL_PROVIDERS_DEPENDENCIES.items():
                if candidate.endswith(optional):
                    for distribution_name, specifier in dependencies.items():
                        result, reason = match_optional_dependencies(distribution_name, specifier)
                        if not result:
                            param_marks.append(pytest.mark.skip(reason=reason))

            if exclude_db_exception and candidate.endswith(tuple(NO_DB_QUERY_EXCEPTION)):
                param_marks.append(pytest.mark.skip(reason="Expected DB call"))

            yield pytest.param(candidate, marks=tuple(param_marks), id=relative_path(candidate))


def example_dags_except_db_exception():
    return [
        dag_file
        for dag_file in example_not_suspended_dags()
        if not dag_file.endswith(tuple(NO_DB_QUERY_EXCEPTION))
    ]


def relative_path(path):
    return os.path.relpath(path, AIRFLOW_SOURCES_ROOT.as_posix())


@pytest.mark.db_test
@pytest.mark.parametrize("example", example_not_suspended_dags())
def test_should_be_importable(example: str):
    dagbag = DagBag(
        dag_folder=example,
        include_examples=False,
    )
    assert len(dagbag.import_errors) == 0, f"import_errors={str(dagbag.import_errors)}"
    assert len(dagbag.dag_ids) >= 1


@pytest.mark.db_test
@pytest.mark.parametrize("example", example_not_suspended_dags(exclude_db_exception=True))
def test_should_not_do_database_queries(example: str):
    with assert_queries_count(0):
        DagBag(
            dag_folder=example,
            include_examples=False,
        )
