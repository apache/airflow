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
from glob import glob
from pathlib import Path

import pytest

from airflow.models import DagBag
from airflow.settings import _ENABLE_AIP_52
from airflow.utils import yaml
from tests.test_utils.asserts import assert_queries_count

AIRFLOW_SOURCES_ROOT = Path(__file__).resolve().parents[3]
AIRFLOW_PROVIDERS_ROOT = AIRFLOW_SOURCES_ROOT / "airflow" / "providers"

NO_DB_QUERY_EXCEPTION = ["/airflow/example_dags/example_subdag_operator.py"]


def get_suspended_providers_folders() -> list[str]:
    """
    Returns a list of suspended providers folders that should be
    skipped when running tests (without any prefix - for example apache/beam, yandex, google etc.).
    """
    suspended_providers = []
    for provider_path in AIRFLOW_PROVIDERS_ROOT.glob("**/provider.yaml"):
        provider_yaml = yaml.safe_load(provider_path.read_text())
        if provider_yaml.get("suspended"):
            suspended_providers.append(
                provider_path.parent.relative_to(AIRFLOW_SOURCES_ROOT)
                .as_posix()
                .replace("airflow/providers/", "")
            )
    return suspended_providers


def example_not_suspended_dags():
    example_dirs = ["airflow/**/example_dags/example_*.py", "tests/system/providers/**/example_*.py"]
    suspended_providers_folders = get_suspended_providers_folders()
    possible_prefixes = ["airflow/providers/", "tests/system/providers/"]
    suspended_providers_folders = [
        f"{prefix}{provider}" for prefix in possible_prefixes for provider in suspended_providers_folders
    ]
    for example_dir in example_dirs:
        candidates = glob(f"{AIRFLOW_SOURCES_ROOT.as_posix()}/{example_dir}", recursive=True)
        for candidate in candidates:
            if any(candidate.startswith(s) for s in suspended_providers_folders):
                continue
            # we will also suspend AIP-52 DAGs unless it is enabled
            if not _ENABLE_AIP_52 and "example_setup_teardown" in candidate:
                continue
            yield candidate


def example_dags_except_db_exception():
    return [
        dag_file
        for dag_file in example_not_suspended_dags()
        if any(not dag_file.endswith(e) for e in NO_DB_QUERY_EXCEPTION)
    ]


def relative_path(path):
    return os.path.relpath(path, AIRFLOW_SOURCES_ROOT.as_posix())


@pytest.mark.parametrize("example", example_not_suspended_dags(), ids=relative_path)
def test_should_be_importable(example):
    dagbag = DagBag(
        dag_folder=example,
        include_examples=False,
    )
    assert len(dagbag.import_errors) == 0, f"import_errors={str(dagbag.import_errors)}"
    assert len(dagbag.dag_ids) >= 1


@pytest.mark.parametrize("example", example_dags_except_db_exception(), ids=relative_path)
def test_should_not_do_database_queries(example):
    with assert_queries_count(0):
        DagBag(
            dag_folder=example,
            include_examples=False,
        )
