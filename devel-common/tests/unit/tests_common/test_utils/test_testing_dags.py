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

import pytest

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.testing_dags import build_dag_bundle_config_list, find_testing_dag_folders

LOCAL_BUNDLE = "airflow.dag_processing.bundles.local.LocalDagBundle"


@pytest.fixture
def providers_root(tmp_path):
    for provider in ("standard", "apache/hive", "amazon", "google"):
        (tmp_path / provider).mkdir(parents=True)
        (tmp_path / provider / "provider.yaml").touch()
    (tmp_path / "standard" / "tests" / "testing_dags").mkdir(parents=True)
    (tmp_path / "apache" / "hive" / "tests" / "testing_dags").mkdir(parents=True)
    # Under ``src/`` it would ship in the provider wheel, so it is not a testing Dags folder.
    (tmp_path / "google" / "src" / "airflow" / "providers" / "google" / "testing_dags").mkdir(parents=True)
    return tmp_path


def test_find_testing_dag_folders(providers_root):
    folders = find_testing_dag_folders(providers_root)

    assert {name: folder.relative_to(providers_root).as_posix() for name, folder in folders.items()} == {
        "apache-airflow-providers-apache-hive-testing-dags": "apache/hive/tests/testing_dags",
        "apache-airflow-providers-standard-testing-dags": "standard/tests/testing_dags",
    }


def test_find_testing_dag_folders_skips_non_provider_folders(tmp_path):
    (tmp_path / "not-a-provider" / "tests" / "testing_dags").mkdir(parents=True)

    assert find_testing_dag_folders(tmp_path) == {}


@pytest.mark.parametrize(
    "configured_bundles",
    [
        pytest.param(
            [{"name": "dags-folder", "classpath": LOCAL_BUNDLE, "kwargs": {}}],
            id="default",
        ),
        pytest.param(
            [{"name": "my-repo", "classpath": "my.bundles.GitDagBundle", "kwargs": {"subdir": "dags"}}],
            id="user-configured",
        ),
    ],
)
def test_build_dag_bundle_config_list_appends_testing_bundles(providers_root, configured_bundles):
    with conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps(configured_bundles)}):
        bundle_configs = json.loads(build_dag_bundle_config_list(providers_root))

    assert bundle_configs == [
        *configured_bundles,
        {
            "name": "apache-airflow-providers-apache-hive-testing-dags",
            "classpath": LOCAL_BUNDLE,
            "kwargs": {"path": str(providers_root / "apache" / "hive" / "tests" / "testing_dags")},
        },
        {
            "name": "apache-airflow-providers-standard-testing-dags",
            "classpath": LOCAL_BUNDLE,
            "kwargs": {"path": str(providers_root / "standard" / "tests" / "testing_dags")},
        },
    ]
