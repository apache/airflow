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
from unittest import mock

import pytest

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.testing_dags import (
    build_dag_bundle_config_list,
    find_testing_dag_folders,
    main,
    parse_provider_ids,
)

LOCAL_BUNDLE = "airflow.dag_processing.bundles.local.LocalDagBundle"
HIVE_BUNDLE = "apache-airflow-providers-apache-hive-testing-dags"
STANDARD_BUNDLE = "apache-airflow-providers-standard-testing-dags"


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


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("all", None, id="all"),
        pytest.param(" all ", None, id="all-with-spaces"),
        pytest.param("amazon", ["amazon"], id="single"),
        pytest.param("amazon, apache.hive,", ["amazon", "apache.hive"], id="list"),
    ],
)
def test_parse_provider_ids(value, expected):
    assert parse_provider_ids(value) == expected


@pytest.mark.parametrize(
    ("provider_ids", "expected"),
    [
        pytest.param(
            None,
            {HIVE_BUNDLE: "apache/hive/tests/testing_dags", STANDARD_BUNDLE: "standard/tests/testing_dags"},
            id="all",
        ),
        pytest.param(["apache.hive"], {HIVE_BUNDLE: "apache/hive/tests/testing_dags"}, id="selected"),
        pytest.param([], {}, id="none"),
    ],
)
def test_find_testing_dag_folders(providers_root, provider_ids, expected):
    folders = find_testing_dag_folders(provider_ids, providers_root)

    assert {
        name: folder.relative_to(providers_root).as_posix() for name, folder in folders.items()
    } == expected


def test_find_testing_dag_folders_skips_non_provider_folders(tmp_path):
    (tmp_path / "not-a-provider" / "tests" / "testing_dags").mkdir(parents=True)

    assert find_testing_dag_folders(providers_root=tmp_path) == {}


@pytest.mark.parametrize(
    "provider_ids",
    [
        pytest.param(["amazon", "standard"], id="provider-without-testing-dags"),
        pytest.param(["apache-hive", "standard"], id="dashed-id"),
    ],
)
def test_find_testing_dag_folders_rejects_unknown_providers(providers_root, provider_ids):
    with pytest.raises(ValueError, match=f"no tests/testing_dags folder: {provider_ids[0]}$"):
        find_testing_dag_folders(provider_ids, providers_root)


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
        bundle_configs = json.loads(build_dag_bundle_config_list(["standard"], providers_root))

    assert bundle_configs == [
        *configured_bundles,
        {
            "name": STANDARD_BUNDLE,
            "classpath": LOCAL_BUNDLE,
            "kwargs": {"path": str(providers_root / "standard" / "tests" / "testing_dags")},
        },
    ]


@mock.patch("tests_common.test_utils.testing_dags.build_dag_bundle_config_list", autospec=True)
def test_main_prints_bundle_config_list(mock_build, capsys):
    mock_build.return_value = "[]"

    assert main(["amazon,apache.hive"]) == 0

    mock_build.assert_called_once_with(["amazon", "apache.hive"])
    assert capsys.readouterr().out == "[]\n"


@mock.patch(
    "tests_common.test_utils.testing_dags.build_dag_bundle_config_list",
    autospec=True,
    side_effect=ValueError("These providers have no tests/testing_dags folder: amazon"),
)
def test_main_reports_unknown_providers(mock_build, capsys):
    assert main(["amazon"]) == 1

    assert capsys.readouterr().err == "Error: These providers have no tests/testing_dags folder: amazon\n"


@pytest.mark.parametrize("argv", [pytest.param([], id="missing"), pytest.param(["a", "b"], id="too-many")])
def test_main_requires_one_argument(argv, capsys):
    assert main(argv) == 2

    assert "Usage:" in capsys.readouterr().err
