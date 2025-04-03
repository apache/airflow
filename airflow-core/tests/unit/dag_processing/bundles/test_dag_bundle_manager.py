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
from contextlib import nullcontext
from unittest.mock import patch

import pytest

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.exceptions import AirflowConfigException
from airflow.models.dagbundle import DagBundleModel
from airflow.utils.session import create_session

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dag_bundles


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(None, {"dags-folder"}, id="default"),
        pytest.param("{}", set(), id="empty dict"),
        pytest.param(
            "[]",
            set(),
            id="empty list",
        ),
        pytest.param(
            json.dumps(
                [
                    {
                        "name": "my-bundle",
                        "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                        "kwargs": {"path": "/tmp/hihi", "refresh_interval": 1},
                    }
                ]
            ),
            {"my-bundle"},
            id="remove_dags_folder_default_add_bundle",
        ),
        pytest.param(
            "[]",
            set(),
            id="remove_dags_folder_default",
        ),
        pytest.param("1", "key `dag_bundle_config_list` must be list", id="int"),
        pytest.param("abc", "Unable to parse .* as valid json", id="not_json"),
    ],
)
def test_parse_bundle_config(value, expected):
    """Test that bundle_configs are read from configuration."""
    envs = {"AIRFLOW__CORE__LOAD_EXAMPLES": "False"}
    if value:
        envs["AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST"] = value
    cm = nullcontext()
    exp_fail = False
    if isinstance(expected, str):
        exp_fail = True
        cm = pytest.raises(AirflowConfigException, match=expected)

    with patch.dict(os.environ, envs), cm:
        bundle_manager = DagBundlesManager()
        names = set(x.name for x in bundle_manager.get_all_dag_bundles())

    if not exp_fail:
        assert names == expected


class BasicBundle(BaseDagBundle):
    def refresh(self):
        pass

    def get_current_version(self):
        pass

    def path(self):
        pass


BASIC_BUNDLE_CONFIG = [
    {
        "name": "my-test-bundle",
        "classpath": "unit.dag_processing.bundles.test_dag_bundle_manager.BasicBundle",
        "kwargs": {"refresh_interval": 1},
    }
]


def test_get_bundle():
    """Test that get_bundle builds and returns a bundle."""
    with patch.dict(
        os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)}
    ):
        bundle_manager = DagBundlesManager()

        with pytest.raises(ValueError, match="'bundle-that-doesn't-exist' is not configured"):
            bundle_manager.get_bundle(name="bundle-that-doesn't-exist", version="hello")
        bundle = bundle_manager.get_bundle(name="my-test-bundle", version="hello")
    assert isinstance(bundle, BasicBundle)
    assert bundle.name == "my-test-bundle"
    assert bundle.version == "hello"
    assert bundle.refresh_interval == 1

    # And none for version also works!
    with patch.dict(
        os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)}
    ):
        bundle = bundle_manager.get_bundle(name="my-test-bundle")
    assert isinstance(bundle, BasicBundle)
    assert bundle.name == "my-test-bundle"
    assert bundle.version is None


@pytest.fixture
def clear_db():
    clear_db_dag_bundles()
    yield
    clear_db_dag_bundles()


@pytest.mark.db_test
@conf_vars({("core", "LOAD_EXAMPLES"): "False"})
def test_sync_bundles_to_db(clear_db):
    def _get_bundle_names_and_active():
        with create_session() as session:
            return (
                session.query(DagBundleModel.name, DagBundleModel.active).order_by(DagBundleModel.name).all()
            )

    # Initial add
    with patch.dict(
        os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)}
    ):
        manager = DagBundlesManager()
        manager.sync_bundles_to_db()
    assert _get_bundle_names_and_active() == [("my-test-bundle", True)]

    # simulate bundle config change
    # note: airflow will detect config changes when they are in env vars
    manager = DagBundlesManager()
    manager.sync_bundles_to_db()
    assert _get_bundle_names_and_active() == [("dags-folder", True), ("my-test-bundle", False)]

    # Re-enable one that reappears in config
    with patch.dict(
        os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)}
    ):
        manager = DagBundlesManager()
        manager.sync_bundles_to_db()
    assert _get_bundle_names_and_active() == [("dags-folder", False), ("my-test-bundle", True)]


@conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps(BASIC_BUNDLE_CONFIG)})
@pytest.mark.parametrize("version", [None, "hello"])
def test_view_url(version):
    """Test that view_url calls the bundle's view_url method."""
    bundle_manager = DagBundlesManager()
    with patch.object(BaseDagBundle, "view_url") as view_url_mock:
        bundle_manager.view_url("my-test-bundle", version=version)
    view_url_mock.assert_called_once_with(version=version)


def test_example_dags_bundle_added():
    manager = DagBundlesManager()
    manager.parse_config()
    assert "example_dags" in manager._bundle_config

    with conf_vars({("core", "LOAD_EXAMPLES"): "False"}):
        manager = DagBundlesManager()
        manager.parse_config()
        assert "example_dags" not in manager._bundle_config


def test_example_dags_name_is_reserved():
    reserved_name_config = [{"name": "example_dags", "classpath": "yo face", "kwargs": {}}]
    with conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps(reserved_name_config)}):
        with pytest.raises(AirflowConfigException, match="Bundle name 'example_dags' is a reserved name."):
            DagBundlesManager().parse_config()
