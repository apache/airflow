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
from unittest.mock import patch

import pytest

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.exceptions import AirflowConfigException
from airflow.models.dagbundle import DagBundleModel
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_dag_bundles


@pytest.mark.parametrize(
    "envs,expected_names",
    [
        pytest.param({}, {"dags_folder"}, id="no_config"),
        pytest.param(
            {"AIRFLOW__DAG_BUNDLES__TESTBUNDLE": "{}"}, {"testbundle", "dags_folder"}, id="add_bundle"
        ),
        pytest.param({"AIRFLOW__DAG_BUNDLES__DAGS_FOLDER": ""}, set(), id="remove_dags_folder_default"),
        pytest.param(
            {"AIRFLOW__DAG_BUNDLES__DAGS_FOLDER": "", "AIRFLOW__DAG_BUNDLES__TESTBUNDLE": "{}"},
            {"testbundle"},
            id="remove_dags_folder_default_add_bundle",
        ),
    ],
)
def test_bundle_configs_property(envs, expected_names):
    """Test that bundle_configs are read from configuration."""
    bundle_manager = DagBundlesManager()
    with patch.dict(os.environ, envs):
        names = set(bundle_manager.bundle_configs.keys())
    assert names == expected_names


@pytest.mark.parametrize(
    "config,message",
    [
        pytest.param("1", "Bundle config for testbundle is not a dict: 1", id="int"),
        pytest.param("[]", r"Bundle config for testbundle is not a dict: \[\]", id="list"),
        pytest.param("abc", r"Unable to parse .* as valid json", id="not_json"),
    ],
)
def test_bundle_configs_property_raises(config, message):
    bundle_manager = DagBundlesManager()
    with patch.dict(os.environ, {"AIRFLOW__DAG_BUNDLES__TESTBUNDLE": config}):
        with pytest.raises(AirflowConfigException, match=message):
            bundle_manager.bundle_configs


class BasicBundle(BaseDagBundle):
    def refresh(self):
        pass

    def get_current_version(self):
        pass

    def path(self):
        pass


BASIC_BUNDLE_CONFIG = {
    "classpath": "tests.dag_processing.bundles.test_dag_bundle_manager.BasicBundle",
    "kwargs": {"refresh_interval": 1},
}


def test_get_bundle():
    """Test that get_bundle builds and returns a bundle."""

    bundle_manager = DagBundlesManager()

    with patch.dict(os.environ, {"AIRFLOW__DAG_BUNDLES__TESTBUNDLE": json.dumps(BASIC_BUNDLE_CONFIG)}):
        bundle = bundle_manager.get_bundle(name="testbundle", version="hello")
    assert isinstance(bundle, BasicBundle)
    assert bundle.name == "testbundle"
    assert bundle.version == "hello"
    assert bundle.refresh_interval == 1

    # And none for version also works!
    with patch.dict(os.environ, {"AIRFLOW__DAG_BUNDLES__TESTBUNDLE": json.dumps(BASIC_BUNDLE_CONFIG)}):
        bundle = bundle_manager.get_bundle(name="testbundle")
    assert isinstance(bundle, BasicBundle)
    assert bundle.name == "testbundle"
    assert bundle.version is None


def test_get_all_dag_bundles():
    """Test that get_all_dag_bundles returns all bundles."""

    bundle_manager = DagBundlesManager()

    with patch.dict(os.environ, {"AIRFLOW__DAG_BUNDLES__TESTBUNDLE": json.dumps(BASIC_BUNDLE_CONFIG)}):
        bundles = bundle_manager.get_all_dag_bundles()
    assert len(bundles) == 2
    assert all(isinstance(x, BaseDagBundle) for x in bundles)

    bundle_names = {x.name for x in bundles}
    assert bundle_names == {"testbundle", "dags_folder"}


@pytest.fixture
def clear_db():
    clear_db_dag_bundles()
    yield
    clear_db_dag_bundles()


@pytest.mark.db_test
def test_sync_bundles_to_db(clear_db):
    bundle_manager = DagBundlesManager()

    def _get_bundle_names_and_active():
        with create_session() as session:
            return (
                session.query(DagBundleModel.name, DagBundleModel.active).order_by(DagBundleModel.name).all()
            )

    # Initial add
    with patch.dict(os.environ, {"AIRFLOW__DAG_BUNDLES__TESTBUNDLE": json.dumps(BASIC_BUNDLE_CONFIG)}):
        bundle_manager.sync_bundles_to_db()
    assert _get_bundle_names_and_active() == [("dags_folder", True), ("testbundle", True)]

    # Disable ones that disappear from config
    bundle_manager.sync_bundles_to_db()
    assert _get_bundle_names_and_active() == [("dags_folder", True), ("testbundle", False)]

    # Re-enable one that reappears in config
    with patch.dict(os.environ, {"AIRFLOW__DAG_BUNDLES__TESTBUNDLE": json.dumps(BASIC_BUNDLE_CONFIG)}):
        bundle_manager.sync_bundles_to_db()
    assert _get_bundle_names_and_active() == [("dags_folder", True), ("testbundle", True)]
