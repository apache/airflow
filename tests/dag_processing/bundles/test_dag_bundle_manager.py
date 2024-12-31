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
        pytest.param({}, {"dags-folder"}, id="default"),
        pytest.param(
            {"AIRFLOW__DAG_BUNDLES__BACKENDS": "[]"},
            {"dags-folder"},
            id="empty-list",
        ),
        pytest.param(
            {"AIRFLOW__DAG_BUNDLES__DAGS_FOLDER": ""},
            {"dags-folder"},
            id="empty-string",
        ),
        pytest.param(
            {
                "AIRFLOW__DAG_BUNDLES__DAGS_FOLDER": "",
                "AIRFLOW__DAG_BUNDLES__BACKENDS": "{}",
            },
            {"dags-folder"},
            id="remove_dags_folder_default_add_bundle",
        ),
    ],
)
def test_bundle_configs_property(envs, expected_names):
    """Test that bundle_configs are read from configuration."""
    bundle_manager = DagBundlesManager()
    with patch.dict(os.environ, envs):
        bundle_manager.parse_config()
        names = set(x.name for x in bundle_manager.get_all_dag_bundles())
    assert names == expected_names


@pytest.mark.parametrize(
    "config,message",
    [
        pytest.param("1", "Bundle config is not a list", id="int"),
        pytest.param("[]", None, id="list"),
        pytest.param("{}", None, id="dict"),
        pytest.param("abc", "Unable to parse .* as valid json", id="not_json"),
    ],
)
def test_bundle_configs_property_raises(config, message):
    bundle_manager = DagBundlesManager()
    with patch.dict(os.environ, {"AIRFLOW__DAG_BUNDLES__BACKENDS": config}):
        if message:
            with pytest.raises(AirflowConfigException, match=message):
                bundle_manager.parse_config()
        else:
            bundle_manager.parse_config()


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
        "classpath": "tests.dag_processing.bundles.test_dag_bundle_manager.BasicBundle",
        "kwargs": {"refresh_interval": 1},
    }
]


def test_get_bundle():
    """Test that get_bundle builds and returns a bundle."""

    bundle_manager = DagBundlesManager()

    with patch.dict(os.environ, {"AIRFLOW__DAG_BUNDLES__BACKENDS": json.dumps(BASIC_BUNDLE_CONFIG)}):
        with pytest.raises(ValueError, match="'bundle-that-doesn't-exist' is not configured"):
            bundle = bundle_manager.get_bundle(name="bundle-that-doesn't-exist", version="hello")
        bundle = bundle_manager.get_bundle(name="my-test-bundle", version="hello")
    assert isinstance(bundle, BasicBundle)
    assert bundle.name == "my-test-bundle"
    assert bundle.version == "hello"
    assert bundle.refresh_interval == 1

    # And none for version also works!
    with patch.dict(os.environ, {"AIRFLOW__DAG_BUNDLES__BACKENDS": json.dumps(BASIC_BUNDLE_CONFIG)}):
        bundle = bundle_manager.get_bundle(name="my-test-bundle")
    assert isinstance(bundle, BasicBundle)
    assert bundle.name == "my-test-bundle"
    assert bundle.version is None


def test_get_all_dag_bundles():
    """Test that get_all_dag_bundles returns all bundles."""

    bundle_manager = DagBundlesManager()

    with patch.dict(os.environ, {"AIRFLOW__DAG_BUNDLES__BACKENDS": json.dumps(BASIC_BUNDLE_CONFIG)}):
        bundles = list(bundle_manager.get_all_dag_bundles())
    assert len(bundles) == 1
    assert all(isinstance(x, BaseDagBundle) for x in bundles)

    bundle_names = {x.name for x in bundles}
    assert bundle_names == {"my-test-bundle"}


def test_get_all_dag_bundles_default():
    """Test that get_all_dag_bundles returns all bundles."""

    bundle_manager = DagBundlesManager()
    bundles = list(bundle_manager.get_all_dag_bundles())
    assert len(bundles) == 1
    assert all(isinstance(x, BaseDagBundle) for x in bundles)

    bundle_names = {x.name for x in bundles}
    assert bundle_names == {"dags-folder"}


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
    with patch.dict(os.environ, {"AIRFLOW__DAG_BUNDLES__BACKENDS": json.dumps(BASIC_BUNDLE_CONFIG)}):
        bundle_manager.sync_bundles_to_db()
    assert _get_bundle_names_and_active() == [("my-test-bundle", True)]

    # Disable ones that disappear from config
    bundle_manager.sync_bundles_to_db()
    assert _get_bundle_names_and_active() == [("dags-folder", True), ("my-test-bundle", False)]

    # Re-enable one that reappears in config
    with patch.dict(os.environ, {"AIRFLOW__DAG_BUNDLES__BACKENDS": json.dumps(BASIC_BUNDLE_CONFIG)}):
        bundle_manager.sync_bundles_to_db()
    assert _get_bundle_names_and_active() == [("dags-folder", False), ("my-test-bundle", True)]


# import yaml
# from pathlib import Path
# d = yaml.safe_load(Path("/Users/dstandish/code/airflow/airflow/config_templates/config.yml").open())
# d["dag_bundles"]
