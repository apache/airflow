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

import importlib

import pytest
from packaging.version import Version

from airflow import __version__ as airflow_version
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.edge.plugins import edge_executor_plugin

from tests_common.test_utils.config import conf_vars

AIRFLOW_VERSION = Version(airflow_version)
AIRFLOW_V_3_0_PLUS = Version(AIRFLOW_VERSION.base_version) >= Version("3.0.0")


def test_plugin_inactive():
    with conf_vars({("edge", "api_enabled"): "false"}):
        importlib.reload(edge_executor_plugin)

        from airflow.providers.edge.plugins.edge_executor_plugin import (
            EDGE_EXECUTOR_ACTIVE,
            EdgeExecutorPlugin,
        )

        rep = EdgeExecutorPlugin()
        assert not EDGE_EXECUTOR_ACTIVE
        assert len(rep.flask_blueprints) == 0
        assert len(rep.appbuilder_views) == 0


def test_plugin_active():
    with conf_vars({("edge", "api_enabled"): "true"}):
        importlib.reload(edge_executor_plugin)

        from airflow.providers.edge.plugins.edge_executor_plugin import (
            EDGE_EXECUTOR_ACTIVE,
            EdgeExecutorPlugin,
        )

        rep = EdgeExecutorPlugin()
        assert EDGE_EXECUTOR_ACTIVE
        assert len(rep.appbuilder_views) == 2
        if AIRFLOW_V_3_0_PLUS:
            assert len(rep.flask_blueprints) == 1
            assert len(rep.fastapi_apps) == 1
        else:
            assert len(rep.flask_blueprints) == 2


@pytest.fixture
def plugin():
    from airflow.providers.edge.plugins.edge_executor_plugin import EdgeExecutorPlugin

    return EdgeExecutorPlugin()


def test_plugin_is_airflow_plugin(plugin):
    assert isinstance(plugin, AirflowPlugin)
