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
from unittest.mock import patch

import pytest
import time_machine

from airflow.plugins_manager import AirflowPlugin
from airflow.providers.edge3.plugins import edge_executor_plugin

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS


def test_plugin_inactive():
    with conf_vars({("edge", "api_enabled"): "false"}):
        importlib.reload(edge_executor_plugin)

        from airflow.providers.edge3.plugins.edge_executor_plugin import (
            EDGE_EXECUTOR_ACTIVE,
            EdgeExecutorPlugin,
        )

        rep = EdgeExecutorPlugin()
        assert not EDGE_EXECUTOR_ACTIVE
        assert len(rep.flask_blueprints) == 0
        assert len(rep.appbuilder_views) == 0


@pytest.mark.db_test
def test_plugin_active_apiserver():
    mock_cli = ["airflow", "api-server"] if AIRFLOW_V_3_0_PLUS else ["gunicorn", "airflow-webserver"]
    with conf_vars({("edge", "api_enabled"): "true"}), patch("sys.argv", mock_cli):
        importlib.reload(edge_executor_plugin)

        from airflow.providers.edge3.plugins.edge_executor_plugin import (
            EDGE_EXECUTOR_ACTIVE,
            RUNNING_ON_APISERVER,
            EdgeExecutorPlugin,
        )

        rep = EdgeExecutorPlugin()
        assert EDGE_EXECUTOR_ACTIVE
        assert RUNNING_ON_APISERVER
        if AIRFLOW_V_3_0_PLUS:
            assert len(rep.appbuilder_views) == 0
            assert len(rep.flask_blueprints) == 0
            assert len(rep.fastapi_apps) == 1
        else:
            assert len(rep.appbuilder_views) == 2
            assert len(rep.flask_blueprints) == 2


@patch("sys.argv", ["airflow", "some-other-command"])
def test_plugin_active_non_apiserver():
    with conf_vars({("edge", "api_enabled"): "true"}):
        importlib.reload(edge_executor_plugin)

        from airflow.providers.edge3.plugins.edge_executor_plugin import (
            EDGE_EXECUTOR_ACTIVE,
            RUNNING_ON_APISERVER,
            EdgeExecutorPlugin,
        )

        rep = EdgeExecutorPlugin()
        assert EDGE_EXECUTOR_ACTIVE
        assert not RUNNING_ON_APISERVER
        assert len(rep.appbuilder_views) == 0
        assert len(rep.flask_blueprints) == 0
        assert len(rep.appbuilder_views) == 0
        if AIRFLOW_V_3_0_PLUS:
            assert len(rep.fastapi_apps) == 0


@pytest.fixture
def plugin():
    from airflow.providers.edge3.plugins.edge_executor_plugin import EdgeExecutorPlugin

    return EdgeExecutorPlugin()


def test_plugin_is_airflow_plugin(plugin):
    assert isinstance(plugin, AirflowPlugin)


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Plugin endpoint is not used in Airflow 3.0+")
@pytest.mark.parametrize(
    ("initial_comment", "expected_comment"),
    [
        pytest.param(
            "comment", "[2020-01-01 00:00] - user updated maintenance mode\nComment: comment", id="no user"
        ),
        pytest.param(
            "[2019-01-01] - another user put node into maintenance mode\nComment:new comment",
            "[2020-01-01 00:00] - user updated maintenance mode\nComment:new comment",
            id="first update",
        ),
        pytest.param(
            "[2019-01-01] - another user updated maintenance mode\nComment:new comment",
            "[2020-01-01 00:00] - user updated maintenance mode\nComment:new comment",
            id="second update",
        ),
        pytest.param(
            None,
            "[2020-01-01 00:00] - user updated maintenance mode\nComment:",
            id="None as input",
        ),
    ],
)
@time_machine.travel("2020-01-01", tick=False)
def test_modify_maintenance_comment_on_update(monkeypatch, initial_comment, expected_comment):
    assert (
        edge_executor_plugin.modify_maintenance_comment_on_update(initial_comment, "user") == expected_comment
    )
