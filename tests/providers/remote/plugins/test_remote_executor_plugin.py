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

from airflow.plugins_manager import AirflowPlugin
from airflow.providers.remote.plugins import remote_executor_plugin
from tests.test_utils.config import conf_vars


def test_plugin_inactive():
    with conf_vars({("remote", "api_enabled"): "false"}):
        importlib.reload(remote_executor_plugin)

        from airflow.providers.remote.plugins.remote_executor_plugin import (
            REMOTE_EXECUTOR_ACTIVE,
            RemoteExecutorPlugin,
        )

        rep = RemoteExecutorPlugin()
        assert not REMOTE_EXECUTOR_ACTIVE
        assert len(rep.flask_blueprints) == 0
        assert len(rep.appbuilder_views) == 0


def test_plugin_active():
    with conf_vars({("remote", "api_enabled"): "true"}):
        importlib.reload(remote_executor_plugin)

        from airflow.providers.remote.plugins.remote_executor_plugin import (
            REMOTE_EXECUTOR_ACTIVE,
            RemoteExecutorPlugin,
        )

        rep = RemoteExecutorPlugin()
        assert REMOTE_EXECUTOR_ACTIVE
        assert len(rep.flask_blueprints) == 2
        assert len(rep.appbuilder_views) == 2


@pytest.fixture
def plugin():
    from airflow.providers.remote.plugins.remote_executor_plugin import RemoteExecutorPlugin

    return RemoteExecutorPlugin()


def test_plugin_is_airflow_plugin(plugin):
    assert isinstance(plugin, AirflowPlugin)
