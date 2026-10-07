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

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

if not AIRFLOW_V_3_1_PLUS:
    pytest.skip("The AI Model panel is only compatible with Airflow >= 3.1.0", allow_module_level=True)

from airflow.providers.common.ai.plugins.model_panel import ModelPanelPlugin, _get_base_url_path

from tests_common.test_utils.config import conf_vars


class TestGetBaseUrlPath:
    def test_default_base_url(self):
        with conf_vars({("api", "base_url"): "/"}):
            assert _get_base_url_path("/ai-model") == "/ai-model"

    def test_http_base_url_extracts_path(self):
        with conf_vars({("api", "base_url"): "http://example.com/airflow/"}):
            assert _get_base_url_path("/ai-model") == "/airflow/ai-model"


class TestModelPanelPlugin:
    def test_plugin_name(self):
        assert ModelPanelPlugin.name == "ai_model_panel"

    def test_fastapi_apps_registered(self):
        assert len(ModelPanelPlugin.fastapi_apps) == 1
        assert ModelPanelPlugin.fastapi_apps[0]["name"] == "ai-model-panel"
        assert "url_prefix" in ModelPanelPlugin.fastapi_apps[0]

    def test_react_apps_registered(self):
        assert len(ModelPanelPlugin.react_apps) == 1
        app = ModelPanelPlugin.react_apps[0]
        assert app["name"] == "Model"
        assert app["url_route"] == "ai-model"
        assert app["destination"] == "task_instance"
        assert "model.umd.cjs" in app["bundle_url"]

    def test_applies_to_scopes_to_operators_that_publish_model_name(self):
        app = ModelPanelPlugin.react_apps[0]
        operator_names = app["applies_to"]["operator_names"]
        assert set(operator_names) == {"LLMOperator", "AgentOperator", "@task.llm", "@task.agent"}
