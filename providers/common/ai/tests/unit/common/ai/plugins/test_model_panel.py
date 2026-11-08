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

import copy
import importlib
import pkgutil

import pytest

from tests_common.test_utils.mock_plugins import mock_plugin_manager
from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_4_PLUS

if not AIRFLOW_V_3_1_PLUS:
    pytest.skip("The AI Model panel is only compatible with Airflow >= 3.1.0", allow_module_level=True)

from airflow import plugins_manager
from airflow.providers.common.ai import decorators, operators
from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.plugins.model_panel import ModelPanelPlugin

requires_3_4 = pytest.mark.skipif(
    not AIRFLOW_V_3_4_PLUS, reason="react_apps' applies_to scoping needs Airflow >= 3.4.0"
)


def get_in_tree_model_operator_names() -> set[str]:
    """Return the operator name a task instance records for each in-tree class that publishes the model name."""
    for package in (operators, decorators):
        for module in pkgutil.iter_modules(package.__path__):
            importlib.import_module(f"{package.__name__}.{module.name}")

    names = set()
    pending: list[type] = [LLMOperator, AgentOperator]
    while pending:
        cls = pending.pop()
        pending.extend(cls.__subclasses__())
        # Subclasses defined by other test modules are not shipped, so only the provider's own count.
        if cls.__module__.startswith("airflow.providers.common.ai."):
            names.add(getattr(cls, "custom_operator_name", None) or cls.__name__)
    return names


class TestModelPanelPlugin:
    def test_plugin_name(self):
        assert ModelPanelPlugin.name == "ai_model_panel"

    def test_fastapi_apps_registered(self):
        assert len(ModelPanelPlugin.fastapi_apps) == 1
        assert ModelPanelPlugin.fastapi_apps[0]["name"] == "ai-model-panel"
        assert "url_prefix" in ModelPanelPlugin.fastapi_apps[0]

    @requires_3_4
    def test_react_apps_registered(self):
        assert len(ModelPanelPlugin.react_apps) == 1
        app = ModelPanelPlugin.react_apps[0]
        assert app["name"] == "AI Model"
        assert app["url_route"] == "ai-model"
        assert app["destination"] == "task_instance"
        assert "model.umd.cjs" in app["bundle_url"]

    @pytest.mark.skipif(
        AIRFLOW_V_3_4_PLUS, reason="exercises the pre-3.4 gate that withholds react_apps entirely"
    )
    def test_react_apps_withheld_before_3_4(self):
        assert ModelPanelPlugin.react_apps == []

    @requires_3_4
    def test_applies_to_scopes_to_operators_that_publish_model_name(self):
        """Built from the in-tree subclasses, so a new LLMOperator subclass or a rename can't silently
        desync the tab's scoping from the operators that publish the model name."""
        app = ModelPanelPlugin.react_apps[0]
        assert set(app["applies_to"]["operator_name"]) == get_in_tree_model_operator_names()

    @requires_3_4
    def test_react_app_survives_core_applies_to_validation(self):
        """Core withholds a React App whose applies_to path names no TaskInstanceResponse field."""
        plugin = ModelPanelPlugin()
        # Core rewrites each view's applies_to in place, so validate a copy, not the class-level list.
        plugin.react_apps = copy.deepcopy(ModelPanelPlugin.react_apps)

        with mock_plugin_manager(plugins=[plugin]):
            _, react_apps = plugins_manager._get_ui_plugins()

        assert [app["name"] for app in react_apps] == ["AI Model"]
