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

from airflow.providers.common.ai.plugins.model_panel import ModelPanelPlugin

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_4_PLUS


def test_model_panel_plugin_registration_matches_airflow_version():
    # react_apps needs the applies_to scoping that only core >= 3.4 understands (see
    # model_panel.py); fastapi_apps (the static bundle server) has no such dependency.
    if AIRFLOW_V_3_1_PLUS:
        assert len(ModelPanelPlugin.fastapi_apps) == 1
    else:
        assert ModelPanelPlugin.fastapi_apps == []
    if AIRFLOW_V_3_4_PLUS:
        assert len(ModelPanelPlugin.react_apps) == 1
    else:
        assert ModelPanelPlugin.react_apps == []


@pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="Requires Airflow 3.1+")
def test_model_panel_plugin_registers_expected_fastapi_app_name():
    assert ModelPanelPlugin.fastapi_apps[0]["name"] == "ai-model-panel"


@pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="react_apps' applies_to scoping needs Airflow >= 3.4.0")
def test_model_panel_plugin_registers_expected_react_app_name():
    assert ModelPanelPlugin.react_apps[0]["name"] == "AI Model"
