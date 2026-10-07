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

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS


def test_model_panel_plugin_registration_matches_airflow_version():
    if AIRFLOW_V_3_1_PLUS:
        assert len(ModelPanelPlugin.fastapi_apps) == 1
        assert len(ModelPanelPlugin.react_apps) == 1
    else:
        assert ModelPanelPlugin.fastapi_apps == []
        assert ModelPanelPlugin.react_apps == []


@pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="Requires Airflow 3.1+")
def test_model_panel_plugin_registers_expected_app_names():
    assert ModelPanelPlugin.fastapi_apps[0]["name"] == "ai-model-panel"
    assert ModelPanelPlugin.react_apps[0]["name"] == "Model"
