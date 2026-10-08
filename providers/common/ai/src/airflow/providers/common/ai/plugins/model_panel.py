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

from typing import TYPE_CHECKING

from airflow.plugins_manager import AirflowPlugin
from airflow.providers.common.ai.plugins._plugin_urls import get_bundle_url
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_4_PLUS

if TYPE_CHECKING:
    from airflow.plugins_manager import FastAPIAppDict, ReactAppDict

_PLUGIN_PREFIX = "/ai-model"


if AIRFLOW_V_3_1_PLUS:
    import mimetypes
    from pathlib import Path

    from fastapi import FastAPI
    from fastapi.staticfiles import StaticFiles

    # Ensure proper MIME type for the plugin bundle (FastAPI serves .cjs as text/plain by default).
    mimetypes.add_type("application/javascript", ".cjs")

    model_panel_app = FastAPI(
        title="AI Model Panel",
        description="Serves the static 'Model' tab bundle for task instances run with common.ai operators.",
    )

    _WWW_DIR = Path(__file__).parent / "www"
    _dist_dir = _WWW_DIR / "dist"
    if _dist_dir.is_dir():
        model_panel_app.mount(
            "/static",
            StaticFiles(directory=str(_dist_dir.absolute()), html=True),
            name="model_panel_static",
        )


class ModelPanelPlugin(AirflowPlugin):
    """Register the 'Model' tab showing the resolved LLM model name and token usage."""

    name = "ai_model_panel"
    fastapi_apps: list[FastAPIAppDict] = []
    react_apps: list[ReactAppDict] = []
    if AIRFLOW_V_3_1_PLUS:
        fastapi_apps = [
            {
                "name": "ai-model-panel",
                "app": model_panel_app,
                "url_prefix": _PLUGIN_PREFIX,
            }
        ]
    if AIRFLOW_V_3_4_PLUS:
        # `applies_to` (the per-operator scoping below) only came into core with
        # apache/airflow#69148 (3.4.0). An older core silently accepts the extra key and
        # ignores it, so the tab would show up on every task instance instead and land on
        # the empty state for anything that isn't an LLM/Agent run -- the whole entry is
        # withheld pre-3.4 rather than shipping that unscoped tab.
        react_apps = [
            {
                "name": "AI Model",
                "bundle_url": get_bundle_url(_PLUGIN_PREFIX, "model.umd.cjs"),
                "destination": "task_instance",
                "url_route": "ai-model",
                # Only LLMOperator and AgentOperator (and their @task.llm/@task.agent
                # TaskFlow forms) push model_name/usage; other operators would show
                # a permanently-empty tab. `operator_name` alone covers both the raw
                # class name and the decorator's display name, since TaskInstance falls
                # back to the class name when there's no custom_operator_name.
                "applies_to": {
                    "operator_names": [
                        "LLMOperator",
                        "AgentOperator",
                        "@task.llm",
                        "@task.agent",
                    ],
                },
            }
        ]
