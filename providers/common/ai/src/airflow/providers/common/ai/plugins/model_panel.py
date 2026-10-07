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
from urllib.parse import urlparse

from airflow.plugins_manager import AirflowPlugin
from airflow.providers.common.compat.sdk import conf
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_1_PLUS

if TYPE_CHECKING:
    from airflow.plugins_manager import FastAPIAppDict, ReactAppDict

_PLUGIN_PREFIX = "/ai-model"


def _get_base_url_path(path: str) -> str:
    """Construct URL path with webserver base_url prefix for non-root deployments."""
    base_url = conf.get("api", "base_url", fallback="/")
    if base_url.startswith(("http://", "https://")):
        base_path = urlparse(base_url).path
    else:
        base_path = base_url
    base_path = base_path.rstrip("/")
    return base_path + path


def _get_bundle_url() -> str:
    """
    Return bundle URL for the React plugin.

    Uses an absolute URL when api.base_url is a full URL so the bundle loads
    correctly in Vite dev mode, where import() resolves relative to the script
    origin (5173) rather than the document origin (28080).
    """
    path = _get_base_url_path(f"{_PLUGIN_PREFIX}/static/model.umd.cjs")
    base_url = conf.get("api", "base_url", fallback="/")
    if base_url.startswith(("http://", "https://")):
        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}" + path
    return path


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
        react_apps = [
            {
                "name": "Model",
                "bundle_url": _get_bundle_url(),
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
