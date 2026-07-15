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

import mimetypes
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from fastapi.staticfiles import StaticFiles

from airflow.plugins_manager import AirflowPlugin
from airflow.providers.common.compat.sdk import conf
from airflow.providers.common.dataquality.api.app import dataquality_app
from airflow.providers.common.dataquality.version_compat import AIRFLOW_V_3_1_PLUS

_PLUGIN_PREFIX = "/dataquality"


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
    Return the bundle URL for the React plugin.

    Uses an absolute URL when ``api.base_url`` is a full URL so the bundle loads correctly in
    Vite dev mode, where ``import()`` resolves relative to the script origin (5175) rather
    than the document origin (28080).
    """
    path = _get_base_url_path(f"{_PLUGIN_PREFIX}/static/main.umd.cjs")
    base_url = conf.get("api", "base_url", fallback="/")
    if base_url.startswith(("http://", "https://")):
        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}" + path
    return path


# Ensure proper MIME type for the plugin bundle (FastAPI serves .cjs as text/plain by default).
mimetypes.add_type("application/javascript", ".cjs")


def _react_apps(dist_dir: Path) -> list[dict[str, str]]:
    """
    React app entries, gated on the bundle actually being built.

    ``dist_dir`` isn't populated by a plain source checkout — it's a build artifact produced
    by ``pnpm build`` (wired into the wheel build via ``hatch_build.py``). Advertising the
    entry before that would point the UI at a 404.
    """
    if not dist_dir.is_dir():
        return []
    return [
        {
            "name": "Data Quality",
            "bundle_url": _get_bundle_url(),
            "destination": "task",
            "url_route": "dataquality-task",
        },
        {
            "name": "Data Quality",
            "bundle_url": _get_bundle_url(),
            "destination": "task_instance",
            "url_route": "dataquality-run",
        },
    ]


_WWW_DIR = Path(__file__).parent / "www"
_dist_dir = _WWW_DIR / "dist"
if AIRFLOW_V_3_1_PLUS and _dist_dir.is_dir():
    dataquality_app.mount(
        "/static", StaticFiles(directory=str(_dist_dir.absolute())), name="dataquality_static"
    )


class DataQualityPlugin(AirflowPlugin):
    """Register the data quality read-only query API and task-instance React view."""

    name = "dataquality"
    fastapi_apps: list[dict[str, Any]] = (
        [{"name": "dataquality", "app": dataquality_app, "url_prefix": _PLUGIN_PREFIX}]
        if AIRFLOW_V_3_1_PLUS
        else []
    )
    react_apps: list[dict[str, str]] = _react_apps(_dist_dir) if AIRFLOW_V_3_1_PLUS else []
