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

import sys
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.edge3.version_compat import AIRFLOW_V_3_1_PLUS
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

from airflow.utils.db import DBLocks, create_global_lock


@provide_session
def _get_api_endpoint(session: Session = NEW_SESSION) -> dict[str, Any]:
    # Ensure all required DB modeals are created before starting the API
    with create_global_lock(session=session, lock=DBLocks.MIGRATIONS):
        engine = session.get_bind().engine
        from airflow.providers.edge3.models.edge_job import EdgeJobModel
        from airflow.providers.edge3.models.edge_logs import EdgeLogsModel
        from airflow.providers.edge3.models.edge_worker import EdgeWorkerModel

        EdgeJobModel.metadata.create_all(engine)
        EdgeLogsModel.metadata.create_all(engine)
        EdgeWorkerModel.metadata.create_all(engine)

    from airflow.providers.edge3.worker_api.app import create_edge_worker_api_app

    return {
        "app": create_edge_worker_api_app(),
        "url_prefix": "/edge_worker",
        "name": "Airflow Edge Worker",
    }


# Check if EdgeExecutor is actually loaded
try:
    EDGE_EXECUTOR_ACTIVE = conf.getboolean("edge", "api_enabled", fallback="False")
except AirflowConfigException:
    EDGE_EXECUTOR_ACTIVE = False

# Load the API endpoint only on api-server
# TODO(jscheffl): Remove this check when the discussion in
#                 https://lists.apache.org/thread/w170czq6r7bslkqp1tk6bjjjo0789wgl
#                 resulted in a proper API to selective initialize. Maybe backcompat-shim
#                 is also needed to support Airflow-versions prior the rework.
RUNNING_ON_APISERVER = (len(sys.argv) > 1 and sys.argv[1] in ["api-server"]) or (
    len(sys.argv) > 2 and sys.argv[2] == "airflow-core/src/airflow/api_fastapi/main.py"
)


def _get_base_url_path(path: str) -> str:
    """Construct URL path with webserver base_url prefix."""
    base_url = conf.get("api", "base_url", fallback="/")
    # Extract pathname from base_url (handles both full URLs and path-only)
    if base_url.startswith(("http://", "https://")):
        from urllib.parse import urlparse

        base_path = urlparse(base_url).path
    else:
        base_path = base_url

    # Normalize paths: remove trailing slash from base, ensure leading slash on path
    base_path = base_path.rstrip("/")
    return base_path + path


class EdgeExecutorPlugin(AirflowPlugin):
    """EdgeExecutor Plugin - provides API endpoints for Edge Workers in Webserver."""

    name = "edge_executor"
    if EDGE_EXECUTOR_ACTIVE and RUNNING_ON_APISERVER:
        fastapi_apps = [_get_api_endpoint()]
        if AIRFLOW_V_3_1_PLUS:
            # Airflow 3.0 does not know about react_apps, so we only provide the API endpoint
            react_apps = [
                {
                    "name": "Edge Executor",
                    "bundle_url": _get_base_url_path("/edge_worker/static/main.umd.cjs"),
                    "destination": "nav",
                    "url_route": "edge_executor",
                    "category": "admin",
                    "icon": _get_base_url_path("/edge_worker/res/cloud-computer.svg"),
                    "icon_dark_mode": _get_base_url_path("/edge_worker/res/cloud-computer-dark.svg"),
                },
            ]
            external_views = [
                {
                    "name": "Edge Worker API docs",
                    "href": _get_base_url_path("/edge_worker/docs"),
                    "destination": "nav",
                    "category": "docs",
                    "icon": _get_base_url_path("/edge_worker/res/cloud-computer.svg"),
                    "icon_dark_mode": _get_base_url_path("/edge_worker/res/cloud-computer-dark.svg"),
                    "url_route": "edge_worker_api_docs",
                }
            ]
