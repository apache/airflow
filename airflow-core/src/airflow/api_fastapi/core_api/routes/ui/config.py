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

from typing import Any

from fastapi import Depends, status

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.config import ConfigResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import GetUserDep, requires_authenticated
from airflow.configuration import conf
from airflow.settings import DASHBOARD_UIALERTS
from airflow.utils.log.log_reader import TaskLogReader
from airflow import plugins_manager

config_router = AirflowRouter(tags=["Config"])


API_CONFIG_KEYS = [
    "enable_swagger_ui",
    "hide_paused_dags_by_default",
    "page_size",
    "default_wrap",
    "auto_refresh_interval",
    "require_confirmation_dag_change",
]


@config_router.get(
    "/config",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_authenticated())],
)
async def get_configs(user: GetUserDep) -> ConfigResponse:
    """Get configs for UI."""
    config = {key: conf.get("api", key) for key in API_CONFIG_KEYS}

    task_log_reader = TaskLogReader()
    plugins_manager.initialize_ui_plugins()

    auth_manager = get_auth_manager()
    authorized_plugins = []
    if plugins_manager.external_views:
        for view in plugins_manager.external_views:
            if auth_manager.is_authorized_custom_view(method="GET", resource_name=view["name"], user=user):
                authorized_plugins.append(view)

    additional_config: dict[str, Any] = {
        "instance_name": conf.get("api", "instance_name", fallback="Airflow"),
        "test_connection": conf.get("core", "test_connection", fallback="Disabled"),
        "dashboard_alert": DASHBOARD_UIALERTS,
        "show_external_log_redirect": task_log_reader.supports_external_link,
        "external_log_name": getattr(task_log_reader.log_handler, "log_name", None),
        "plugins_extra_menu_items": [
            view for view in authorized_plugins if view.get("destination") == "nav"
        ],
    }

    config.update({key: value for key, value in additional_config.items()})

    return ConfigResponse.model_validate(config)
