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

from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.config import ConfigResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_configuration
from airflow.configuration import conf
from airflow.settings import DASHBOARD_UIALERTS

config_router = AirflowRouter(tags=["Config"])

WEBSERVER_CONFIG_KEYS = [
    "navbar_color",
    "page_size",
    "auto_refresh_interval",
    "hide_paused_dags_by_default",
    "warn_deployment_exposure",
    "default_wrap",
    "require_confirmation_dag_change",
    "enable_swagger_ui",
    "instance_name_has_markup",
    "navbar_text_color",
    "navbar_hover_color",
    "navbar_text_hover_color",
]


@config_router.get(
    "/config",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_configuration("GET"))],
)
def get_configs() -> ConfigResponse:
    """Get configs for UI."""
    conf_dict = conf.as_dict()

    config = {key: conf_dict["webserver"].get(key) for key in WEBSERVER_CONFIG_KEYS}

    additional_config: dict[str, Any] = {
        "instance_name": conf.get("webserver", "instance_name", fallback="Airflow"),
        "audit_view_included_events": conf.get("webserver", "audit_view_included_events", fallback=""),
        "audit_view_excluded_events": conf.get("webserver", "audit_view_excluded_events", fallback=""),
        "test_connection": conf.get("core", "test_connection", fallback="Disabled"),
        "dashboard_alert": DASHBOARD_UIALERTS,
    }

    config.update({key: value for key, value in additional_config.items()})

    return ConfigResponse.model_validate(config)
