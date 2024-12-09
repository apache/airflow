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

import logging

import pendulum

import airflow
from airflow.api_fastapi.app import get_auth_manager
from airflow.configuration import conf
from airflow.settings import STATE_COLORS
from airflow.utils.net import get_hostname
from airflow.utils.platform import get_airflow_git_version

logger = logging.getLogger(__name__)


def init_jinja_globals(app):
    """Add extra globals variable to Jinja context."""
    server_timezone = conf.get("core", "default_timezone")
    if server_timezone == "system":
        server_timezone = pendulum.local_timezone().name
    elif server_timezone == "utc":
        server_timezone = "UTC"

    default_ui_timezone = conf.get("webserver", "default_ui_timezone")
    if default_ui_timezone == "system":
        default_ui_timezone = pendulum.local_timezone().name
    elif default_ui_timezone == "utc":
        default_ui_timezone = "UTC"
    if not default_ui_timezone:
        default_ui_timezone = server_timezone

    expose_hostname = conf.getboolean("webserver", "EXPOSE_HOSTNAME")
    hostname = get_hostname() if expose_hostname else "redact"

    try:
        airflow_version = airflow.__version__
    except Exception as e:
        airflow_version = None
        logger.error(e)

    git_version = get_airflow_git_version()

    def prepare_jinja_globals():
        extra_globals = {
            "server_timezone": server_timezone,
            "default_ui_timezone": default_ui_timezone,
            "hostname": hostname,
            "navbar_color": conf.get("webserver", "NAVBAR_COLOR"),
            "navbar_text_color": conf.get("webserver", "NAVBAR_TEXT_COLOR"),
            "navbar_hover_color": conf.get("webserver", "NAVBAR_HOVER_COLOR"),
            "navbar_text_hover_color": conf.get("webserver", "NAVBAR_TEXT_HOVER_COLOR"),
            "navbar_logo_text_color": conf.get("webserver", "NAVBAR_LOGO_TEXT_COLOR"),
            "state_color_mapping": STATE_COLORS,
            "airflow_version": airflow_version,
            "git_version": git_version,
        }

        # Extra global specific to auth manager
        extra_globals["auth_manager"] = get_auth_manager()

        return extra_globals

    app.context_processor(prepare_jinja_globals)
