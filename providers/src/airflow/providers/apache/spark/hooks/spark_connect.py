#
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
from urllib.parse import quote, urlparse, urlunparse

from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class SparkConnectHook(BaseHook, LoggingMixin):
    """Hook for Spark Connect."""

    # from pyspark's ChannelBuilder
    PARAM_USE_SSL = "use_ssl"
    PARAM_TOKEN = "token"
    PARAM_USER_ID = "user_id"

    conn_name_attr = "conn_id"
    default_conn_name = "spark_connect_default"
    conn_type = "spark_connect"
    hook_name = "Spark Connect"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for Spark Connect connection."""
        return {
            "hidden_fields": [
                "schema",
            ],
            "relabeling": {"password": "Token", "login": "User ID"},
        }

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to Spark Connect connection form."""
        from flask_babel import lazy_gettext
        from wtforms import BooleanField

        return {
            SparkConnectHook.PARAM_USE_SSL: BooleanField(
                lazy_gettext("Use SSL"), default=False
            ),
        }

    def __init__(self, conn_id: str = default_conn_name) -> None:
        super().__init__()
        self._conn_id = conn_id

    def get_connection_url(self) -> str:
        conn = self.get_connection(self._conn_id)

        host = conn.host
        if conn.host.find("://") == -1:
            host = f"sc://{conn.host}"
        if conn.port:
            host = f"{conn.host}:{conn.port}"

        url = urlparse(host)

        if url.path:
            raise ValueError(
                "Path {url.path} is not supported in Spark Connect connection URL"
            )

        params = []

        if conn.login:
            params.append(f"{SparkConnectHook.PARAM_USER_ID}={quote(conn.login)}")

        if conn.password:
            params.append(f"{SparkConnectHook.PARAM_TOKEN}={quote(conn.password)}")

        use_ssl = conn.extra_dejson.get(SparkConnectHook.PARAM_USE_SSL)
        if use_ssl is not None:
            params.append(f"{SparkConnectHook.PARAM_USE_SSL}={quote(str(use_ssl))}")

        return urlunparse(
            (
                "sc",
                url.netloc,
                "/",
                ";".join(params),  # params
                "",
                url.fragment,
            )
        )
