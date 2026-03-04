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

from typing import Any, cast
from urllib.parse import quote, urlparse, urlunparse

from airflow.providers.common.compat.sdk import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class SailHook(BaseHook, LoggingMixin):
    """
    Hook for Sail — a Rust-native Spark Connect compatible engine.

    Supports two modes:
    - **Remote**: connects to an existing Sail server via the Spark Connect protocol (``sc://``).
    - **Local embedded**: starts a Sail server in-process using ``pysail``.

    :param conn_id: Airflow connection ID. When the connection has a host configured,
        the hook connects to a remote Sail server. When there is no connection or the
        host is empty, a local embedded server is started.
    """

    PARAM_USE_SSL = "use_ssl"
    PARAM_TOKEN = "token"
    PARAM_USER_ID = "user_id"

    conn_name_attr = "conn_id"
    default_conn_name = "sail_default"
    conn_type = "sail"
    hook_name = "Sail"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for Sail connection."""
        return {
            "hidden_fields": [
                "schema",
            ],
            "relabeling": {"password": "Token", "login": "User ID"},
        }

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to Sail connection form."""
        from flask_babel import lazy_gettext
        from wtforms import BooleanField

        return {
            SailHook.PARAM_USE_SSL: BooleanField(lazy_gettext("Use SSL"), default=False),
        }

    def __init__(self, conn_id: str = default_conn_name) -> None:
        super().__init__()
        self._conn_id = conn_id

    def get_connection_url(self) -> str:
        """
        Build a Spark Connect URL (``sc://host:port/``) from the Airflow connection.

        :return: Spark Connect URL string.
        :raises ValueError: If the connection URL contains a path segment.
        """
        conn = self.get_connection(self._conn_id)

        host = cast("str", conn.host)
        if host.find("://") == -1:
            host = f"sc://{host}"
        if conn.port:
            host = f"{host}:{conn.port}"

        url = urlparse(host)

        if url.path and url.path != "/":
            raise ValueError(f"Path {url.path} is not supported in Sail connection URL")

        params: list[str] = []

        if conn.login:
            params.append(f"{SailHook.PARAM_USER_ID}={quote(conn.login)}")

        if conn.password:
            params.append(f"{SailHook.PARAM_TOKEN}={quote(conn.password)}")

        use_ssl = conn.extra_dejson.get(SailHook.PARAM_USE_SSL)
        if use_ssl is not None:
            params.append(f"{SailHook.PARAM_USE_SSL}={quote(str(use_ssl))}")

        return urlunparse(
            (
                "sc",
                url.netloc,
                "/",
                ";".join(params),
                "",
                url.fragment,
            )
        )

    def start_local_server(self, port: int = 50051, background: bool = True) -> Any:
        """
        Start a local embedded Sail server using ``pysail``.

        :param port: Port to listen on. Defaults to ``50051``.
        :param background: Whether to run the server in a background thread.
        :return: The ``SparkConnectServer`` instance.
        """
        from pysail.spark import SparkConnectServer

        self.log.info("Starting local Sail server on port %s", port)
        server = SparkConnectServer(port=port)
        server.start(background=background)
        return server
