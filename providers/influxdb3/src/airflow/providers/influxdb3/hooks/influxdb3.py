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
"""
This module allows to connect to an InfluxDB 3 database.

InfluxDB 3.x (Core/Enterprise/Cloud Dedicated) uses SQL queries and a different
API compared to InfluxDB 2.x. This provider is specifically designed for InfluxDB 3.x.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

try:
    from influxdb3 import InfluxDBClient3, Point
    INFLUXDB_CLIENT_3_AVAILABLE = True
except ImportError:
    try:
        # Alternative import path
        from influxdb_client_3 import InfluxDBClient3, Point
        INFLUXDB_CLIENT_3_AVAILABLE = True
    except ImportError:
        INFLUXDB_CLIENT_3_AVAILABLE = False
        InfluxDBClient3 = None  # type: ignore[assignment, misc]
        Point = None  # type: ignore[assignment, misc]

from airflow.providers.common.compat.sdk import BaseHook

import pandas as pd

if TYPE_CHECKING:
    from airflow.models import Connection


class InfluxDB3Hook(BaseHook):
    """
    Interact with InfluxDB 3.x (Core/Enterprise/Cloud Dedicated).

    Performs a connection to InfluxDB 3.x and retrieves client.

    :param influxdb3_conn_id: Reference to :ref:`InfluxDB 3 connection id <howto/connection:influxdb3>`.
    """

    conn_name_attr = "influxdb3_conn_id"
    default_conn_name = "influxdb3_default"
    conn_type = "influxdb3"
    hook_name = "InfluxDB 3"

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.influxdb3_conn_id = conn_id
        self.connection: Connection | None = kwargs.pop("connection", None)
        self.client: InfluxDBClient3 | None = None
        self.extras: dict[str, Any] = {}
        self.uri: str | None = None

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "token": StringField(lazy_gettext("Token"), widget=BS3TextFieldWidget(), default=""),
            "database": StringField(
                lazy_gettext("Database"),
                widget=BS3TextFieldWidget(),
                default="",
            ),
            "org": StringField(
                lazy_gettext("Organization Name (optional)"),
                widget=BS3TextFieldWidget(),
                default="",
            ),
        }

    def get_client(self, uri: str, kwargs: dict[str, Any]) -> InfluxDBClient3:
        """Get InfluxDB 3.x client."""
        if not INFLUXDB_CLIENT_3_AVAILABLE:
            raise ImportError(
                "influxdb3-python is required for InfluxDB 3.x support. "
                "Install it with: pip install influxdb3-python"
            )

        database = kwargs.pop("database", None) or kwargs.pop("db", None)
        if not database:
            raise ValueError("database parameter is required for InfluxDB 3.x")

        return InfluxDBClient3(
            host=uri,
            token=kwargs.get("token"),
            database=database,
            org=kwargs.get("org", ""),
        )

    def get_uri(self, conn: Connection) -> str:
        """Build URI from connection parameters."""
        conn_scheme = "https" if conn.schema is None else conn.schema
        
        # Use appropriate default port based on scheme
        if conn.port is None:
            conn_port = 443 if conn_scheme == "https" else 8086
        else:
            conn_port = conn.port
        
        # For InfluxDB Cloud Dedicated, if host ends with .influxdb.io and using HTTPS,
        # default to port 443 if port is 8086 (common misconfiguration)
        if (
            conn_scheme == "https"
            and conn.host
            and conn.host.lower().endswith(".influxdb.io")
            and conn_port == 8086
        ):
            self.log.warning(
                "InfluxDB Cloud Dedicated detected with HTTPS but port 8086. "
                "Switching to port 443. If this is incorrect, explicitly set the port in connection."
            )
            conn_port = 443
        
        host = conn.host or ""
        return f"{conn_scheme}://{host}:{conn_port}"

    def get_conn(self) -> InfluxDBClient3:
        """
        Initiate a new InfluxDB 3.x connection with token and database.
        
        Reads connection parameters from:
        - Custom form fields (token, database, org) - automatically stored in extras
        - Connection password field (as fallback for token)
        - Connection extras JSON (for manual configuration)
        """
        self.connection = self.get_connection(self.influxdb3_conn_id)
        self.extras = self.connection.extra_dejson.copy()

        self.uri = self.get_uri(self.connection)
        self.log.info("URI: %s", self.uri)

        if self.client is not None:
            return self.client

        # Token: prefer extras (from form widget), fallback to password field
        if "token" not in self.extras or not self.extras.get("token"):
            token = getattr(self.connection, "password", None)
            if token:
                self.extras["token"] = token
            elif not self.extras.get("token"):
                raise ValueError(
                    "token is required for InfluxDB 3.x. "
                    "Set it in the 'Token' field of the connection form or in connection extras."
                )

        # Database: required for InfluxDB 3.x (from form widget or extras)
        database = self.extras.get("database") or self.extras.get("db")
        if not database:
            raise ValueError(
                "database is required for InfluxDB 3.x. "
                "Set it in the 'Database' field of the connection form or in connection extras."
            )
        self.extras["database"] = database

        # Org: optional (from form widget or extras)
        if "org" not in self.extras:
            self.extras["org"] = ""

        self.client = self.get_client(self.uri, self.extras)

        return self.client

    def query(self, query: str) -> pd.DataFrame:
        """
        Run a SQL query and return results as a pandas DataFrame.

        :param query: SQL query string
        :return: pandas DataFrame with query results
        """
        client = self.get_conn()
        result = client.query(query=query, language="sql", mode="pandas")
        
        if not isinstance(result, pd.DataFrame):
            raise ValueError(
                f"Query did not return a DataFrame. "
                f"Result type: {type(result).__module__}.{type(result).__name__}"
            )
        
        return result

    def write(
        self,
        measurement: str,
        tags: dict[str, str] | None = None,
        fields: dict[str, Any] | None = None,
    ) -> None:
        """
        Write a Point to the database.

        :param measurement: Measurement name
        :param tags: Dictionary of tags (key-value pairs)
        :param fields: Dictionary of fields (key-value pairs)

        Example::
            hook.write(
                measurement="temperature",
                tags={"location": "Prague", "sensor": "A1"},
                fields={"value": 25.3, "unit": "celsius"}
            )
        """
        if not INFLUXDB_CLIENT_3_AVAILABLE or Point is None:
            raise ImportError(
                "influxdb3-python is required for InfluxDB 3.x support. "
                "Install it with: pip install influxdb3-python"
            )
        if not fields:
            raise ValueError("At least one field is required")

        client = self.get_conn()

        # Create Point
        point = Point(measurement)
        if tags:
            for tag_key, tag_val in tags.items():
                point = point.tag(tag_key, tag_val)
        for field_key, field_val in fields.items():
            point = point.field(field_key, field_val)

        client.write(record=point)
