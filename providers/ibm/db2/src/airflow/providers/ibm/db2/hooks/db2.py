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
"""This module allows to connect to an IBM Db2 database."""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any
from urllib.parse import quote_plus, urlencode

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.ibm.db2.dialects.db2 import Db2Dialect

if TYPE_CHECKING:
    from airflow.providers.common.sql.dialects.dialect import Dialect


class Db2Hook(DbApiHook):
    """
    Interact with IBM Db2.

    You can specify SSL and other connection parameters in the extra field of your connection.

    **SSL Configuration:**
    - ``{"SECURITY": "SSL", "SSLServerCertificate": "/path/to/cert.crt"}`` - For SSL with server certificate
    - ``{"SECURITY": "SSL"}`` - For SSL without certificate validation

    **Other Parameters:**
    Any additional Db2 connection string parameters can be added to the extra field.
    Parameter names will be automatically converted to uppercase.

    :param db2_conn_id: The :ref:`Db2 connection id <howto/connection:Db2>`
        reference to a specific Db2 database.
    """

    conn_name_attr = "db2_conn_id"
    default_conn_name = "db2_default"
    conn_type = "Db2"
    hook_name = "IBM Db2"
    supports_autocommit = True
    supports_executemany = True
    _test_connection_sql = "SELECT 1 FROM SYSIBM.SYSDUMMY1"
    _placeholder = "?"

    @cached_property
    def dialect_name(self) -> str:
        return "db2"

    @cached_property
    def dialect(self) -> Dialect:
        return Db2Dialect(self)

    @staticmethod
    def _get_default_values(conn) -> dict[str, Any]:
        """Extract connection values with defaults."""
        return {
            "host": conn.host or "localhost",
            "port": conn.port or 50000,
            "schema": conn.schema or "",
            "login": conn.login or "",
            "password": conn.password or "",
        }

    def get_conn_str(self) -> str:
        """
        Build and return the DB2 connection string.

        Constructs a connection string from the connection parameters including
        host, port, database, credentials, and any extra parameters from the
        connection's extra field.

        :return: DB2 connection string
        """
        conn = self.get_connection(self.get_conn_id())
        defaults = self._get_default_values(conn)

        # Build connection string for Db2
        conn_str_parts = [
            f"DATABASE={defaults['schema']}",
            f"HOSTNAME={defaults['host']}",
            f"PORT={defaults['port']}",
            "PROTOCOL=TCPIP",
            f"UID={defaults['login']}",
            f"PWD={defaults['password']}",
        ]

        # Add extra connection parameters from connection extra field
        extra = conn.extra_dejson
        if extra:
            # Add all extra parameters to connection string
            # Parameter names are automatically converted to uppercase for Db2
            for key, value in extra.items():
                # Convert boolean values to appropriate strings
                if isinstance(value, bool):
                    converted_value = "true" if value else "false"
                else:
                    converted_value = value
                conn_str_parts.append(f"{key.upper()}={converted_value}")

        return ";".join(conn_str_parts) + ";"

    def get_conn(self) -> Any:
        """
        Return ibm_db_dbi connection object.

        :return: Db2 connection object
        """
        import ibm_db_dbi

        return ibm_db_dbi.connect(self.get_conn_str(), "", "")

    def get_uri(self) -> str:
        """
        Return connection URI for SQLAlchemy using ibm-db-sa dialect.

        Includes extra connection parameters (like SSL configuration) as query parameters.

        :return: SQLAlchemy connection URI
        """
        conn = self.get_connection(self.get_conn_id())
        defaults = self._get_default_values(conn)

        # URL encode password if it contains special characters
        password = quote_plus(defaults["password"]) if defaults["password"] else ""

        # Build base URI
        base_uri = f"db2+ibm_db://{defaults['login']}:{password}@{defaults['host']}:{defaults['port']}/{defaults['schema']}"

        # Add extra parameters as query string (e.g., SSL configuration)
        extra = conn.extra_dejson
        if extra:
            query_params = {}
            for key, value in extra.items():
                # Convert boolean values to appropriate strings
                if isinstance(value, bool):
                    query_params[key.upper()] = "true" if value else "false"
                else:
                    query_params[key.upper()] = str(value)

            query_string = urlencode(query_params)
            return f"{base_uri}?{query_string}"

        return base_uri
