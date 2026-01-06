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

from collections.abc import Callable, Iterable, Mapping
from typing import Any, overload

from vertica_python import connect

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.common.sql.hooks.sql import DbApiHook

try:
    from sqlalchemy.engine import URL
except ImportError:
    URL = None


def vertica_fetch_all_handler(cursor) -> list[tuple] | None:
    """
    Replace the default DbApiHook fetch_all_handler in order to fix this issue https://github.com/apache/airflow/issues/32993.

    Returned value will not change after the initial call of fetch_all_handler, all the remaining code is here
    only to make vertica client throws error.
    With Vertica, if you run the following sql (with split_statements set to false):

    INSERT INTO MyTable (Key, Label) values (1, 'test 1');
    INSERT INTO MyTable (Key, Label) values (1, 'test 2');
    INSERT INTO MyTable (Key, Label) values (3, 'test 3');

    each insert will have its own result set and if you don't try to fetch data of those result sets
    you won't detect error on the second insert.
    """
    result = fetch_all_handler(cursor)
    # loop on all statement result sets to get errors
    if cursor.description is not None:
        while cursor.nextset():
            if cursor.description is not None:
                row = cursor.fetchone()
                while row:
                    row = cursor.fetchone()
    return result


class VerticaHook(DbApiHook):
    """
    Interact with Vertica.

    This hook use a customized version of default fetch_all_handler named vertica_fetch_all_handler.
    """

    conn_name_attr = "vertica_conn_id"
    default_conn_name = "vertica_default"
    conn_type = "vertica"
    hook_name = "Vertica"
    supports_autocommit = True

    def get_conn(self) -> connect:
        """Return vertica connection object."""
        conn = self.get_connection(self.get_conn_id())
        conn_config: dict[str, Any] = {
            "user": conn.login,
            "password": conn.password or "",
            "database": conn.schema,
            "host": conn.host or "localhost",
        }

        if not conn.port:
            conn_config["port"] = 5433
        else:
            conn_config["port"] = int(conn.port)

        bool_options = [
            "connection_load_balance",
            "binary_transfer",
            "disable_copy_local",
            "request_complex_types",
            "use_prepared_statements",
        ]
        std_options = [
            "session_label",
            "backup_server_node",
            "kerberos_host_name",
            "kerberos_service_name",
            "unicode_error",
            "workload",
            "ssl",
        ]
        conn_extra = conn.extra_dejson

        for bo in bool_options:
            if bo in conn_extra:
                conn_config[bo] = str(conn_extra[bo]).lower() in ["true", "on"]

        for so in std_options:
            if so in conn_extra:
                conn_config[so] = conn_extra[so]

        if "connection_timeout" in conn_extra:
            conn_config["connection_timeout"] = float(conn_extra["connection_timeout"])

        if "log_level" in conn_extra:
            import logging

            log_lvl = conn_extra["log_level"]
            conn_config["log_path"] = None
            if isinstance(log_lvl, str):
                log_lvl = log_lvl.lower()
                if log_lvl == "critical":
                    conn_config["log_level"] = logging.CRITICAL
                elif log_lvl == "error":
                    conn_config["log_level"] = logging.ERROR
                elif log_lvl == "warning":
                    conn_config["log_level"] = logging.WARNING
                elif log_lvl == "info":
                    conn_config["log_level"] = logging.INFO
                elif log_lvl == "debug":
                    conn_config["log_level"] = logging.DEBUG
                elif log_lvl == "notset":
                    conn_config["log_level"] = logging.NOTSET
            else:
                conn_config["log_level"] = int(conn_extra["log_level"])

        conn = connect(**conn_config)
        return conn

    @property
    def sqlalchemy_url(self) -> URL:
        """Return a SQLAlchemy URL object with properly formatted query parameters."""
        if URL is None:
            raise AirflowOptionalProviderFeatureException(
                "The 'sqlalchemy' library is required to use 'sqlalchemy_url'."
                "Please install it with: pip install 'apache-airflow-providers-vertica[sqlalchemy]'"
            )
        conn = self.get_connection(self.get_conn_id())
        extra = conn.extra_dejson or {}

        # Normalize query dictionary
        query = {
            k: ([str(x) for x in v] if isinstance(v, (list, tuple)) else str(v))
            for k, v in extra.items()
            if v is not None
        }

        return URL.create(
            drivername="vertica-python",
            username=conn.login,
            password=conn.password or "",
            host=conn.host or "localhost",
            port=conn.port or 5433,
            database=conn.schema,
            query=query,
        )

    def get_uri(self) -> str:
        """Return a URI string with password visible."""
        return self.sqlalchemy_url.render_as_string(hide_password=False)

    @overload
    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = ...,
        parameters: Iterable | Mapping[str, Any] | None = ...,
        handler: None = ...,
        split_statements: bool = ...,
        return_last: bool = ...,
    ) -> None: ...

    @overload
    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = ...,
        parameters: Iterable | Mapping[str, Any] | None = ...,
        handler: Callable[[Any], Any] = ...,
        split_statements: bool = ...,
        return_last: bool = ...,
    ) -> Any | list[Any]: ...

    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = False,
        parameters: Iterable | Mapping | None = None,
        handler: Callable[[Any], Any] | None = None,
        split_statements: bool = False,
        return_last: bool = True,
    ) -> Any | list[Any] | None:
        """
        Overwrite the common sql run.

        Will automatically replace fetch_all_handler by vertica_fetch_all_handler.
        """
        if handler == fetch_all_handler:
            handler = vertica_fetch_all_handler
        return DbApiHook.run(self, sql, autocommit, parameters, handler, split_statements, return_last)
