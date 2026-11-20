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

import functools
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any, overload

from vertica_python import connect

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from sqlalchemy.engine import URL


def _drain_result_sets(cursor) -> None:
    """
    Consume all remaining rows and result sets of the cursor.

    vertica-python streams results: an error raised by a statement other than the first one in a
    multi-statement query is only reported once the client reads up to it. ``nextset()`` alone is
    not enough: it skips the rest of the current result set, including an error that arrives after
    the set has started returning rows (e.g. ``SELECT a/b FROM t`` failing on its second row), and
    on vertica-python < 1.3.5 skipping can hang instead. So every result set is read row by row
    with ``fetchone()``, which raises on such an error, before moving to the next one.

    ``nextset()`` also overwrites ``cursor.description`` with the columns of each next result set,
    while ``DbApiHook.run`` reads it after the handler returns. The description of the result set
    the handler consumed is restored, so that ``hook.descriptions`` matches the returned rows.

    See https://github.com/apache/airflow/issues/32993 and
    https://github.com/vertica/vertica-python/issues/197.
    """
    description = cursor.description
    while True:
        while cursor.fetchone() is not None:
            pass
        if not cursor.nextset():
            break
    cursor.description = description


def _with_result_set_drain(handler: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Wrap a cursor handler so that all result sets are consumed after the handler has run."""

    @functools.wraps(handler)
    def wrapper(cursor):
        result = handler(cursor)
        _drain_result_sets(cursor)
        return result

    return wrapper


def vertica_fetch_all_handler(cursor) -> list[tuple] | None:
    """
    Return all rows of the current result set, then consume the remaining result sets.

    Kept for backward compatibility: ``VerticaHook.run`` now drains the result sets for every
    handler, so passing ``fetch_all_handler`` is enough. See https://github.com/apache/airflow/issues/32993.
    """
    result = fetch_all_handler(cursor)
    _drain_result_sets(cursor)
    return result


class VerticaHook(DbApiHook):
    """
    Interact with Vertica.

    ``run`` consumes all rows and result sets after each executed statement, whatever the handler is,
    so that an error raised by any statement of a multi-statement query is not silently ignored.
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
        """
        Return a SQLAlchemy URL object with properly formatted query parameters.

        The URL uses the ``vertica+vertica_python`` scheme: Vertica SQLAlchemy dialects register
        themselves as ``vertica.vertica_python``, so a dialect package (for SQLAlchemy 2.x, for example
        ``sqlalchemy-vertica``) has to be installed for ``get_sqlalchemy_engine()`` to work.
        """
        try:
            from sqlalchemy.engine import URL
        except (ImportError, ModuleNotFoundError) as err:
            raise AirflowOptionalProviderFeatureException(
                "The 'sqlalchemy' library is required to use this feature. "
                "Please install it with: pip install 'apache-airflow-providers-vertica[sqlalchemy]'"
            ) from err
        conn = self.get_connection(self.get_conn_id())
        extra = conn.extra_dejson or {}

        # Normalize query dictionary
        query = {
            k: ([str(x) for x in v] if isinstance(v, (list, tuple)) else str(v))
            for k, v in extra.items()
            if v is not None
        }

        return URL.create(
            drivername="vertica+vertica_python",
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

        vertica-python only reports an error raised by a statement once its result set is reached, so
        every result set is consumed after each executed statement, whatever the handler is (including
        when no handler is passed). Otherwise a failing statement in a multi-statement query could be
        silently ignored.
        """
        if handler is None:
            DbApiHook.run(
                self, sql, autocommit, parameters, _drain_result_sets, split_statements, return_last
            )
            # Without a handler, DbApiHook.run does not collect descriptions; keep that behaviour.
            self.descriptions = []
            return None
        return DbApiHook.run(
            self, sql, autocommit, parameters, _with_result_set_drain(handler), split_statements, return_last
        )
