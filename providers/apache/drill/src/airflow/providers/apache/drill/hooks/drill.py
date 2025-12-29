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

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, NoReturn
from airflow.exceptions import AirflowOptionalProviderFeatureException
try:
    from sqlalchemy import create_engine
except ImportError:
    create_engine = None

from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection
    from sqlalchemy.pool import PoolProxiedConnection


class DrillHook(DbApiHook):
    """
    Interact with Apache Drill via sqlalchemy-drill.

    You can specify the SQLAlchemy dialect and driver that sqlalchemy-drill
    will employ to communicate with Drill in the extras field of your
    connection, e.g. ``{"dialect_driver": "drill+sadrill"}`` for communication
    over Drill's REST API.  See the sqlalchemy-drill documentation for
    descriptions of the supported dialects and drivers.

    You can specify the default storage_plugin for the sqlalchemy-drill
    connection using the extras field e.g. ``{"storage_plugin": "dfs"}``.
    """

    conn_name_attr = "drill_conn_id"
    default_conn_name = "drill_default"
    conn_type = "drill"
    hook_name = "Drill"
    supports_autocommit = False

    def get_conn(self) -> PoolProxiedConnection:
        """Establish a connection to Drillbit."""
        conn_md = self.get_connection(self.get_conn_id())

        if create_engine is None:

            raise AirflowOptionalProviderFeatureException("The 'sqlalchemy' library is required to use this hook.")

        creds = f"{conn_md.login}:{conn_md.password}@" if conn_md.login else ""
        database_url = (
            f"{conn_md.extra_dejson.get('dialect_driver', 'drill+sadrill')}://{creds}"
            f"{conn_md.host}:{conn_md.port}/"
            f"{conn_md.extra_dejson.get('storage_plugin', 'dfs')}"
        )
        if "?" in database_url:
            raise ValueError("Drill database_url should not contain a '?'")
        engine = create_engine(database_url)

        self.log.info(
            "Connected to the Drillbit at %s:%s as user %s", conn_md.host, conn_md.port, conn_md.login
        )
        return engine.raw_connection()

    def get_uri(self) -> str:
        """
        Return the connection URI.

        e.g: ``drill://localhost:8047/dfs``
        """
        conn_md = self.get_connection(self.get_conn_id())
        host = conn_md.host or ""
        if conn_md.port is not None:
            host += f":{conn_md.port}"
        conn_type = conn_md.conn_type or "drill"
        dialect_driver = conn_md.extra_dejson.get("dialect_driver", "drill+sadrill")
        storage_plugin = conn_md.extra_dejson.get("storage_plugin", "dfs")
        return f"{conn_type}://{host}/{storage_plugin}?dialect_driver={dialect_driver}"

    # The superclass DbApiHook's method implementation has a return type `None` and mypy fails saying
    # return type `NotImplementedError` is incompatible with it. Hence, we ignore the mypy error here.
    def set_autocommit(self, conn: Connection, autocommit: bool) -> NoReturn:
        raise NotImplementedError("There are no transactions in Drill.")

    # The superclass DbApiHook's method implementation has a return type `None` and mypy fails saying
    # return type `NotImplementedError` is incompatible with it. Hence, we ignore the mypy error here.
    def insert_rows(
        self,
        table: str,
        rows: Iterable[tuple[str]],
        target_fields: Iterable[str] | None = None,
        commit_every: int = 1000,
        replace: bool = False,
        **kwargs: Any,
    ) -> NotImplementedError:
        raise NotImplementedError("There is no INSERT statement in Drill.")
