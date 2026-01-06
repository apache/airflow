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

from typing import TYPE_CHECKING

from impala.dbapi import connect

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from impala.interface import Connection
    from sqlalchemy.engine import URL


class ImpalaHook(DbApiHook):
    """Interact with Apache Impala through impyla."""

    conn_name_attr = "impala_conn_id"
    default_conn_name = "impala_default"
    conn_type = "impala"
    hook_name = "Impala"

    def get_conn(self) -> Connection:
        conn_id: str = self.get_conn_id()
        connection = self.get_connection(conn_id)
        return connect(
            host=connection.host,
            port=connection.port,
            user=connection.login,
            password=connection.password,
            database=connection.schema,
            **connection.extra_dejson,
        )

    @property
    def sqlalchemy_url(self) -> URL:
        """Return a `sqlalchemy.engine.URL` object constructed from the connection."""
        try:
            from sqlalchemy.engine import URL
        except (ImportError, ModuleNotFoundError) as err:
            raise AirflowOptionalProviderFeatureException(
                "The 'sqlalchemy' library is required to use this feature. "
                "Please install it with: pip install 'apache-airflow-providers-apache-impala[sqlalchemy]'"
            ) from err

        conn = self.get_connection(self.get_conn_id())
        extra = conn.extra_dejson or {}

        required_attrs = ["host", "login"]
        for attr in required_attrs:
            if getattr(conn, attr) is None:
                raise ValueError(f"Impala Connection Error: '{attr}' is missing in the connection")

        query = {k: str(v) for k, v in extra.items() if v is not None and k != "__extra__"}

        return URL.create(
            drivername="impala",
            username=conn.login,
            password=conn.password or "",
            host=str(conn.host),
            port=conn.port or 21050,
            database=conn.schema,
            query=query,
        )

    def get_uri(self) -> str:
        """Return a SQLAlchemy engine URL as a string."""
        return self.sqlalchemy_url.render_as_string(hide_password=False)
