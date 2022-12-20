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

from vertica_python import connect

from airflow.providers.common.sql.hooks.sql import DbApiHook


class VerticaHook(DbApiHook):
    """Interact with Vertica."""

    conn_name_attr = "vertica_conn_id"
    default_conn_name = "vertica_default"
    conn_type = "vertica"
    hook_name = "Vertica"
    supports_autocommit = True

    def get_conn(self) -> connect:
        """Return verticaql connection object"""
        conn = self.get_connection(self.vertica_conn_id)  # type: ignore
        conn_config = {
            "user": conn.login,
            "password": conn.password or "",
            "database": conn.schema,
            "host": conn.host or "localhost",
        }

        if not conn.port:
            conn_config["port"] = 5433
        else:
            conn_config["port"] = int(conn.port)

        conn = connect(**conn_config)
        return conn
