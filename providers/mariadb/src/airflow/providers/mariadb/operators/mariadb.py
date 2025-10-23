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
MariaDB operators for Apache Airflow.

This module provides operators for executing SQL queries against MariaDB databases.
"""

from __future__ import annotations

from typing import Sequence

from deprecated import deprecated

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@deprecated(
    reason=(
        "Please use `airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`."
        "Also, you can provide `hook_params={'schema': <database>}`."
    ),
    category=AirflowProviderDeprecationWarning,
)
class MariaDBOperator(SQLExecuteQueryOperator):
    """
    Execute SQL code in a MariaDB database.

    This operator is deprecated. Please use
    :class:`airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`
    with the MariaDB hook instead.

    :param mariadb_conn_id: The connection ID to use when connecting to MariaDB.
    :param database: The database name to connect to.
    :param sql: The SQL code to be executed.
    :param parameters: The parameters to render the SQL query with.
    """

    template_fields: Sequence[str] = ("sql", "parameters")
    template_fields_renderers = {
        "sql": "mysql",
        "parameters": "json",
    }
    template_ext: Sequence[str] = (".sql", ".json")
    ui_color = "#ededed"

    def __init__(
        self,
        *,
        mariadb_conn_id: str = "mariadb_default",
        database: str | None = None,
        **kwargs,
    ) -> None:
        if database is not None:
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {"schema": database, **hook_params}

        super().__init__(conn_id=mariadb_conn_id, **kwargs)
