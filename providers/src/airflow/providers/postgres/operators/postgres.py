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

import warnings
from typing import ClassVar, Mapping

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
class PostgresOperator(SQLExecuteQueryOperator):
    """
    Executes sql code in a specific Postgres database.

    This class is deprecated.

    Please use :class:`airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`.

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param postgres_conn_id: The :ref:`postgres conn id <howto/connection:postgres>`
        reference to a specific postgres database.
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param database: name of database which overwrite defined one in connection
    :param runtime_parameters: a mapping of runtime params added to the final sql being executed.
        For example, you could set the schema via `{"search_path": "CUSTOM_SCHEMA"}`.
        Deprecated - use `hook_params={'options': '-c <connection_options>'}` instead.
    """

    template_fields_renderers: ClassVar[dict] = {
        **SQLExecuteQueryOperator.template_fields_renderers,
        "sql": "postgresql",
    }
    ui_color = "#ededed"

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_default",
        database: str | None = None,
        runtime_parameters: Mapping | None = None,
        **kwargs,
    ) -> None:
        if database is not None:
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {"database": database, **hook_params}

        if runtime_parameters:
            warnings.warn(
                """`runtime_parameters` is deprecated.
                Please use `hook_params={'options': '-c <connection_options>}`.""",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            hook_params = kwargs.pop("hook_params", {})
            options = " ".join(f"-c {param}={val}" for param, val in runtime_parameters.items())
            kwargs["hook_params"] = {"options": options, **hook_params}

        super().__init__(conn_id=postgres_conn_id, **kwargs)
