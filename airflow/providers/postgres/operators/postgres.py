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
from typing import Mapping, Sequence

from psycopg2.sql import SQL, Identifier

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


class PostgresOperator(SQLExecuteQueryOperator):
    """
    Executes sql code in a specific Postgres database

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param postgres_conn_id: The :ref:`postgres conn id <howto/connection:postgres>`
        reference to a specific postgres database.
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param database: name of database which overwrite defined one in connection
    """

    template_fields: Sequence[str] = ("sql",)
    template_fields_renderers = {"sql": "postgresql"}
    template_ext: Sequence[str] = (".sql",)
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
            kwargs["hook_params"] = {"schema": database, **hook_params}

        if runtime_parameters:
            sql = kwargs.pop("sql")
            parameters = kwargs.pop("parameters", {})

            final_sql = []
            sql_param = {}
            for param in runtime_parameters:
                set_param_sql = f"SET {{}} TO %({param})s;"
                dynamic_sql = SQL(set_param_sql).format(Identifier(f"{param}"))
                final_sql.append(dynamic_sql)
            for param, val in runtime_parameters.items():
                sql_param.update({f"{param}": f"{val}"})
            if parameters:
                sql_param.update(parameters)
            if isinstance(sql, str):
                final_sql.append(SQL(sql))
            else:
                final_sql.extend(list(map(SQL, sql)))

            kwargs["sql"] = final_sql
            kwargs["parameters"] = sql_param

        super().__init__(conn_id=postgres_conn_id, **kwargs)
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`.
            Also, you can provide `hook_params={'schema': <database>}`.""",
            DeprecationWarning,
            stacklevel=2,
        )
