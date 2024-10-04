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

from typing import Iterable, Mapping

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


class YDBExecuteQueryOperator(SQLExecuteQueryOperator):
    """
    Executes sql code in a specific YDB database.

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param ydb_conn_id: The :ref:`ydb conn id <howto/connection:ydb>`
        reference to a specific YDB cluster and database.
    :param parameters: (optional) the parameters to render the SQL query with.
    """

    ui_color = "#ededed"

    def __init__(
        self,
        sql: str | list[str],
        is_ddl: bool = False,
        ydb_conn_id: str = "ydb_default",
        parameters: Mapping | Iterable | None = None,
        **kwargs,
    ) -> None:
        if parameters is not None:
            raise AirflowException("parameters are not supported yet")

        if is_ddl:
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {"is_ddl": is_ddl, **hook_params}

        super().__init__(conn_id=ydb_conn_id, sql=sql, parameters=parameters, **kwargs)


class YDBScanQueryOperator(SQLExecuteQueryOperator):
    """
    Executes scan query in a specific YDB database.

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param ydb_conn_id: The :ref:`ydb conn id <howto/connection:ydb>`
        reference to a specific YDB cluster and database.
    :param parameters: (optional) the parameters to render the SQL query with.
    """

    ui_color = "#ededed"

    def __init__(
        self,
        sql: str | list[str],
        ydb_conn_id: str = "ydb_default",
        parameters: Mapping | Iterable | None = None,
        **kwargs,
    ) -> None:
        if parameters is not None:
            raise AirflowException("parameters are not supported yet")

        hook_params = kwargs.pop("hook_params", {})
        kwargs["hook_params"] = {"use_scan_query": True, **hook_params}

        super().__init__(conn_id=ydb_conn_id, sql=sql, parameters=parameters, **kwargs)
