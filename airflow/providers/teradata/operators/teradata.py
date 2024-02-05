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

from typing import Sequence

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.teradata.hooks.teradata import TeradataHook


class TeradataOperator(SQLExecuteQueryOperator):
    """
    General Teradata Operator to execute queries on Teradata Database.

    Executes sql statements in the Teradata SQL Database using teradatasql jdbc driver

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TeradataOperator`

    :param sql: the SQL query to be executed as a single string, or a list of str (sql statements)
    :param conn_id: reference to a predefined database
    :param autocommit: if True, each command is automatically committed.(default value: False)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param schema: The Teradata database to connect to.
    """

    template_fields: Sequence[str] = (
        "parameters",
        "sql",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#e07c24"

    def __init__(
        self,
        conn_id: str = TeradataHook.default_conn_name,
        schema: str | None = None,
        **kwargs,
    ) -> None:
        if schema:
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {
                "database": schema,
                **hook_params,
            }
        super().__init__(**kwargs)
        self.conn_id = conn_id
