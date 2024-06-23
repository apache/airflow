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

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.teradata.hooks.teradata import TeradataHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class TeradataOperator(SQLExecuteQueryOperator):
    """
    General Teradata Operator to execute queries on Teradata Database.

    Executes sql statements in the Teradata SQL Database using Teradata Python SQL Driver

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TeradataOperator`

    :param sql: the SQL query to be executed as a single string, or a list of str (sql statements)
    :param teradata_conn_id: reference to a predefined database
    :param autocommit: if True, each command is automatically committed.(default value: False)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param schema: The Teradata database to connect to.
    """

    template_fields: Sequence[str] = (
        "sql",
        "parameters",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#e07c24"

    def __init__(
        self,
        teradata_conn_id: str = TeradataHook.default_conn_name,
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
        self.conn_id = teradata_conn_id


class TeradataStoredProcedureOperator(BaseOperator):
    """
    Executes stored procedure in a specific Teradata database.

    :param procedure: name of stored procedure to call (templated)
    :param teradata_conn_id: The :ref:`Teradata connection id <howto/connection:teradata>`
        reference to a specific Teradata database.
    :param parameters: (optional, templated) the parameters provided in the call

    """

    template_fields: Sequence[str] = (
        "procedure",
        "parameters",
    )
    ui_color = "#ededed"

    def __init__(
        self,
        *,
        procedure: str,
        teradata_conn_id: str = TeradataHook.default_conn_name,
        parameters: dict | list | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.teradata_conn_id = teradata_conn_id
        self.procedure = procedure
        self.parameters = parameters

    def execute(self, context: Context):
        hook = TeradataHook(teradata_conn_id=self.teradata_conn_id)
        return hook.callproc(self.procedure, autocommit=True, parameters=self.parameters)
