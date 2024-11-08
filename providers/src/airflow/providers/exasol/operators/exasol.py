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

from typing import ClassVar, Sequence

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.exasol.hooks.exasol import exasol_fetch_all_handler


class ExasolOperator(SQLExecuteQueryOperator):
    """
    Executes sql code in a specific Exasol database.

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        template references are recognized by str ending in '.sql'
    :param exasol_conn_id: reference to a specific Exasol database
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param schema: (optional) name of the schema which overwrite defined one in connection
    :param handler: (optional) handler to process the results of the query
    """

    template_fields: Sequence[str] = ("sql", "exasol_conn_id")
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers: ClassVar[dict] = {"sql": "sql"}
    ui_color = "#ededed"
    conn_id_field = "exasol_conn_id"

    def __init__(
        self,
        *,
        exasol_conn_id: str = "exasol_default",
        schema: str | None = None,
        handler=exasol_fetch_all_handler,
        **kwargs,
    ) -> None:
        self.exasol_conn_id = exasol_conn_id
        if schema is not None:
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {"schema": schema, **hook_params}
        super().__init__(conn_id=exasol_conn_id, handler=handler, **kwargs)
