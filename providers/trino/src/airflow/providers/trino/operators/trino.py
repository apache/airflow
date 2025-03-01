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
"""This module contains the Trino operator."""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, List

from airflow.providers.common.sql.hooks.sql import return_single_query_results
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.trino.hooks.trino import TrinoHook


class TrinoOperator(SQLExecuteQueryOperator):
    """
    General Trino Operator to execute queries using Trino query engine.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TrinoOperator`

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
    :param trino_conn_id: id of the connection config for the target Trino environment
    :param autocommit: What to set the connection's autocommit setting to before executing the query
    :param handler: (optional) the function that will be applied to the cursor (default: fetch_all_handler).
    :param parameters: (optional) the parameters to render the SQL query with.
    :param output_processor: (optional) the function that will be applied to the result
        (default: default_output_processor).
    :param split_statements: (optional) if split single SQL string into statements. (default: True).
    :param show_return_value_in_logs: (optional) if true operator output will be printed to the task log.
        Use with caution. It's not recommended to dump large datasets to the log. (default: False).

    """

    template_fields: Sequence[str] = tuple({"trino_conn_id"} | set(SQLExecuteQueryOperator.template_fields))
    template_fields_renderers: ClassVar[dict] = {"sql": "sql"}
    conn_id_field = "trino_conn_id"

    def __init__(
        self,
        *,
        sql: str | List[str],
        trino_conn_id: str = TrinoHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(sql=sql, conn_id=trino_conn_id, **kwargs)
        self.trino_conn_id = trino_conn_id

    def get_db_hook(self) -> TrinoHook:
        return TrinoHook(self.trino_conn_id)

    def execute(self, context):
        self.log.info("Executing: %s", self.sql)
        hook = self.get_db_hook()

        if self.split_statements is not None:
            extra_kwargs = {"split_statements": self.split_statements}
        else:
            extra_kwargs = {}

        output = hook.run(
            sql=self.sql,
            autocommit=self.autocommit,
            parameters=self.parameters,
            handler=self.handler,
            return_last=self.return_last,
            **extra_kwargs,
        )
        if return_single_query_results(self.sql, self.return_last, self.split_statements):
            # For simplicity, we pass always list as input to _process_output, regardless if
            # single query results are going to be returned, and we return the first element
            # of the list in this case from the (always) list returned by _process_output
            return self._process_output([output], hook.descriptions)[-1]
        result = self._process_output(output, hook.descriptions)
        self.log.info("result: %s", result)
        return result
