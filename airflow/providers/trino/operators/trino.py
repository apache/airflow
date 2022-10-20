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
"""This module contains the Trino operator."""
from __future__ import annotations

import warnings
from typing import Any, Sequence

from trino.exceptions import TrinoQueryError

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.trino.hooks.trino import TrinoHook


class TrinoOperator(SQLExecuteQueryOperator):
    """
    Executes sql code using a specific Trino query Engine.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TrinoOperator`

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
    :param trino_conn_id: id of the connection config for the target Trino
        environment
    :param autocommit: What to set the connection's autocommit setting to
        before executing the query
    :param handler: The result handler which is called with the result of each statement.
    :param parameters: (optional) the parameters to render the SQL query with.
    """

    template_fields: Sequence[str] = ('sql',)
    template_fields_renderers = {'sql': 'sql'}
    template_ext: Sequence[str] = ('.sql',)
    ui_color = '#ededed'

    def __init__(self, *, trino_conn_id: str = "trino_default", **kwargs: Any) -> None:
        super().__init__(conn_id=trino_conn_id, **kwargs)
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`.""",
            DeprecationWarning,
            stacklevel=2,
        )

    def on_kill(self) -> None:
        if self._hook is not None and isinstance(self._hook, TrinoHook):
            query_id = "'" + self._hook.query_id + "'"
            try:
                self.log.info("Stopping query run with queryId - %s", self._hook.query_id)
                self._hook.run(
                    sql=f"CALL system.runtime.kill_query(query_id => {query_id},message => 'Job "
                    f"killed by "
                    f"user');",
                    handler=list,
                )
            except TrinoQueryError as e:
                self.log.info(str(e))
            self.log.info("Trino query (%s) terminated", query_id)
