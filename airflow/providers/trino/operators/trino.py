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

from typing import TYPE_CHECKING, Any, Callable, List, Optional, Sequence, Union

from trino.exceptions import TrinoQueryError

from airflow.models import BaseOperator
from airflow.providers.trino.hooks.trino import TrinoHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class TrinoOperator(BaseOperator):
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

    def __init__(
        self,
        *,
        sql: Union[str, List[str]],
        trino_conn_id: str = "trino_default",
        autocommit: bool = False,
        parameters: Optional[tuple] = None,
        handler: Optional[Callable] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.trino_conn_id = trino_conn_id
        self.hook: Optional[TrinoHook] = None
        self.autocommit = autocommit
        self.parameters = parameters
        self.handler = handler

    def get_hook(self) -> TrinoHook:
        """Get Trino hook"""
        return TrinoHook(
            trino_conn_id=self.trino_conn_id,
        )

    def execute(self, context: 'Context') -> None:
        """Execute Trino SQL"""
        self.hook = self.get_hook()
        self.hook.run(
            sql=self.sql, autocommit=self.autocommit, parameters=self.parameters, handler=self.handler
        )

    def on_kill(self) -> None:
        if self.hook is not None and isinstance(self.hook, TrinoHook):
            query_id = "'" + self.hook.query_id + "'"
            try:
                self.log.info("Stopping query run with queryId - %s", self.hook.query_id)
                self.hook.run(
                    sql=f"CALL system.runtime.kill_query(query_id => {query_id},message => 'Job "
                    f"killed by "
                    f"user');",
                    handler=list,
                )
            except TrinoQueryError as e:
                self.log.info(str(e))
            self.log.info("Trino query (%s) terminated", query_id)
