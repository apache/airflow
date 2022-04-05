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
from typing import TYPE_CHECKING, Callable, Optional, Sequence, Any, Dict

from airflow.models import BaseOperator
from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ClickHouseOperator(BaseOperator):
    """
    Executes SQL query in a ClickHouse database

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ClickHouseOperator`

    :param query: the SQL query to be executed. Can receive a str representing a
        SQL statement, or you can provide .sql file having the query
    :param params: substitution parameters for SELECT queries and data for INSERT queries.
    :param database:Optional[str], database to query, if not provided schema from Connection will be used

    :param result_processor: function to further process the Result from ClickHouse
    :param clickhouse_conn_id: Reference to :ref:`ClickHouse connection id <howto/connection:clickhouse>`.
    """

    template_fields: Sequence[str] = ('query',)

    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"query": "sql"}

    def __init__(
        self,
        *,
        query: str,
        clickhouse_conn_id: str = 'clickhouse_default',
        params: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
        result_processor: Optional[Callable] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.clickhouse_conn_id = clickhouse_conn_id
        self.query = query
        self.params = params
        self.database = database
        self.result_processor = result_processor

    def execute(self, context: 'Context'):
        hook = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id, database=self.database)

        result = hook.query(query=self.query, params=self.params)
        if self.result_processor:
            self.result_processor(result)
