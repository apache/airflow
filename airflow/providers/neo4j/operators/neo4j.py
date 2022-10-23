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

from typing import TYPE_CHECKING, Iterable, Mapping, Sequence

from airflow.models import BaseOperator
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class Neo4jOperator(BaseOperator):
    """
    Executes sql code in a specific Neo4j database

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:Neo4jOperator`

    :param sql: the sql code to be executed. Can receive a str representing a
        sql statement
    :param neo4j_conn_id: Reference to :ref:`Neo4j connection id <howto/connection:neo4j>`.
    """

    template_fields: Sequence[str] = ("sql",)

    def __init__(
        self,
        *,
        sql: str,
        neo4j_conn_id: str = "neo4j_default",
        parameters: Iterable | Mapping | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.neo4j_conn_id = neo4j_conn_id
        self.sql = sql
        self.parameters = parameters

    def execute(self, context: Context) -> None:
        self.log.info("Executing: %s", self.sql)
        hook = Neo4jHook(conn_id=self.neo4j_conn_id)
        hook.run(self.sql)
