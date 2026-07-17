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
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook

if TYPE_CHECKING:
    from airflow.sdk import Context


class Neo4jOperator(BaseOperator):
    """
    Executes a Cypher query in a specific Neo4j database.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:Neo4jOperator`

    :param cypher: the Cypher query to be executed. Can receive a str representing a
        Cypher statement.
    :param sql: (Deprecated) the Cypher query to be executed. Use ``cypher`` instead.
    :param neo4j_conn_id: Reference to :ref:`Neo4j connection id <howto/connection:neo4j>`.
    :param parameters: the parameters to send to Neo4j driver session
    """

    template_fields: Sequence[str] = ("cypher", "parameters")
    template_fields_renderers = {"cypher": "sql", "parameters": "json"}

    def __init__(
        self,
        *,
        cypher: str | None = None,
        sql: str | None = None,
        neo4j_conn_id: str = "neo4j_default",
        parameters: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if sql is not None:
            warnings.warn(
                "`sql` parameter is deprecated, please use `cypher` instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            if cypher is not None:
                raise ValueError("Cannot provide both `sql` and `cypher`. Use `cypher` only.")
            cypher = sql
        if cypher is None:
            raise ValueError("Parameter `cypher` is required.")
        self.neo4j_conn_id = neo4j_conn_id
        self.cypher = cypher
        self.parameters = parameters

    def execute(self, context: Context) -> None:
        self.log.info("Executing: %s", self.cypher)
        hook = Neo4jHook(conn_id=self.neo4j_conn_id)
        hook.run(self.cypher, self.parameters)
