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

from typing import TYPE_CHECKING, Callable, Sequence

from airflow.models import BaseOperator
from airflow.providers.arangodb.hooks.arangodb import ArangoDBHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AQLOperator(BaseOperator):
    """
    Executes AQL query in a ArangoDB database

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AQLOperator`

    :param query: the AQL query to be executed. Can receive a str representing a
        AQL statement, or you can provide .sql file having the query
    :param result_processor: function to further process the Result from ArangoDB
    :param arangodb_conn_id: Reference to :ref:`ArangoDB connection id <howto/connection:arangodb>`.
    """

    template_fields: Sequence[str] = ("query",)

    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"query": "sql"}

    def __init__(
        self,
        *,
        query: str,
        arangodb_conn_id: str = "arangodb_default",
        result_processor: Callable | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.arangodb_conn_id = arangodb_conn_id
        self.query = query
        self.result_processor = result_processor

    def execute(self, context: Context):
        self.log.info("Executing: %s", self.query)
        hook = ArangoDBHook(arangodb_conn_id=self.arangodb_conn_id)
        result = hook.query(self.query)
        if self.result_processor:
            self.result_processor(result)
