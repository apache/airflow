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

from airflow.providers.arangodb.hooks.arangodb import ArangoDBHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AQLSensor(BaseSensorOperator):
    """
    Checks for the existence of a document which
    matches the given query in ArangoDB. Example:

    :param collection: Target DB collection.
    :param query: The query to poke, or you can provide .sql file having the query
    :param arangodb_conn_id: The :ref:`ArangoDB connection id <howto/connection:arangodb>` to use
        when connecting to ArangoDB.
    :param arangodb_db: Target ArangoDB name.
    """

    template_fields: Sequence[str] = ("query",)

    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"query": "sql"}

    def __init__(self, *, query: str, arangodb_conn_id: str = "arangodb_default", **kwargs) -> None:
        super().__init__(**kwargs)
        self.arangodb_conn_id = arangodb_conn_id
        self.query = query

    def poke(self, context: Context) -> bool:
        self.log.info("Sensor running the following query: %s", self.query)
        hook = ArangoDBHook(self.arangodb_conn_id)
        records = hook.query(self.query, count=True).count()
        self.log.info("Total records found: %d", records)
        return 0 != records
