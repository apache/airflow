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

from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class MongoSensor(BaseSensorOperator):
    """
    Checks for the existence of a document which
    matches the given query in MongoDB. Example:

    >>> mongo_sensor = MongoSensor(collection="coll",
    ...                            query={"key": "value"},
    ...                            mongo_conn_id="mongo_default",
    ...                            mongo_db="admin",
    ...                            task_id="mongo_sensor")

    :param collection: Target MongoDB collection.
    :param query: The query to find the target document.
    :param mongo_conn_id: The :ref:`Mongo connection id <howto/connection:mongo>` to use
        when connecting to MongoDB.
    :param mongo_db: Target MongoDB name.
    """

    template_fields: Sequence[str] = ("collection", "query")

    def __init__(
        self, *, collection: str, query: dict, mongo_conn_id: str = "mongo_default", mongo_db=None, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.collection = collection
        self.query = query
        self.mongo_db = mongo_db

    def poke(self, context: Context) -> bool:
        self.log.info(
            "Sensor check existence of the document that matches the following query: %s", self.query
        )
        hook = MongoHook(self.mongo_conn_id)
        return hook.find(self.collection, self.query, mongo_db=self.mongo_db, find_one=True) is not None
