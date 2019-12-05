# -*- coding: utf-8 -*-
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
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class MongoSensor(BaseSensorOperator):
    """
    Checks for the existence of a document which
    matches the given query in MongoDB. Example:

    >>> mongo_sensor = MongoSensor(collection="coll",
    ...                            query={"key": "value"},
    ...                            mongo_conn_id="mongo_default",
    ...                            task_id="mongo_sensor")
    """
    template_fields = ('collection', 'query')

    @apply_defaults
    def __init__(self, collection, query, mongo_conn_id="mongo_default", *args, **kwargs):
        """
        Create a new MongoSensor

        :param collection: Target MongoDB collection.
        :type collection: str
        :param query: The query to find the target document.
        :type query: dict
        :param mongo_conn_id: The connection ID to use
                              when connecting to MongoDB.
        :type mongo_conn_id: str
        """
        super(MongoSensor, self).__init__(*args, **kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.collection = collection
        self.query = query

    def poke(self, context):
        self.log.info("Sensor check existence of the document "
                      "that matches the following query: %s", self.query)
        hook = MongoHook(self.mongo_conn_id)
        return hook.find(self.collection, self.query, find_one=True) is not None
