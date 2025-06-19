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

import os

import pytest
from gremlin_python.driver.serializer import GraphSONSerializersV2d0

from airflow.providers.apache.tinkerpop.hooks.gremlin import GremlinHook

AIRFLOW_CONN_GREMLIN_DEFAULT = "ws://mylogin:mysecret@gremlin:8182/gremlin"


@pytest.mark.integration("gremlin")
class TestGremlinHook:
    def setup_method(self):
        os.environ["AIRFLOW_CONN_GREMLIN_DEFAULT"] = AIRFLOW_CONN_GREMLIN_DEFAULT
        self.hook = GremlinHook()
        add_query = "g.addV('person').property('id', 'person1').property('name', 'Alice')"
        self.hook.run(add_query)

    def teardown_method(self):
        self.hook.run("g.V().drop().iterate()")

    def test_another_query(self):
        result = self.hook.run("g.V().hasLabel('person').count()")
        assert isinstance(result, list)

    def test_run(self):
        result = self.hook.run(
            "g.V().hasLabel('person').valueMap(true)", serializer=GraphSONSerializersV2d0()
        )
        expected = "[{'id': ['person1'], 'label': 'person', 'name': ['Alice']}]"
        assert str(result) == expected
