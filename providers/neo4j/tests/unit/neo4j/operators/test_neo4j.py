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

from unittest import mock

from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "unit_test_dag"


class TestNeo4jOperator:
    @mock.patch("airflow.providers.neo4j.operators.neo4j.Neo4jHook")
    def test_neo4j_operator_test(self, mock_hook):
        sql = """
            MATCH (tom {name: "Tom Hanks"}) RETURN tom
            """
        op = Neo4jOperator(task_id="basic_neo4j", sql=sql)
        op.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(conn_id="neo4j_default")
        mock_hook.return_value.run.assert_called_once_with(sql)
