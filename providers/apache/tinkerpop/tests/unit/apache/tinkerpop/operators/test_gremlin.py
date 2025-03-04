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

from airflow.models import Connection
from airflow.providers.apache.tinkerpop.operators.gremlin import GremlinOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
TEST_DAG_ID = "unit_test_dag"


class TestGremlinOperator:
    @mock.patch("airflow.providers.apache.tinkerpop.operators.gremlin.GremlinHook")
    def test_gremlin_operator(self, mock_hook):
        """
        Test that the GremlinOperator instantiates the hook with the proper connection id
        and calls its run() method with the provided query.
        """
        query = "g.V().limit(1)"
        op = GremlinOperator(task_id="basic_gremlin", query=query, gremlin_conn_id="gremlin_default")

        # Create a dummy context (the operator doesn't use it in execute())
        context = mock.MagicMock()

        # Create a dummy connection to avoid the AirflowNotFoundException.
        dummy_conn = Connection(
            conn_id="gremlin_default",
            host="host",
            port=443,
            schema="mydb",
            login="mylogin",
            password="mypassword",
        )

        # Override get_connection on the hook instance so that it returns the dummy connection.
        mock_hook.return_value.get_connection.return_value = dummy_conn

        # Execute the operator.
        op.execute(context)

        # Ensure that the hook was instantiated with the expected connection ID.
        mock_hook.assert_called_once_with(conn_id="gremlin_default")
        # Ensure that run() was called with the query.
        mock_hook.return_value.run.assert_called_once_with(query)
