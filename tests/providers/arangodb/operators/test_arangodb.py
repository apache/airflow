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

import unittest
from unittest import mock

from airflow.providers.arangodb.operators.arangodb import AQLOperator


class TestAQLOperator(unittest.TestCase):
    @mock.patch('airflow.providers.arangodb.operators.arangodb.ArangoDBHook')
    def test_arangodb_operator_test(self, mock_hook):

        arangodb_query = "FOR doc IN students RETURN doc"
        op = AQLOperator(task_id='basic_aql_task', query=arangodb_query)
        op.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(arangodb_conn_id='arangodb_default')
        mock_hook.return_value.query.assert_called_once_with(arangodb_query)
