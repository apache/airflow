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

import unittest
from unittest.mock import patch

from airflow.providers.jdbc.operators.jdbc import JdbcOperator


class TestJdbcOperator(unittest.TestCase):
    def setUp(self):
        self.kwargs = dict(sql='sql', task_id='test_jdbc_operator', dag=None)

    @patch('airflow.providers.jdbc.operators.jdbc.JdbcHook')
    def test_execute(self, mock_jdbc_hook):
        jdbc_operator = JdbcOperator(**self.kwargs)
        jdbc_operator.execute(context={})

        mock_jdbc_hook.assert_called_once_with(jdbc_conn_id=jdbc_operator.jdbc_conn_id)
        mock_jdbc_hook.return_value.run.assert_called_once_with(
            jdbc_operator.sql, jdbc_operator.autocommit, parameters=jdbc_operator.parameters
        )
