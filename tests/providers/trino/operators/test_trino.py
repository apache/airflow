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

import unittest
from unittest import mock

from airflow.providers.trino.operators.trino import TrinoOperator

TRINO_CONN_ID = "test_trino"
TASK_ID = "test_trino_task"


class TestTrinoOperator(unittest.TestCase):
    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_execute(self, mock_get_db_hook):
        """Asserts that the run method is called when a TrinoOperator task is executed"""

        op = TrinoOperator(
            task_id=TASK_ID,
            sql="SELECT 1;",
            trino_conn_id=TRINO_CONN_ID,
            handler=list,
        )
        op.execute(None)

        mock_get_db_hook.return_value.run.assert_called_once_with(
            sql="SELECT 1;",
            autocommit=False,
            handler=list,
            parameters=None,
            return_last=True,
            split_statements=False,
        )
