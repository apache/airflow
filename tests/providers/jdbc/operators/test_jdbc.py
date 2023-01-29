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

from unittest.mock import patch

from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.jdbc.operators.jdbc import JdbcOperator


class TestJdbcOperator:
    def setup_method(self):
        self.kwargs = dict(sql="sql", task_id="test_jdbc_operator", dag=None)

    @patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_execute_do_push(self, mock_get_db_hook):
        jdbc_operator = JdbcOperator(**self.kwargs, do_xcom_push=True)
        jdbc_operator.execute(context={})

        mock_get_db_hook.return_value.run.assert_called_once_with(
            sql=jdbc_operator.sql,
            autocommit=jdbc_operator.autocommit,
            handler=fetch_all_handler,
            parameters=jdbc_operator.parameters,
            return_last=True,
        )

    @patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_execute_dont_push(self, mock_get_db_hook):
        jdbc_operator = JdbcOperator(**self.kwargs, do_xcom_push=False)
        jdbc_operator.execute(context={})

        mock_get_db_hook.return_value.run.assert_called_once_with(
            sql=jdbc_operator.sql,
            autocommit=jdbc_operator.autocommit,
            parameters=jdbc_operator.parameters,
            handler=None,
            return_last=True,
        )
