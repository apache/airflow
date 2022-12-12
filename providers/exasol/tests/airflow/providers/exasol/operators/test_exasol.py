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

from unittest import mock

from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.exasol.operators.exasol import ExasolOperator


class TestExasol:
    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_overwrite_autocommit(self, mock_get_db_hook):
        operator = ExasolOperator(task_id="TEST", sql="SELECT 1", autocommit=True)
        operator.execute({})
        mock_get_db_hook.return_value.run.assert_called_once_with(
            sql="SELECT 1",
            autocommit=True,
            parameters=None,
            handler=fetch_all_handler,
            return_last=True,
            split_statements=False,
        )

    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_pass_parameters(self, mock_get_db_hook):
        operator = ExasolOperator(task_id="TEST", sql="SELECT {value!s}", parameters={"value": 1})
        operator.execute({})
        mock_get_db_hook.return_value.run.assert_called_once_with(
            sql="SELECT {value!s}",
            autocommit=False,
            parameters={"value": 1},
            handler=fetch_all_handler,
            return_last=True,
            split_statements=False,
        )

    @mock.patch("airflow.providers.common.sql.operators.sql.BaseSQLOperator.__init__")
    def test_overwrite_schema(self, mock_base_op):
        ExasolOperator(task_id="TEST", sql="SELECT 1", schema="dummy")
        mock_base_op.assert_called_once_with(
            conn_id="exasol_default",
            hook_params={"schema": "dummy"},
            default_args={},
            task_id="TEST",
        )
