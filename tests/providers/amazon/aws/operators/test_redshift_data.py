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

from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

CONN_ID = "aws_conn_test"
TASK_ID = "task_id"
SQL = "sql"
DATABASE = "database"
STATEMENT_ID = "statement_id"


class TestRedshiftDataOperator:
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    def test_execute(self, mock_exec_query):
        cluster_identifier = "cluster_identifier"
        db_user = "db_user"
        secret_arn = "secret_arn"
        statement_name = "statement_name"
        parameters = [{"name": "id", "value": "1"}]
        poll_interval = 5
        await_result = True

        operator = RedshiftDataOperator(
            aws_conn_id=CONN_ID,
            task_id=TASK_ID,
            sql=SQL,
            database=DATABASE,
            cluster_identifier=cluster_identifier,
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
            parameters=parameters,
            await_result=True,
            poll_interval=poll_interval,
        )
        operator.execute(None)
        mock_exec_query.assert_called_once_with(
            sql=SQL,
            database=DATABASE,
            cluster_identifier=cluster_identifier,
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
            parameters=parameters,
            with_event=False,
            wait_for_completion=await_result,
            poll_interval=poll_interval,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_on_kill_without_query(self, mock_conn):
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        operator = RedshiftDataOperator(
            aws_conn_id=CONN_ID,
            task_id=TASK_ID,
            sql=SQL,
            database=DATABASE,
            await_result=False,
        )
        operator.on_kill()
        mock_conn.cancel_statement.assert_not_called()

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
    def test_on_kill_with_query(self, mock_conn):
        mock_conn.execute_statement.return_value = {"Id": STATEMENT_ID}
        operator = RedshiftDataOperator(
            aws_conn_id=CONN_ID,
            task_id=TASK_ID,
            sql=SQL,
            database=DATABASE,
            await_result=False,
        )
        operator.execute(None)
        operator.on_kill()
        mock_conn.cancel_statement.assert_called_once_with(
            Id=STATEMENT_ID,
        )
