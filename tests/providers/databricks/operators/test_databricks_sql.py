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
#
import os
import tempfile
import unittest
from unittest import mock

from databricks.sql.types import Row

from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

DATE = '2017-04-20'
TASK_ID = 'databricks-sql-operator'
DEFAULT_CONN_ID = 'databricks_default'


class TestDatabricksSqlOperator(unittest.TestCase):
    @mock.patch('airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook')
    def test_exec_success(self, db_mock_class):
        """
        Test the execute function in case where SQL query was successful.
        """
        sql = "select * from dummy"
        op = DatabricksSqlOperator(task_id=TASK_ID, sql=sql, do_xcom_push=True)
        db_mock = db_mock_class.return_value
        mock_schema = [('id',), ('value',)]
        mock_results = [Row(id=1, value='value1')]
        db_mock.run.return_value = (mock_schema, mock_results)

        results = op.execute(None)

        assert results == mock_results
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID, http_path=None, session_configuration=None, sql_endpoint_name=None
        )
        db_mock.run.assert_called_once_with(sql, parameters=None)

    @mock.patch('airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook')
    def test_exec_write_file(self, db_mock_class):
        """
        Test the execute function in case where SQL query was successful and data is written as CSV
        """
        sql = "select * from dummy"
        tempfile_path = tempfile.mkstemp()[1]
        op = DatabricksSqlOperator(task_id=TASK_ID, sql=sql, output_path=tempfile_path)
        db_mock = db_mock_class.return_value
        mock_schema = [('id',), ('value',)]
        mock_results = [Row(id=1, value='value1')]
        db_mock.run.return_value = (mock_schema, mock_results)

        try:
            op.execute(None)
            results = [line.strip() for line in open(tempfile_path)]
        finally:
            os.remove(tempfile_path)

        assert results == ["id,value", "1,value1"]
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID, http_path=None, session_configuration=None, sql_endpoint_name=None
        )
        db_mock.run.assert_called_once_with(sql, parameters=None)
