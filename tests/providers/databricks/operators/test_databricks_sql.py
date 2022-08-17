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

import pytest
from databricks.sql.types import Row

from airflow import AirflowException
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.databricks.operators.databricks_sql import (
    DatabricksCopyIntoOperator,
    DatabricksSqlOperator,
)

DATE = '2017-04-20'
TASK_ID = 'databricks-sql-operator'
DEFAULT_CONN_ID = 'databricks_default'
COPY_FILE_LOCATION = 's3://my-bucket/jsonData'


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
        db_mock.run.return_value = [(mock_schema, mock_results)]

        results = op.execute(None)

        assert results == mock_results
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            http_path=None,
            session_configuration=None,
            sql_endpoint_name=None,
            http_headers=None,
            catalog=None,
            schema=None,
            caller='DatabricksSqlOperator',
        )
        db_mock.run.assert_called_once_with(sql, parameters=None, handler=fetch_all_handler)

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
        db_mock.run.return_value = [(mock_schema, mock_results)]

        try:
            op.execute(None)
            results = [line.strip() for line in open(tempfile_path)]
        finally:
            os.remove(tempfile_path)

        assert results == ["id,value", "1,value1"]
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            http_path=None,
            session_configuration=None,
            sql_endpoint_name=None,
            http_headers=None,
            catalog=None,
            schema=None,
            caller='DatabricksSqlOperator',
        )
        db_mock.run.assert_called_once_with(sql, parameters=None, handler=fetch_all_handler)


class TestDatabricksSqlCopyIntoOperator(unittest.TestCase):
    def test_copy_with_files(self):
        op = DatabricksCopyIntoOperator(
            file_location=COPY_FILE_LOCATION,
            file_format='JSON',
            table_name='test',
            files=['file1', 'file2', 'file3'],
            format_options={'dateFormat': 'yyyy-MM-dd'},
            task_id=TASK_ID,
        )
        assert (
            op._create_sql_query()
            == f"""COPY INTO test
FROM '{COPY_FILE_LOCATION}'
FILEFORMAT = JSON
FILES = ('file1','file2','file3')
FORMAT_OPTIONS ('dateFormat' = 'yyyy-MM-dd')
""".strip()
        )

    def test_copy_with_expression(self):
        expression = "col1, col2"
        op = DatabricksCopyIntoOperator(
            file_location=COPY_FILE_LOCATION,
            file_format='CSV',
            table_name='test',
            task_id=TASK_ID,
            pattern='folder1/file_[a-g].csv',
            expression_list=expression,
            format_options={'header': 'true'},
            force_copy=True,
        )
        assert (
            op._create_sql_query()
            == f"""COPY INTO test
FROM (SELECT {expression} FROM '{COPY_FILE_LOCATION}')
FILEFORMAT = CSV
PATTERN = 'folder1/file_[a-g].csv'
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS ('force' = 'true')
""".strip()
        )

    def test_copy_with_credential(self):
        expression = "col1, col2"
        op = DatabricksCopyIntoOperator(
            file_location=COPY_FILE_LOCATION,
            file_format='CSV',
            table_name='test',
            task_id=TASK_ID,
            expression_list=expression,
            credential={'AZURE_SAS_TOKEN': 'abc'},
        )
        assert (
            op._create_sql_query()
            == f"""COPY INTO test
FROM (SELECT {expression} FROM '{COPY_FILE_LOCATION}' WITH (CREDENTIAL (AZURE_SAS_TOKEN = 'abc') ))
FILEFORMAT = CSV
""".strip()
        )

    def test_copy_with_target_credential(self):
        expression = "col1, col2"
        op = DatabricksCopyIntoOperator(
            file_location=COPY_FILE_LOCATION,
            file_format='CSV',
            table_name='test',
            task_id=TASK_ID,
            expression_list=expression,
            storage_credential='abc',
            credential={'AZURE_SAS_TOKEN': 'abc'},
        )
        assert (
            op._create_sql_query()
            == f"""COPY INTO test WITH (CREDENTIAL abc)
FROM (SELECT {expression} FROM '{COPY_FILE_LOCATION}' WITH (CREDENTIAL (AZURE_SAS_TOKEN = 'abc') ))
FILEFORMAT = CSV
""".strip()
        )

    def test_copy_with_encryption(self):
        op = DatabricksCopyIntoOperator(
            file_location=COPY_FILE_LOCATION,
            file_format='CSV',
            table_name='test',
            task_id=TASK_ID,
            encryption={'TYPE': 'AWS_SSE_C', 'MASTER_KEY': 'abc'},
        )
        assert (
            op._create_sql_query()
            == f"""COPY INTO test
FROM '{COPY_FILE_LOCATION}' WITH ( ENCRYPTION (TYPE = 'AWS_SSE_C', MASTER_KEY = 'abc'))
FILEFORMAT = CSV
""".strip()
        )

    def test_copy_with_encryption_and_credential(self):
        op = DatabricksCopyIntoOperator(
            file_location=COPY_FILE_LOCATION,
            file_format='CSV',
            table_name='test',
            task_id=TASK_ID,
            encryption={'TYPE': 'AWS_SSE_C', 'MASTER_KEY': 'abc'},
            credential={'AZURE_SAS_TOKEN': 'abc'},
        )
        assert (
            op._create_sql_query()
            == f"""COPY INTO test
FROM '{COPY_FILE_LOCATION}' WITH (CREDENTIAL (AZURE_SAS_TOKEN = 'abc') """
            """ENCRYPTION (TYPE = 'AWS_SSE_C', MASTER_KEY = 'abc'))
FILEFORMAT = CSV
""".strip()
        )

    def test_copy_with_validate_all(self):
        op = DatabricksCopyIntoOperator(
            file_location=COPY_FILE_LOCATION,
            file_format='JSON',
            table_name='test',
            task_id=TASK_ID,
            validate=True,
        )
        assert (
            op._create_sql_query()
            == f"""COPY INTO test
FROM '{COPY_FILE_LOCATION}'
FILEFORMAT = JSON
VALIDATE ALL
""".strip()
        )

    def test_copy_with_validate_N_rows(self):
        op = DatabricksCopyIntoOperator(
            file_location=COPY_FILE_LOCATION,
            file_format='JSON',
            table_name='test',
            task_id=TASK_ID,
            validate=10,
        )
        assert (
            op._create_sql_query()
            == f"""COPY INTO test
FROM '{COPY_FILE_LOCATION}'
FILEFORMAT = JSON
VALIDATE 10 ROWS
""".strip()
        )

    def test_incorrect_params_files_patterns(self):
        exception_message = "Only one of 'pattern' or 'files' should be specified"
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksCopyIntoOperator(
                task_id=TASK_ID,
                file_location=COPY_FILE_LOCATION,
                file_format='JSON',
                table_name='test',
                files=['file1', 'file2', 'file3'],
                pattern="abc",
            )

    def test_incorrect_params_emtpy_table(self):
        exception_message = "table_name shouldn't be empty"
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksCopyIntoOperator(
                task_id=TASK_ID,
                file_location=COPY_FILE_LOCATION,
                file_format='JSON',
                table_name='',
            )

    def test_incorrect_params_emtpy_location(self):
        exception_message = "file_location shouldn't be empty"
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksCopyIntoOperator(
                task_id=TASK_ID,
                file_location="",
                file_format='JSON',
                table_name='abc',
            )

    def test_incorrect_params_wrong_format(self):
        file_format = 'JSONL'
        exception_message = f"file_format '{file_format}' isn't supported"
        with pytest.raises(AirflowException, match=exception_message):
            DatabricksCopyIntoOperator(
                task_id=TASK_ID,
                file_location=COPY_FILE_LOCATION,
                file_format=file_format,
                table_name='abc',
            )
