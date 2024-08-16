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

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.databricks.operators.databricks_sql import DatabricksCopyIntoOperator
from airflow.utils import timezone

DATE = "2017-04-20"
TASK_ID = "databricks-sql-operator"
DEFAULT_CONN_ID = "databricks_default"
COPY_FILE_LOCATION = "s3://my-bucket/jsonData"


def test_copy_with_files():
    op = DatabricksCopyIntoOperator(
        file_location=COPY_FILE_LOCATION,
        file_format="JSON",
        table_name="test",
        files=["file1", "file2", "file3"],
        format_options={"dateFormat": "yyyy-MM-dd"},
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


def test_copy_with_expression():
    expression = "col1, col2"
    op = DatabricksCopyIntoOperator(
        file_location=COPY_FILE_LOCATION,
        file_format="CSV",
        table_name="test",
        task_id=TASK_ID,
        pattern="folder1/file_[a-g].csv",
        expression_list=expression,
        format_options={"header": "true"},
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


def test_copy_with_credential():
    expression = "col1, col2"
    op = DatabricksCopyIntoOperator(
        file_location=COPY_FILE_LOCATION,
        file_format="CSV",
        table_name="test",
        task_id=TASK_ID,
        expression_list=expression,
        credential={"AZURE_SAS_TOKEN": "abc"},
    )
    assert (
        op._create_sql_query()
        == f"""COPY INTO test
FROM (SELECT {expression} FROM '{COPY_FILE_LOCATION}' WITH (CREDENTIAL (AZURE_SAS_TOKEN = 'abc') ))
FILEFORMAT = CSV
""".strip()
    )


def test_copy_with_target_credential():
    expression = "col1, col2"
    op = DatabricksCopyIntoOperator(
        file_location=COPY_FILE_LOCATION,
        file_format="CSV",
        table_name="test",
        task_id=TASK_ID,
        expression_list=expression,
        storage_credential="abc",
        credential={"AZURE_SAS_TOKEN": "abc"},
    )
    assert (
        op._create_sql_query()
        == f"""COPY INTO test WITH (CREDENTIAL abc)
FROM (SELECT {expression} FROM '{COPY_FILE_LOCATION}' WITH (CREDENTIAL (AZURE_SAS_TOKEN = 'abc') ))
FILEFORMAT = CSV
""".strip()
    )


def test_copy_with_encryption():
    op = DatabricksCopyIntoOperator(
        file_location=COPY_FILE_LOCATION,
        file_format="CSV",
        table_name="test",
        task_id=TASK_ID,
        encryption={"TYPE": "AWS_SSE_C", "MASTER_KEY": "abc"},
    )
    assert (
        op._create_sql_query()
        == f"""COPY INTO test
FROM '{COPY_FILE_LOCATION}' WITH ( ENCRYPTION (TYPE = 'AWS_SSE_C', MASTER_KEY = 'abc'))
FILEFORMAT = CSV
""".strip()
    )


def test_copy_with_encryption_and_credential():
    op = DatabricksCopyIntoOperator(
        file_location=COPY_FILE_LOCATION,
        file_format="CSV",
        table_name="test",
        task_id=TASK_ID,
        encryption={"TYPE": "AWS_SSE_C", "MASTER_KEY": "abc"},
        credential={"AZURE_SAS_TOKEN": "abc"},
    )
    assert (
        op._create_sql_query()
        == f"""COPY INTO test
FROM '{COPY_FILE_LOCATION}' WITH (CREDENTIAL (AZURE_SAS_TOKEN = 'abc') """
        """ENCRYPTION (TYPE = 'AWS_SSE_C', MASTER_KEY = 'abc'))
FILEFORMAT = CSV
""".strip()
    )


def test_copy_with_validate_all():
    op = DatabricksCopyIntoOperator(
        file_location=COPY_FILE_LOCATION,
        file_format="JSON",
        table_name="test",
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


def test_copy_with_validate_N_rows():
    op = DatabricksCopyIntoOperator(
        file_location=COPY_FILE_LOCATION,
        file_format="JSON",
        table_name="test",
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


def test_incorrect_params_files_patterns():
    exception_message = "Only one of 'pattern' or 'files' should be specified"
    with pytest.raises(AirflowException, match=exception_message):
        DatabricksCopyIntoOperator(
            task_id=TASK_ID,
            file_location=COPY_FILE_LOCATION,
            file_format="JSON",
            table_name="test",
            files=["file1", "file2", "file3"],
            pattern="abc",
        )


def test_incorrect_params_emtpy_table():
    exception_message = "table_name shouldn't be empty"
    with pytest.raises(AirflowException, match=exception_message):
        DatabricksCopyIntoOperator(
            task_id=TASK_ID,
            file_location=COPY_FILE_LOCATION,
            file_format="JSON",
            table_name="",
        )


def test_incorrect_params_emtpy_location():
    exception_message = "file_location shouldn't be empty"
    with pytest.raises(AirflowException, match=exception_message):
        DatabricksCopyIntoOperator(
            task_id=TASK_ID,
            file_location="",
            file_format="JSON",
            table_name="abc",
        )


def test_incorrect_params_wrong_format():
    file_format = "JSONL"
    exception_message = f"file_format '{file_format}' isn't supported"
    with pytest.raises(AirflowException, match=exception_message):
        DatabricksCopyIntoOperator(
            task_id=TASK_ID,
            file_location=COPY_FILE_LOCATION,
            file_format=file_format,
            table_name="abc",
        )


@pytest.mark.db_test
def test_templating(create_task_instance_of_operator, session):
    ti = create_task_instance_of_operator(
        DatabricksCopyIntoOperator,
        # Templated fields
        file_location="{{ 'file-location' }}",
        files="{{ 'files' }}",
        table_name="{{ 'table-name' }}",
        databricks_conn_id="{{ 'databricks-conn-id' }}",
        # Other parameters
        file_format="JSON",
        dag_id="test_template_body_templating_dag",
        task_id="test_template_body_templating_task",
        execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        session=session,
    )
    session.add(ti)
    session.commit()
    ti.render_templates()
    task: DatabricksCopyIntoOperator = ti.task
    assert task.file_location == "file-location"
    assert task.files == "files"
    assert task.table_name == "table-name"
    assert task.databricks_conn_id == "databricks-conn-id"
