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

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.openlineage.facet import (
    Dataset,
    SQLJobFacet,
)
from airflow.providers.databricks.operators.databricks_sql import DatabricksCopyIntoOperator
from airflow.providers.openlineage.extractors import OperatorLineage

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
FROM '{COPY_FILE_LOCATION}' WITH (CREDENTIAL (AZURE_SAS_TOKEN = 'abc') ENCRYPTION (TYPE = 'AWS_SSE_C', MASTER_KEY = 'abc'))
FILEFORMAT = CSV""".strip()
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


@mock.patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook")
def test_get_openlineage_facets_on_complete_s3(mock_hook):
    """Test OpenLineage facets generation for S3 source."""
    mock_hook().run.return_value = [
        {"file": "s3://bucket/dir1/file1.csv"},
        {"file": "s3://bucket/dir1/file2.csv"},
    ]
    mock_hook().get_connection().host = "databricks.com"

    op = DatabricksCopyIntoOperator(
        task_id="test",
        table_name="schema.table",
        file_location="s3://bucket/dir1",
        file_format="CSV",
    )
    op._sql = "COPY INTO schema.table FROM 's3://bucket/dir1'"
    op._result = mock_hook().run.return_value

    lineage = op.get_openlineage_facets_on_complete(None)

    assert lineage == OperatorLineage(
        inputs=[Dataset(namespace="s3://bucket", name="dir1")],
        outputs=[Dataset(namespace="databricks://databricks.com", name="schema.table")],
        job_facets={"sql": SQLJobFacet(query="COPY INTO schema.table FROM 's3://bucket/dir1'")},
        run_facets={},
    )


@mock.patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook")
def test_get_openlineage_facets_on_complete_with_errors(mock_hook):
    """Test OpenLineage facets generation with extraction errors."""
    mock_hook().run.return_value = [
        {"file": "s3://bucket/dir1/file1.csv"},
        {"file": "invalid://location/file.csv"},  # Invalid URI
        {"file": "azure://account.invalid.windows.net/container/file.csv"},  # Invalid Azure URI
    ]
    mock_hook().get_connection().host = "databricks.com"

    op = DatabricksCopyIntoOperator(
        task_id="test",
        table_name="schema.table",
        file_location="s3://bucket/dir1",
        file_format="CSV",
    )
    op._sql = "COPY INTO schema.table FROM 's3://bucket/dir1'"
    op._result = mock_hook().run.return_value

    lineage = op.get_openlineage_facets_on_complete(None)

    # Check inputs and outputs
    assert len(lineage.inputs) == 1
    assert lineage.inputs[0].namespace == "s3://bucket"
    assert lineage.inputs[0].name == "dir1"

    assert len(lineage.outputs) == 1
    assert lineage.outputs[0].namespace == "databricks://databricks.com"
    assert lineage.outputs[0].name == "schema.table"

    # Check facets exist and have correct structure
    assert "sql" in lineage.job_facets
    assert lineage.job_facets["sql"].query == "COPY INTO schema.table FROM 's3://bucket/dir1'"

    assert "extractionError" not in lineage.run_facets


@mock.patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook")
def test_get_openlineage_facets_on_complete_no_sql(mock_hook):
    """Test OpenLineage facets generation when no SQL is available."""
    op = DatabricksCopyIntoOperator(
        task_id="test",
        table_name="schema.table",
        file_location="s3://bucket/dir1",
        file_format="CSV",
    )

    lineage = op.get_openlineage_facets_on_complete(None)
    assert lineage == OperatorLineage()


@mock.patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook")
def test_get_openlineage_facets_on_complete_gcs(mock_hook):
    """Test OpenLineage facets generation specifically for GCS paths."""
    mock_hook().run.return_value = [
        {"file": "gs://bucket1/dir1/file1.csv"},
        {"file": "gs://bucket1/dir2/nested/file2.csv"},
        {"file": "gs://bucket2/file3.csv"},
        {"file": "gs://bucket2"},  # Edge case: root path
        {"file": "gs://invalid-bucket/@#$%"},  # Invalid path
    ]
    mock_hook().get_connection.return_value.host = "databricks.com"
    mock_hook().query_ids = ["query_123"]

    op = DatabricksCopyIntoOperator(
        task_id="test",
        table_name="catalog.schema.table",
        file_location="gs://location",
        file_format="CSV",
    )
    op.execute(None)
    result = op.get_openlineage_facets_on_complete(None)

    # Check inputs - only one input from file_location
    assert len(result.inputs) == 1
    assert result.inputs[0].namespace == "gs://location"
    assert result.inputs[0].name == "/"

    # Check outputs
    assert len(result.outputs) == 1
    assert result.outputs[0].namespace == "databricks://databricks.com"
    assert result.outputs[0].name == "catalog.schema.table"

    # Check SQL job facet
    assert "sql" in result.job_facets
    assert "COPY INTO catalog.schema.table" in result.job_facets["sql"].query
    assert "FILEFORMAT = CSV" in result.job_facets["sql"].query


@mock.patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook")
def test_get_openlineage_facets_on_complete_invalid_location(mock_hook):
    """Test OpenLineage facets generation with invalid file_location."""
    mock_hook().get_connection().host = "databricks.com"

    op = DatabricksCopyIntoOperator(
        task_id="test",
        table_name="schema.table",
        file_location="invalid://location",  # Invalid location
        file_format="CSV",
    )
    op._sql = "COPY INTO schema.table FROM 'invalid://location'"
    op._result = [{"file": "s3://bucket/file.csv"}]

    lineage = op.get_openlineage_facets_on_complete(None)

    # Should have no inputs due to invalid location
    assert len(lineage.inputs) == 0

    # Should not have output and SQL facets
    assert len(lineage.outputs) == 0
    assert "sql" in lineage.job_facets

    # Should have extraction error facet
    assert "extractionError" in lineage.run_facets
    assert lineage.run_facets["extractionError"].totalTasks == 1
    assert lineage.run_facets["extractionError"].failedTasks == 1
    assert len(lineage.run_facets["extractionError"].errors) == 1
