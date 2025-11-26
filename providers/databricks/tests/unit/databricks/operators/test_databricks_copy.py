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

from airflow.providers.common.compat.openlineage.facet import (
    Dataset,
    ExternalQueryRunFacet,
    SQLJobFacet,
)
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.databricks.operators.databricks_sql import DatabricksCopyIntoOperator
from airflow.providers.openlineage.extractors import OperatorLineage

DATE = "2017-04-20"
TASK_ID = "databricks-sql-operator"
DEFAULT_CONN_ID = "databricks_default"
COPY_FILE_LOCATION = "s3://my-bucket/jsonData"


def test_copy_with_files():
    import re

    op = DatabricksCopyIntoOperator(
        file_location=COPY_FILE_LOCATION,
        file_format="JSON",
        table_name="test",
        files=["file1", "file2", "file3"],
        format_options={"dateFormat": "yyyy-MM-dd"},
        task_id=TASK_ID,
    )
    sql = op._create_sql_query()
    expected_pattern = (
        rf"COPY INTO test\s+FROM '{COPY_FILE_LOCATION}'\s+"
        r"FILEFORMAT = JSON\s+"
        r"(FILES = (ARRAY\(|\())'file1','file2','file3'\)?\s+"
        r"FORMAT_OPTIONS \('dateFormat' = 'yyyy-MM-dd'\)"
    )

    assert re.fullmatch(expected_pattern, sql.strip(), flags=re.MULTILINE)


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


def test_hook_is_cached():
    op = DatabricksCopyIntoOperator(
        file_location=COPY_FILE_LOCATION,
        file_format="JSON",
        table_name="test",
        task_id=TASK_ID,
    )
    hook = op._get_hook()
    hook2 = op._get_hook()
    assert hook is hook2


@pytest.mark.parametrize(
    ("file_location", "expected_namespace", "expected_name"),
    (
        ("gs://bucket/another_dir/file1.csv", "gs://bucket", "another_dir/file1.csv"),
        ("gs://bucket/another_dir/", "gs://bucket", "another_dir"),
        ("s3://bucket/another_dir", "s3://bucket", "another_dir"),
        ("s3://bucket/", "s3://bucket", "/"),
        (
            "abfss://container@account.dfs.core.windows.net/my-data/csv",
            "abfss://container@account.dfs.core.windows.net",
            "my-data/csv",
        ),
        (
            "abfss://container@account.dfs.core.windows.net",
            "abfss://container@account.dfs.core.windows.net",
            "/",
        ),
        (
            "wasbs://container@account.dfs.core.windows.net",
            "wasbs://container@account.dfs.core.windows.net",
            "/",
        ),
    ),
)
def test_build_input_openlineage_dataset_correct(file_location, expected_namespace, expected_name):
    op = DatabricksCopyIntoOperator(
        file_location=file_location,
        file_format="JSON",
        table_name="test",
        task_id=TASK_ID,
    )
    ds, errors = op._build_input_openlineage_dataset()
    assert ds == Dataset(namespace=expected_namespace, name=expected_name)
    assert not errors


@pytest.mark.parametrize(
    ("file_location", "expected_error"),
    (
        ("azure://bucket/another_dir/file1.csv", "Unsupported scheme: `azure`"),
        ("r2://bucket/another_dir/file1.csv", "Unsupported scheme: `r2`"),
        ("my_random_location", "Unsupported scheme: ``"),
    ),
)
def test_build_input_openlineage_dataset_silences_error(file_location, expected_error):
    op = DatabricksCopyIntoOperator(
        file_location=file_location,
        file_format="JSON",
        table_name="test",
        task_id=TASK_ID,
    )
    ds, errors = op._build_input_openlineage_dataset()
    assert ds is None
    assert len(errors) == 1
    assert errors[0].task == file_location
    assert expected_error in errors[0].errorMessage


@pytest.mark.parametrize(
    ("table_name", "default_catalog", "default_schema", "expected_name"),
    (
        ("c.s.t", None, None, "c.s.t"),
        ("s.t", None, None, "s.t"),
        ("s.t", "dfc", None, "dfc.s.t"),
        ("t", None, None, "ol_default_schema.t"),
        ("c.s.t", "dfc", "dfs", "c.s.t"),
        ("s.t", "dfc", "dfs", "dfc.s.t"),
        ("t", "dfc", "dfs", "dfc.dfs.t"),
    ),
)
def test_build_output_openlineage_dataset_correct(table_name, default_catalog, default_schema, expected_name):
    op = DatabricksCopyIntoOperator(
        file_location=COPY_FILE_LOCATION,
        file_format="JSON",
        table_name=table_name,
        task_id=TASK_ID,
        catalog=default_catalog,
        schema=default_schema,
    )
    mock_hook = mock.MagicMock()
    mock_hook.get_openlineage_default_schema.return_value = (
        "ol_default_schema" if not default_schema else default_schema
    )
    mock_hook.catalog = default_catalog

    mock_get_hook = mock.MagicMock()
    mock_get_hook.return_value = mock_hook
    op._get_hook = mock_get_hook

    ds, errors = op._build_output_openlineage_dataset("ol_namespace")
    assert ds == Dataset(namespace="ol_namespace", name=expected_name)
    assert not errors


def test_build_output_openlineage_dataset_silences_error():
    table_name = "test"
    op = DatabricksCopyIntoOperator(
        file_location=COPY_FILE_LOCATION,
        file_format="JSON",
        table_name=table_name,
        task_id=TASK_ID,
    )

    def mock_raise():
        raise ValueError("test")

    op._get_hook = mock_raise

    ds, errors = op._build_output_openlineage_dataset("ol_namespace")
    assert ds is None
    assert len(errors) == 1
    assert errors[0].task == table_name
    assert errors[0].errorMessage == "test"


def test_get_openlineage_facets_early_return_when_no_sql_found():
    op = DatabricksCopyIntoOperator(
        file_location=COPY_FILE_LOCATION,
        file_format="JSON",
        table_name="test",
        task_id=TASK_ID,
    )
    op._sql = None
    result = op.get_openlineage_facets_on_complete(None)
    assert result == OperatorLineage()


def test_get_openlineage_facets():
    op = DatabricksCopyIntoOperator(
        file_location=COPY_FILE_LOCATION,
        file_format="JSON",
        table_name="test",
        task_id=TASK_ID,
        catalog="default_catalog",
        schema="default_schema",
    )
    mock_hook = mock.MagicMock()
    mock_hook.get_openlineage_default_schema.return_value = "default_schema"
    mock_hook.catalog = "default_catalog"
    mock_hook.query_ids = ["query_id"]
    mock_hook.get_openlineage_database_info.return_value = mock.MagicMock(scheme="scheme", authority="host")

    mock_get_hook = mock.MagicMock()
    mock_get_hook.return_value = mock_hook
    op._get_hook = mock_get_hook

    op.execute(None)

    result = op.get_openlineage_facets_on_complete(None)
    assert result.inputs == [Dataset(namespace="s3://my-bucket", name="jsonData")]
    assert result.outputs == [Dataset(namespace="scheme://host", name="default_catalog.default_schema.test")]
    assert result.run_facets == {
        "externalQuery": ExternalQueryRunFacet(externalQueryId="query_id", source="scheme://host")
    }
    assert result.job_facets == {"sql": SQLJobFacet(query=op._sql)}
