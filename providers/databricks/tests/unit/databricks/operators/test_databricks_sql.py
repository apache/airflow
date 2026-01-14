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

import json
import os
from collections import namedtuple
from unittest.mock import patch

import pytest
from databricks.sql.types import Row

from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

DATE = "2017-04-20"
TASK_ID = "databricks-sql-operator"
DEFAULT_CONN_ID = "databricks_default"


# Serializable Row object similar to the one returned by the Hook
SerializableRow = namedtuple("Row", ["id", "value"])  # type: ignore[name-match]
SerializableRow2 = namedtuple("Row2", ["id2", "value2"])  # type: ignore[name-match]


@pytest.mark.parametrize(
    ("sql", "return_last", "split_statement", "hook_results", "hook_descriptions", "expected_results"),
    [
        pytest.param(
            "select * from dummy",
            True,
            True,
            [SerializableRow(1, "value1"), SerializableRow(2, "value2")],
            [[("id",), ("value",)]],
            ([("id",), ("value",)], [Row(id=1, value="value1"), Row(id=2, value="value2")]),
            id="Scalar: Single SQL statement, return_last, split statement",
        ),
        pytest.param(
            "select * from dummy;select * from dummy2",
            True,
            True,
            [SerializableRow(1, "value1"), SerializableRow(2, "value2")],
            [[("id",), ("value",)]],
            ([("id",), ("value",)], [Row(id=1, value="value1"), Row(id=2, value="value2")]),
            id="Scalar: Multiple SQL statements, return_last, split statement",
        ),
        pytest.param(
            "select * from dummy",
            False,
            False,
            [SerializableRow(1, "value1"), SerializableRow(2, "value2")],
            [[("id",), ("value",)]],
            ([("id",), ("value",)], [Row(id=1, value="value1"), Row(id=2, value="value2")]),
            id="Scalar: Single SQL statements, no return_last (doesn't matter), no split statement",
        ),
        pytest.param(
            "select * from dummy",
            True,
            False,
            [SerializableRow(1, "value1"), SerializableRow(2, "value2")],
            [[("id",), ("value",)]],
            ([("id",), ("value",)], [Row(id=1, value="value1"), Row(id=2, value="value2")]),
            id="Scalar: Single SQL statements, return_last (doesn't matter), no split statement",
        ),
        pytest.param(
            ["select * from dummy"],
            False,
            False,
            [[SerializableRow(1, "value1"), SerializableRow(2, "value2")]],
            [[("id",), ("value",)]],
            [([("id",), ("value",)], [Row(id=1, value="value1"), Row(id=2, value="value2")])],
            id="Non-Scalar: Single SQL statements in list, no return_last, no split statement",
        ),
        pytest.param(
            ["select * from dummy", "select * from dummy2"],
            False,
            False,
            [
                [SerializableRow(1, "value1"), SerializableRow(2, "value2")],
                [SerializableRow2(1, "value1"), SerializableRow2(2, "value2")],
            ],
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [
                ([("id",), ("value",)], [Row(id=1, value="value1"), Row(id=2, value="value2")]),
                ([("id2",), ("value2",)], [Row(id2=1, value2="value1"), Row(id2=2, value2="value2")]),
            ],
            id="Non-Scalar: Multiple SQL statements in list, no return_last (no matter), no split statement",
        ),
        pytest.param(
            ["select * from dummy", "select * from dummy2"],
            True,
            False,
            [
                [SerializableRow(1, "value1"), SerializableRow(2, "value2")],
                [SerializableRow2(1, "value1"), SerializableRow2(2, "value2")],
            ],
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [
                ([("id",), ("value",)], [Row(id=1, value="value1"), Row(id=2, value="value2")]),
                ([("id2",), ("value2",)], [Row(id2=1, value2="value1"), Row(id2=2, value2="value2")]),
            ],
            id="Non-Scalar: Multiple SQL statements in list, return_last (no matter), no split statement",
        ),
    ],
)
def test_exec_success(sql, return_last, split_statement, hook_results, hook_descriptions, expected_results):
    """
    Test the execute function in case where SQL query was successful.
    """
    with patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook") as db_mock_class:
        op = DatabricksSqlOperator(
            task_id=TASK_ID,
            sql=sql,
            do_xcom_push=True,
            return_last=return_last,
            split_statements=split_statement,
        )
        db_mock = db_mock_class.return_value
        db_mock.run.return_value = hook_results
        db_mock.descriptions = hook_descriptions

        execute_results = op.execute(None)

        assert execute_results == expected_results
        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            http_path=None,
            session_configuration=None,
            sql_endpoint_name=None,
            http_headers=None,
            catalog=None,
            schema=None,
            caller="DatabricksSqlOperator",
        )
        db_mock.run.assert_called_once_with(
            sql=sql,
            parameters=None,
            handler=fetch_all_handler,
            autocommit=False,
            return_last=return_last,
            split_statements=split_statement,
        )


@pytest.mark.parametrize(
    ("return_last", "split_statements", "sql", "descriptions", "hook_results", "do_xcom_push"),
    [
        pytest.param(
            True,
            False,
            "select * from dummy",
            [[("id",), ("value",)]],
            [SerializableRow(1, "value1"), SerializableRow(2, "value2")],
            True,
            id="Scalar: return_last True and split_statement False",
        ),
        pytest.param(
            False,
            True,
            "select * from dummy",
            [[("id",), ("value",)]],
            [[SerializableRow(1, "value1"), SerializableRow(2, "value2")]],
            True,
            id="Non-Scalar: return_last False and split_statement True",
        ),
        pytest.param(
            True,
            True,
            "select * from dummy",
            [[("id",), ("value",)]],
            [SerializableRow(1, "value1"), SerializableRow(2, "value2")],
            True,
            id="Scalar: return_last True and split_statement True",
        ),
        pytest.param(
            False,
            False,
            "select * from dummy",
            [[("id",), ("value",)]],
            [SerializableRow(1, "value1"), SerializableRow(2, "value2")],
            True,
            id="Scalar: return_last False and split_statement is False",
        ),
        pytest.param(
            False,
            True,
            "select * from dummy2; select * from dummy",
            [[("id2",), ("value2",)], [("id",), ("value",)]],
            [
                [SerializableRow2(1, "value1"), SerializableRow(2, "value2")],
                [SerializableRow(id=1, value="value1"), SerializableRow(id=2, value="value2")],
            ],
            True,
            id="Non-Scalar: return_last False and split_statement is True",
        ),
        pytest.param(
            True,
            True,
            "select * from dummy2; select * from dummy",
            [[("id2",), ("value2",)], [("id",), ("value",)]],
            [SerializableRow(1, "value1"), SerializableRow(2, "value2")],
            True,
            id="Scalar: return_last True and split_statement is True",
        ),
        pytest.param(
            True,
            True,
            ["select * from dummy2", "select * from dummy"],
            [[("id2",), ("value2",)], [("id",), ("value",)]],
            [[SerializableRow(1, "value1"), SerializableRow(2, "value2")]],
            True,
            id="Non-Scalar: sql is list and return_last is True",
        ),
        pytest.param(
            False,
            True,
            ["select * from dummy2", "select * from dummy"],
            [[("id2",), ("value2",)], [("id",), ("value",)]],
            [[SerializableRow(1, "value1"), SerializableRow(2, "value2")]],
            True,
            id="Non-Scalar: sql is list and return_last is False",
        ),
        pytest.param(
            False,
            True,
            ["select * from dummy2", "select * from dummy"],
            [[("id2",), ("value2",)], [("id",), ("value",)]],
            [[SerializableRow(1, "value1"), SerializableRow(2, "value2")]],
            False,
            id="Write output when do_xcom_push is False",
        ),
    ],
)
@pytest.mark.parametrize("output_format", ["csv", "json", "jsonl"])
def test_exec_write_file(
    return_last, split_statements, sql, descriptions, hook_results, do_xcom_push, output_format, tmp_path
):
    """
    Test the execute function in case where SQL query was successful
    and data is written as CSV, JSON.
    """
    with patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook") as db_mock_class:
        path = tmp_path / "testfile"
        op = DatabricksSqlOperator(
            task_id=TASK_ID,
            sql=sql,
            output_path=os.fspath(path),
            output_format=output_format,
            return_last=return_last,
            do_xcom_push=do_xcom_push,
            split_statements=split_statements,
        )
        db_mock = db_mock_class.return_value
        mock_results = hook_results
        db_mock.run.return_value = mock_results
        db_mock.descriptions = descriptions

        op.execute(None)

        if output_format == "csv":
            results = path.read_text().splitlines()
            # In all cases only result of last query i output as file
            assert results == ["id,value", "1,value1", "2,value2"]
        elif output_format == "json":
            results = json.loads(path.read_text())
            assert results == [
                {"id": 1, "value": "value1"},
                {"id": 2, "value": "value2"},
            ]
        elif output_format == "jsonl":
            results = path.read_text().splitlines()
            assert results == [
                '{"id": 1, "value": "value1"}',
                '{"id": 2, "value": "value2"}',
            ]

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            http_path=None,
            session_configuration=None,
            sql_endpoint_name=None,
            http_headers=None,
            catalog=None,
            schema=None,
            caller="DatabricksSqlOperator",
        )
        db_mock.run.assert_called_once_with(
            sql=sql,
            parameters=None,
            handler=fetch_all_handler,
            autocommit=False,
            return_last=return_last,
            split_statements=split_statements,
        )


def test_hook_is_cached():
    op = DatabricksSqlOperator(task_id=TASK_ID, sql="SELECT 42")
    hook = op.get_db_hook()
    hook2 = op.get_db_hook()
    assert hook is hook2


@patch("os.unlink")
@patch("airflow.providers.databricks.operators.databricks_sql.tempfile")
@patch("airflow.providers.google.cloud.hooks.gcs.GCSHook")
@patch("pyarrow.parquet.write_table")
@patch("pyarrow.Table")
@patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook")
def test_parquet_export_to_gcs(
    db_mock_class, mock_pa_table, mock_pq_write, mock_gcs_hook_class, mock_tempfile, mock_os_unlink
):
    """Test Parquet export to GCS."""
    mock_db = db_mock_class.return_value
    mock_db.run.return_value = [SerializableRow(1, "value1"), SerializableRow(2, "value2")]
    mock_db.descriptions = [[("id", "int"), ("value", "string")]]

    mock_gcs_hook = mock_gcs_hook_class.return_value

    # Mock tempfile
    mock_tmp = mock_tempfile.NamedTemporaryFile.return_value.__enter__.return_value
    mock_tmp.name = "/tmp/test.parquet"

    op = DatabricksSqlOperator(
        task_id=TASK_ID,
        sql="SELECT * FROM dummy",
        output_path="gs://test-bucket/output.parquet",
        output_format="parquet",
        gcp_conn_id="google_cloud_default",
    )

    op.execute(None)

    # Verify GCSHook was created with correct connection ID
    mock_gcs_hook_class.assert_called_once_with(gcp_conn_id="google_cloud_default")

    # Verify upload was called
    assert mock_gcs_hook.upload.called
    upload_call = mock_gcs_hook.upload.call_args
    assert upload_call.kwargs["bucket_name"] == "test-bucket"
    assert upload_call.kwargs["object_name"] == "output.parquet"

    # Verify temp file cleanup was called
    mock_os_unlink.assert_called_once_with("/tmp/test.parquet")


@patch("os.unlink")
@patch("airflow.providers.databricks.operators.databricks_sql.tempfile")
@patch("airflow.providers.google.cloud.hooks.gcs.GCSHook")
@patch("fastavro.writer")
@patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook")
def test_avro_export_to_gcs(
    db_mock_class, mock_fastavro_writer, mock_gcs_hook_class, mock_tempfile, mock_os_unlink
):
    """Test Avro export to GCS."""
    mock_db = db_mock_class.return_value
    mock_db.run.return_value = [SerializableRow(1, "value1"), SerializableRow(2, "value2")]
    mock_db.descriptions = [[("id", "int"), ("value", "string")]]

    mock_gcs_hook = mock_gcs_hook_class.return_value

    # Mock tempfile
    mock_tmp = mock_tempfile.NamedTemporaryFile.return_value.__enter__.return_value
    mock_tmp.name = "/tmp/test.avro"

    op = DatabricksSqlOperator(
        task_id=TASK_ID,
        sql="SELECT * FROM dummy",
        output_path="gs://test-bucket/output.avro",
        output_format="avro",
        gcp_conn_id="google_cloud_default",
    )

    op.execute(None)

    # Verify GCSHook was created with correct connection ID
    mock_gcs_hook_class.assert_called_once_with(gcp_conn_id="google_cloud_default")

    # Verify upload was called
    assert mock_gcs_hook.upload.called
    upload_call = mock_gcs_hook.upload.call_args
    assert upload_call.kwargs["bucket_name"] == "test-bucket"
    assert upload_call.kwargs["object_name"] == "output.avro"

    # Verify fastavro.writer was called
    assert mock_fastavro_writer.called

    # Verify temp file cleanup was called
    mock_os_unlink.assert_called_once_with("/tmp/test.avro")


@patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook")
def test_parquet_requires_gcs_path(db_mock_class):
    """Test that Parquet format requires GCS path."""
    mock_db = db_mock_class.return_value
    mock_db.run.return_value = [SerializableRow(1, "value1")]
    mock_db.descriptions = [[("id", "int")]]

    op = DatabricksSqlOperator(
        task_id=TASK_ID,
        sql="SELECT * FROM dummy",
        output_path="/tmp/output.parquet",
        output_format="parquet",
    )

    with pytest.raises(ValueError, match="Parquet format is only supported for GCS exports"):
        op.execute(None)


@patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook")
def test_avro_requires_gcs_path(db_mock_class):
    """Test that Avro format requires GCS path."""
    mock_db = db_mock_class.return_value
    mock_db.run.return_value = [SerializableRow(1, "value1")]
    mock_db.descriptions = [[("id", "int")]]

    op = DatabricksSqlOperator(
        task_id=TASK_ID,
        sql="SELECT * FROM dummy",
        output_path="/tmp/output.avro",
        output_format="avro",
    )

    with pytest.raises(ValueError, match="Avro format is only supported for GCS exports"):
        op.execute(None)


@patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook")
def test_gcs_requires_gcp_conn_id(db_mock_class):
    """Test that GCS export requires gcp_conn_id."""
    mock_db = db_mock_class.return_value
    mock_db.run.return_value = [SerializableRow(1, "value1")]
    mock_db.descriptions = [[("id", "int")]]

    op = DatabricksSqlOperator(
        task_id=TASK_ID,
        sql="SELECT * FROM dummy",
        output_path="gs://test-bucket/output.parquet",
        output_format="parquet",
        # Note: gcp_conn_id not specified
    )

    with pytest.raises(ValueError, match="gcp_conn_id must be specified for GCS exports"):
        op.execute(None)


@patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook")
def test_csv_not_supported_for_gcs(db_mock_class):
    """Test that CSV format with GCS path is not yet supported."""
    mock_db = db_mock_class.return_value
    mock_db.run.return_value = [SerializableRow(1, "value1")]
    mock_db.descriptions = [[("id", "int")]]

    op = DatabricksSqlOperator(
        task_id=TASK_ID,
        sql="SELECT * FROM dummy",
        output_path="gs://test-bucket/output.csv",
        output_format="csv",
        gcp_conn_id="google_cloud_default",
    )

    with pytest.raises(ValueError, match="Format 'csv' with GCS path is not yet supported"):
        op.execute(None)
