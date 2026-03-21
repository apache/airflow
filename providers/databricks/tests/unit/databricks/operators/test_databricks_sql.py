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


def test_exec_write_parquet_file(tmp_path):
    """Test writing output to Parquet format."""
    with patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook") as db_mock_class:
        path = tmp_path / "testfile.parquet"
        op = DatabricksSqlOperator(
            task_id=TASK_ID,
            sql="select * from dummy",
            output_path=os.fspath(path),
            output_format="parquet",
            return_last=True,
            do_xcom_push=True,
            split_statements=False,
        )
        db_mock = db_mock_class.return_value
        db_mock.run.return_value = [SerializableRow(1, "value1"), SerializableRow(2, "value2")]
        db_mock.descriptions = [[("id",), ("value",)]]

        op.execute(None)

        import pyarrow.parquet as pq

        table = pq.read_table(path)
        assert table.num_rows == 2
        assert table.column_names == ["id", "value"]
        assert table.column("id").to_pylist() == [1, 2]
        assert table.column("value").to_pylist() == ["value1", "value2"]


def test_exec_write_avro_file_with_fastavro(tmp_path):
    """Test writing output to Avro format using fastavro."""
    pytest.importorskip("fastavro")
    with patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook") as db_mock_class:
        path = tmp_path / "testfile.avro"
        op = DatabricksSqlOperator(
            task_id=TASK_ID,
            sql="select * from dummy",
            output_path=os.fspath(path),
            output_format="avro",
            return_last=True,
            do_xcom_push=True,
            split_statements=False,
        )
        db_mock = db_mock_class.return_value
        db_mock.run.return_value = [SerializableRow(1, "value1"), SerializableRow(2, "value2")]
        db_mock.descriptions = [[("id",), ("value",)]]

        op.execute(None)

        from fastavro import reader

        with open(path, "rb") as f:
            records = list(reader(f))
        assert len(records) == 2
        assert records[0] == {"id": 1, "value": "value1"}
        assert records[1] == {"id": 2, "value": "value2"}


def test_exec_write_gcs_output(tmp_path):
    """Test writing output to GCS."""
    with (
        patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook") as db_mock_class,
        patch("airflow.providers.google.cloud.hooks.gcs.GCSHook") as gcs_mock_class,
    ):
        op = DatabricksSqlOperator(
            task_id=TASK_ID,
            sql="select * from dummy",
            output_path="gs://my-bucket/path/to/output.csv",
            output_format="csv",
            return_last=True,
            do_xcom_push=True,
            split_statements=False,
            gcp_conn_id="my_gcp_conn",
        )
        db_mock = db_mock_class.return_value
        db_mock.run.return_value = [SerializableRow(1, "value1"), SerializableRow(2, "value2")]
        db_mock.descriptions = [[("id",), ("value",)]]

        op.execute(None)

        gcs_mock_class.assert_called_once_with(
            gcp_conn_id="my_gcp_conn",
            impersonation_chain=None,
        )
        gcs_mock_class.return_value.upload.assert_called_once()
        call_kwargs = gcs_mock_class.return_value.upload.call_args[1]
        assert call_kwargs["bucket_name"] == "my-bucket"
        assert call_kwargs["object_name"] == "path/to/output.csv"


def test_exec_write_gcs_parquet_output(tmp_path):
    """Test writing Parquet output to GCS."""
    with (
        patch("airflow.providers.databricks.operators.databricks_sql.DatabricksSqlHook") as db_mock_class,
        patch("airflow.providers.google.cloud.hooks.gcs.GCSHook") as gcs_mock_class,
    ):
        op = DatabricksSqlOperator(
            task_id=TASK_ID,
            sql="select * from dummy",
            output_path="gs://my-bucket/data/results.parquet",
            output_format="parquet",
            return_last=True,
            do_xcom_push=True,
            split_statements=False,
        )
        db_mock = db_mock_class.return_value
        db_mock.run.return_value = [SerializableRow(1, "value1"), SerializableRow(2, "value2")]
        db_mock.descriptions = [[("id",), ("value",)]]

        op.execute(None)

        gcs_mock_class.return_value.upload.assert_called_once()
        call_kwargs = gcs_mock_class.return_value.upload.call_args[1]
        assert call_kwargs["bucket_name"] == "my-bucket"
        assert call_kwargs["object_name"] == "data/results.parquet"


def test_is_gcs_output():
    """Test _is_gcs_output property."""
    op_gcs = DatabricksSqlOperator(task_id=TASK_ID, sql="SELECT 1", output_path="gs://bucket/path")
    assert op_gcs._is_gcs_output is True

    op_local = DatabricksSqlOperator(task_id=TASK_ID, sql="SELECT 1", output_path="/local/path")
    assert op_local._is_gcs_output is False

    op_s3 = DatabricksSqlOperator(task_id=TASK_ID, sql="SELECT 1", output_path="s3://bucket/path")
    assert op_s3._is_gcs_output is False

    op_none = DatabricksSqlOperator(task_id=TASK_ID, sql="SELECT 1")
    assert op_none._is_gcs_output is False


def test_parse_gcs_path():
    """Test _parse_gcs_path method."""
    op = DatabricksSqlOperator(task_id=TASK_ID, sql="SELECT 1")
    bucket, object_name = op._parse_gcs_path("gs://my-bucket/path/to/file.parquet")
    assert bucket == "my-bucket"
    assert object_name == "path/to/file.parquet"
