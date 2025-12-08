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

import gzip
import io
from unittest import mock

import pandas as pd
import polars as pl
import pytest

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import Connection
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator


class TestSqlToS3Operator:
    @pytest.mark.parametrize(
        ("file_format", "dtype_backend", "df_kwargs", "expected_key_suffix"),
        [
            ("csv", "numpy_nullable", {"index": False, "header": False}, ".csv"),
            ("csv", "pyarrow", {"index": False, "header": False}, ".csv"),
            ("parquet", "numpy_nullable", {}, ".parquet"),
            ("parquet", "pyarrow", {}, ".parquet"),
            (
                "json",
                None,
                {"date_format": "iso", "lines": True, "orient": "records"},
                ".json",
            ),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.transfers.sql_to_s3.S3Hook")
    def test_execute_formats(self, mock_s3_hook, file_format, dtype_backend, df_kwargs, expected_key_suffix):
        query = "query"
        s3_bucket = "bucket"
        s3_key = "key"

        mock_dbapi_hook = mock.Mock()
        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_df_mock = mock_dbapi_hook.return_value.get_df
        get_df_mock.return_value = test_df

        read_df_kwargs = {"dtype_backend": dtype_backend} if dtype_backend else {}

        op = SqlToS3Operator(
            query=query,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            sql_conn_id="mysql_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            file_format=file_format,
            replace=True,
            read_kwargs=read_df_kwargs,
            df_kwargs=df_kwargs,
            dag=None,
        )
        op._get_hook = mock_dbapi_hook
        op.execute(None)

        mock_s3_hook.assert_called_once_with(aws_conn_id="aws_conn_id", verify=None)

        expected_df_kwargs = {
            "sql": query,
            "parameters": None,
            "df_type": "pandas",
        }
        if dtype_backend:
            expected_df_kwargs["dtype_backend"] = dtype_backend

        get_df_mock.assert_called_once_with(**expected_df_kwargs)

        file_obj = mock_s3_hook.return_value.load_file_obj.call_args[1]["file_obj"]
        assert isinstance(file_obj, io.BytesIO)

        mock_s3_hook.return_value.load_file_obj.assert_called_once_with(
            file_obj=file_obj,
            key=f"{s3_key}{expected_key_suffix}",
            bucket_name=s3_bucket,
            replace=True,
        )

    @mock.patch("airflow.providers.amazon.aws.transfers.sql_to_s3.S3Hook")
    def test_execute_gzip_with_bytesio(self, mock_s3_hook):
        query = "query"
        s3_bucket = "bucket"
        s3_key = "key.csv.gz"

        mock_dbapi_hook = mock.Mock()
        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_df_mock = mock_dbapi_hook.return_value.get_df
        get_df_mock.return_value = test_df

        op = SqlToS3Operator(
            query=query,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            sql_conn_id="mysql_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            replace=True,
            df_kwargs={"index": False, "compression": "gzip"},
            dag=None,
        )
        op._get_hook = mock_dbapi_hook
        op.execute(None)

        mock_s3_hook.assert_called_once_with(aws_conn_id="aws_conn_id", verify=None)
        get_df_mock.assert_called_once_with(sql=query, parameters=None, df_type="pandas")
        file_obj = mock_s3_hook.return_value.load_file_obj.call_args[1]["file_obj"]
        assert isinstance(file_obj, io.BytesIO)
        mock_s3_hook.return_value.load_file_obj.assert_called_once_with(
            file_obj=file_obj, key=s3_key, bucket_name=s3_bucket, replace=True
        )

        file_obj.seek(0)
        with gzip.GzipFile(fileobj=file_obj, mode="rb") as gz:
            decompressed_buf = io.BytesIO(gz.read())
        decompressed_buf.seek(0)
        read_df = pd.read_csv(decompressed_buf, dtype={"a": str, "b": str})
        assert read_df.equals(test_df)

    @pytest.mark.parametrize(
        "params",
        [
            pytest.param({"file_format": "csv", "null_string_result": None}, id="with-csv"),
            pytest.param({"file_format": "parquet", "null_string_result": "None"}, id="with-parquet"),
        ],
    )
    def test_fix_dtypes(self, params):
        op = SqlToS3Operator(
            query="query",
            s3_bucket="s3_bucket",
            s3_key="s3_key",
            task_id="task_id",
            sql_conn_id="mysql_conn_id",
            file_format=params["file_format"],
        )
        dirty_df = pd.DataFrame({"strings": ["a", "b", None], "ints": [1, 2, None]})
        op._fix_dtypes(df=dirty_df, file_format=op.file_format)
        assert dirty_df["strings"].values[2] == params["null_string_result"]
        assert dirty_df["ints"].dtype.kind == "i"

    @mock.patch("airflow.providers.amazon.aws.transfers.sql_to_s3.S3Hook")
    def test_fix_dtypes_not_called(self, mock_s3_hook):
        query = "query"
        s3_bucket = "bucket"
        s3_key = "key"

        mock_dbapi_hook = mock.Mock()
        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])

        get_df_mock = mock_dbapi_hook.return_value.get_df
        get_df_mock.return_value = test_df

        op = SqlToS3Operator(
            query=query,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            sql_conn_id="mysql_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            read_kwargs={"dtype_backend": "pyarrow"},
            file_format="parquet",
            replace=True,
            dag=None,
        )

        op._get_hook = mock_dbapi_hook

        with mock.patch.object(SqlToS3Operator, "_fix_dtypes") as mock_fix_dtypes:
            op.execute(None)

        mock_fix_dtypes.assert_not_called()

    def test_invalid_file_format(self):
        with pytest.raises(AirflowException):
            SqlToS3Operator(
                query="query",
                s3_bucket="bucket",
                s3_key="key",
                sql_conn_id="mysql_conn_id",
                task_id="task_id",
                file_format="invalid_format",
                dag=None,
            )

    def test_with_groupby_kwarg(self):
        """
        Test operator when the groupby_kwargs is specified
        """
        query = "query"
        s3_bucket = "bucket"
        s3_key = "key"

        op = SqlToS3Operator(
            query=query,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            sql_conn_id="mysql_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            replace=True,
            df_kwargs={"index": False, "header": False},
            groupby_kwargs={"by": "Team"},
            dag=None,
        )
        example = {
            "Team": ["Australia", "Australia", "India", "India"],
            "Player": ["Ricky", "David Warner", "Virat Kohli", "Rohit Sharma"],
            "Runs": [345, 490, 672, 560],
        }

        df = pd.DataFrame(example)
        data = []
        for group_name, df in op._partition_dataframe(df):
            data.append((group_name, df))
        data.sort(key=lambda d: d[0])
        team, df = data[0]
        assert df.equals(
            pd.DataFrame(
                {
                    "Team": ["Australia", "Australia"],
                    "Player": ["Ricky", "David Warner"],
                    "Runs": [345, 490],
                }
            )
        )
        team, df = data[1]
        assert df.equals(
            pd.DataFrame(
                {
                    "Team": ["India", "India"],
                    "Player": ["Virat Kohli", "Rohit Sharma"],
                    "Runs": [672, 560],
                }
            )
        )

    def test_without_groupby_kwarg(self):
        """
        Test operator when the groupby_kwargs is not specified
        """
        query = "query"
        s3_bucket = "bucket"
        s3_key = "key"

        op = SqlToS3Operator(
            query=query,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            sql_conn_id="mysql_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            replace=True,
            df_kwargs={"index": False, "header": False},
            dag=None,
        )
        example = {
            "Team": ["Australia", "Australia", "India", "India"],
            "Player": ["Ricky", "David Warner", "Virat Kohli", "Rohit Sharma"],
            "Runs": [345, 490, 672, 560],
        }

        df = pd.DataFrame(example)
        data = []
        for group_name, df in op._partition_dataframe(df):
            data.append((group_name, df))

        assert len(data) == 1
        team, df = data[0]
        assert df.equals(
            pd.DataFrame(
                {
                    "Team": ["Australia", "Australia", "India", "India"],
                    "Player": ["Ricky", "David Warner", "Virat Kohli", "Rohit Sharma"],
                    "Runs": [345, 490, 672, 560],
                }
            )
        )

    def test_with_max_rows_per_file(self):
        """
        Test operator when the max_rows_per_file is specified
        """
        query = "query"
        s3_bucket = "bucket"
        s3_key = "key"

        op = SqlToS3Operator(
            query=query,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            sql_conn_id="mysql_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            replace=True,
            df_kwargs={"index": False, "header": False},
            max_rows_per_file=3,
            dag=None,
        )
        example = {
            "Team": ["Australia", "Australia", "India", "India"],
            "Player": ["Ricky", "David Warner", "Virat Kohli", "Rohit Sharma"],
            "Runs": [345, 490, 672, 560],
        }

        df = pd.DataFrame(example)
        data = []
        for group_name, df in op._partition_dataframe(df):
            data.append((group_name, df))
        data.sort(key=lambda d: d[0])
        team, df = data[0]
        assert df.equals(
            pd.DataFrame(
                {
                    "Team": ["Australia", "Australia", "India"],
                    "Player": ["Ricky", "David Warner", "Virat Kohli"],
                    "Runs": [345, 490, 672],
                }
            )
        )
        team, df = data[1]
        assert df.equals(
            pd.DataFrame(
                {
                    "Team": ["India"],
                    "Player": ["Rohit Sharma"],
                    "Runs": [560],
                }
            )
        )

    @mock.patch("airflow.providers.common.sql.operators.sql.BaseHook.get_connection")
    def test_hook_params(self, mock_get_conn):
        mock_get_conn.return_value = Connection(conn_id="postgres_test", conn_type="postgres")
        op = SqlToS3Operator(
            query="query",
            s3_bucket="bucket",
            s3_key="key",
            sql_conn_id="postgres_test",
            task_id="task_id",
            sql_hook_params={
                "log_sql": False,
            },
            dag=None,
        )
        hook = op._get_hook()
        assert hook.log_sql == op.sql_hook_params["log_sql"]

    @pytest.mark.parametrize(
        ("df_type_param", "expected_df_type"),
        [
            pytest.param("polars", "polars", id="with-polars"),
            pytest.param("pandas", "pandas", id="with-pandas"),
            pytest.param(None, "pandas", id="with-default"),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.transfers.sql_to_s3.S3Hook")
    def test_execute_with_df_type(self, mock_s3_hook, df_type_param, expected_df_type):
        query = "query"
        s3_bucket = "bucket"
        s3_key = "key.csv"

        mock_dbapi_hook = mock.Mock()
        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_df_mock = mock_dbapi_hook.return_value.get_df
        get_df_mock.return_value = test_df

        kwargs = {
            "query": query,
            "s3_bucket": s3_bucket,
            "s3_key": s3_key,
            "sql_conn_id": "mysql_conn_id",
            "aws_conn_id": "aws_conn_id",
            "task_id": "task_id",
            "replace": True,
            "dag": None,
        }
        if df_type_param is not None:
            kwargs["df_type"] = df_type_param

        op = SqlToS3Operator(**kwargs)
        op._get_hook = mock_dbapi_hook
        op.execute(None)

        mock_s3_hook.assert_called_once_with(aws_conn_id="aws_conn_id", verify=None)
        get_df_mock.assert_called_once_with(sql=query, parameters=None, df_type=expected_df_type)
        file_obj = mock_s3_hook.return_value.load_file_obj.call_args[1]["file_obj"]
        assert isinstance(file_obj, io.BytesIO)
        mock_s3_hook.return_value.load_file_obj.assert_called_once_with(
            file_obj=file_obj, key=s3_key, bucket_name=s3_bucket, replace=True
        )

    @pytest.mark.parametrize(
        ("df_type", "input_df_creator"),
        [
            pytest.param(
                "pandas",
                lambda: pd.DataFrame({"category": ["A", "A", "B", "B"], "value": [1, 2, 3, 4]}),
                id="with-pandas-dataframe",
            ),
            pytest.param(
                "polars",
                lambda: pytest.importorskip("polars").DataFrame(
                    {"category": ["A", "A", "B", "B"], "value": [1, 2, 3, 4]}
                ),
                id="with-polars-dataframe",
            ),
        ],
    )
    def test_partition_dataframe(self, df_type, input_df_creator):
        """Test that _partition_dataframe works with both pandas and polars DataFrames."""
        op = SqlToS3Operator(
            query="query",
            s3_bucket="bucket",
            s3_key="key",
            sql_conn_id="mysql_conn_id",
            task_id="task_id",
            df_type=df_type,
            groupby_kwargs={"by": "category"},
        )

        input_df = input_df_creator()
        partitions = list(op._partition_dataframe(input_df))

        assert len(partitions) == 2
        for group_name, df in partitions:
            if df_type == "polars":
                assert isinstance(df, pl.DataFrame)
            else:
                assert isinstance(df, pd.DataFrame)
            assert group_name in ["A", "B"]

    @pytest.mark.parametrize(
        ("kwargs", "expected_warning", "expected_error", "expected_read_kwargs"),
        [
            pytest.param(
                {"read_pd_kwargs": {"dtype_backend": "pyarrow"}},
                "The 'read_pd_kwargs' parameter is deprecated",
                None,
                {"dtype_backend": "pyarrow"},
                id="deprecated-read-pd-kwargs-warning",
            ),
            pytest.param(
                {
                    "read_kwargs": {"dtype_backend": "pyarrow"},
                    "read_pd_kwargs": {"dtype_backend": "numpy_nullable"},
                },
                "The 'read_pd_kwargs' parameter is deprecated",
                None,
                {"dtype_backend": "pyarrow"},
                id="read-kwargs-priority-over-deprecated",
            ),
            pytest.param(
                {"max_rows_per_file": 2, "groupby_kwargs": {"by": "category"}},
                None,
                "can not be both specified",
                None,
                id="max-rows-groupby-conflict-error",
            ),
            pytest.param(
                {"pd_kwargs": {"index": False}},
                "The 'pd_kwargs' parameter is deprecated",
                None,
                None,
                id="deprecated-pd-kwargs-warning",
            ),
            pytest.param(
                {"df_kwargs": {"index": False}, "pd_kwargs": {"header": False}},
                "The 'pd_kwargs' parameter is deprecated",
                None,
                None,
                id="df-kwargs-priority-over-deprecated",
            ),
        ],
    )
    def test_parameter_validation(self, kwargs, expected_warning, expected_error, expected_read_kwargs):
        """Test parameter validation and deprecation warnings."""
        base_kwargs = {
            "query": "query",
            "s3_bucket": "bucket",
            "s3_key": "key",
            "sql_conn_id": "mysql_conn_id",
            "task_id": "task_id",
        }
        base_kwargs.update(kwargs)

        if expected_error:
            with pytest.raises(AirflowException, match=expected_error):
                SqlToS3Operator(**base_kwargs)
        elif expected_warning:
            with pytest.warns(AirflowProviderDeprecationWarning, match=expected_warning):
                op = SqlToS3Operator(**base_kwargs)
            if expected_read_kwargs:
                assert op.read_kwargs == expected_read_kwargs
        else:
            op = SqlToS3Operator(**base_kwargs)
            if expected_read_kwargs:
                assert op.read_kwargs == expected_read_kwargs

    @pytest.mark.parametrize(
        ("df_type", "should_call_fix_dtypes"),
        [
            pytest.param("pandas", True, id="pandas-calls-fix-dtypes"),
            pytest.param("polars", False, id="polars-skips-fix-dtypes"),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.transfers.sql_to_s3.S3Hook")
    def test_fix_dtypes_behavior_by_df_type(self, mock_s3_hook, df_type, should_call_fix_dtypes):
        """Test that _fix_dtypes is called/not called based on df_type."""
        query = "query"
        s3_bucket = "bucket"
        s3_key = "key"

        mock_dbapi_hook = mock.Mock()
        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_df_mock = mock_dbapi_hook.return_value.get_df
        get_df_mock.return_value = test_df

        op = SqlToS3Operator(
            query=query,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            sql_conn_id="mysql_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            df_type=df_type,
            replace=True,
            dag=None,
        )
        op._get_hook = mock_dbapi_hook

        with mock.patch.object(SqlToS3Operator, "_fix_dtypes") as mock_fix_dtypes:
            op.execute(None)

        if should_call_fix_dtypes:
            mock_fix_dtypes.assert_called_once()
        else:
            mock_fix_dtypes.assert_not_called()

    @pytest.mark.parametrize(
        ("kwargs", "expected_warning", "expected_read_kwargs", "expected_df_kwargs"),
        [
            pytest.param(
                {
                    "read_kwargs": {"dtype_backend": "pyarrow"},
                    "read_pd_kwargs": {"dtype_backend": "numpy_nullable"},
                },
                "The 'read_pd_kwargs' parameter is deprecated",
                {"dtype_backend": "pyarrow"},
                {},
                id="read-kwargs-priority-over-deprecated",
            ),
            pytest.param(
                {"read_pd_kwargs": {"dtype_backend": "numpy_nullable"}},
                "The 'read_pd_kwargs' parameter is deprecated",
                {"dtype_backend": "numpy_nullable"},
                {},
                id="read-pd-kwargs-used-when-read-kwargs-none",
            ),
            pytest.param(
                {
                    "df_kwargs": {"index": False},
                    "pd_kwargs": {"header": False},
                },
                "The 'pd_kwargs' parameter is deprecated",
                {},
                {"index": False},
                id="df-kwargs-priority-over-deprecated",
            ),
            pytest.param(
                {"pd_kwargs": {"header": False}},
                "The 'pd_kwargs' parameter is deprecated",
                {},
                {"header": False},
                id="pd-kwargs-used-when-df-kwargs-none",
            ),
        ],
    )
    def test_deprecated_kwargs_priority_behavior(
        self, kwargs, expected_warning, expected_read_kwargs, expected_df_kwargs
    ):
        """Test priority behavior and deprecation warnings for deprecated parameters."""
        base_kwargs = {
            "query": "query",
            "s3_bucket": "bucket",
            "s3_key": "key",
            "sql_conn_id": "mysql_conn_id",
            "task_id": "task_id",
        }
        base_kwargs.update(kwargs)

        with pytest.warns(AirflowProviderDeprecationWarning, match=expected_warning):
            op = SqlToS3Operator(**base_kwargs)

        assert op.read_kwargs == expected_read_kwargs
        assert op.df_kwargs == expected_df_kwargs

    @pytest.mark.parametrize(
        ("fmt", "df_kwargs", "expected_key"),
        [
            ("csv", {"compression": "gzip", "index": False}, "data.csv.gz"),
            ("csv", {"index": False}, "data.csv"),
            ("json", {"compression": "gzip"}, "data.json.gz"),
            ("json", {}, "data.json"),
            ("parquet", {"compression": "gzip"}, "data.parquet"),
            ("parquet", {}, "data.parquet"),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.transfers.sql_to_s3.S3Hook")
    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook")
    def test_file_format_handling(self, mock_dbapi_hook, mock_s3_hook, fmt, df_kwargs, expected_key):
        s3_bucket = "bucket"
        s3_key = "data." + fmt
        test_df = pd.DataFrame({"x": [1, 2]})
        mock_dbapi_hook.return_value.get_df.return_value = test_df

        op = SqlToS3Operator(
            query="SELECT * FROM test",
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            sql_conn_id="sqlite_conn",
            aws_conn_id="aws_default",
            task_id="task_id",
            file_format=fmt,
            df_kwargs=df_kwargs,
            replace=True,
            dag=None,
        )
        op._get_hook = lambda: mock_dbapi_hook.return_value
        op.execute(context=None)

        uploaded_key = mock_s3_hook.return_value.load_file_obj.call_args[1]["key"]
        assert uploaded_key == expected_key

    @pytest.mark.parametrize(
        ("file_format", "df_kwargs", "expected_suffix"),
        [
            ("csv", {"compression": "gzip", "index": False}, ".csv.gz"),
            ("csv", {"index": False}, ".csv"),
            ("json", {"compression": "gzip"}, ".json.gz"),
            ("json", {}, ".json"),
            ("parquet", {"compression": "gzip"}, ".parquet"),
            ("parquet", {}, ".parquet"),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.transfers.sql_to_s3.S3Hook")
    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook")
    def test_file_format_handling_with_groupby(
        self, mock_dbapi_hook, mock_s3_hook, file_format, df_kwargs, expected_suffix
    ):
        s3_bucket = "bucket"
        s3_key = "data"

        # Input DataFrame with groups
        test_data = pd.DataFrame(
            {"x": [1, 2, 3, 4, 5, 6], "group": ["group1", "group1", "group2", "group2", "group3", "group4"]}
        )

        mock_dbapi_hook.return_value.get_df.return_value = test_data

        op = SqlToS3Operator(
            query="SELECT * FROM test",
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            sql_conn_id="sqlite_conn",
            aws_conn_id="aws_default",
            task_id="task_id",
            file_format=file_format,
            df_kwargs=df_kwargs,
            groupby_kwargs={"by": "group"},
            replace=True,
            dag=None,
        )

        op._get_hook = lambda: mock_dbapi_hook.return_value
        op.execute(context=None)

        expected_groups = test_data["group"].unique()
        assert mock_s3_hook.return_value.load_file_obj.call_count == len(expected_groups)

        called_keys = [call.kwargs["key"] for call in mock_s3_hook.return_value.load_file_obj.call_args_list]

        for group in expected_groups:
            expected_key = f"{s3_key}_{group}{expected_suffix}"
            assert expected_key in called_keys, f"Missing expected key: {expected_key}"
