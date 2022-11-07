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

import unittest
from tempfile import NamedTemporaryFile
from unittest import mock

import pandas as pd
import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator


class TestSqlToS3Operator(unittest.TestCase):
    @mock.patch("airflow.providers.amazon.aws.transfers.sql_to_s3.NamedTemporaryFile")
    @mock.patch("airflow.providers.amazon.aws.transfers.sql_to_s3.S3Hook")
    def test_execute_csv(self, mock_s3_hook, temp_mock):
        query = "query"
        s3_bucket = "bucket"
        s3_key = "key"

        mock_dbapi_hook = mock.Mock()
        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_pandas_df_mock = mock_dbapi_hook.return_value.get_pandas_df
        get_pandas_df_mock.return_value = test_df
        with NamedTemporaryFile() as f:
            temp_mock.return_value.__enter__.return_value.name = f.name

            op = SqlToS3Operator(
                query=query,
                s3_bucket=s3_bucket,
                s3_key=s3_key,
                sql_conn_id="mysql_conn_id",
                aws_conn_id="aws_conn_id",
                task_id="task_id",
                replace=True,
                pd_kwargs={"index": False, "header": False},
                dag=None,
            )
            op._get_hook = mock_dbapi_hook
            op.execute(None)
            mock_s3_hook.assert_called_once_with(aws_conn_id="aws_conn_id", verify=None)

            get_pandas_df_mock.assert_called_once_with(sql=query, parameters=None)

            temp_mock.assert_called_once_with(mode="r+", suffix=".csv")
            mock_s3_hook.return_value.load_file.assert_called_once_with(
                filename=f.name,
                key=s3_key,
                bucket_name=s3_bucket,
                replace=True,
            )

    @mock.patch("airflow.providers.amazon.aws.transfers.sql_to_s3.NamedTemporaryFile")
    @mock.patch("airflow.providers.amazon.aws.transfers.sql_to_s3.S3Hook")
    def test_execute_parquet(self, mock_s3_hook, temp_mock):
        query = "query"
        s3_bucket = "bucket"
        s3_key = "key"

        mock_dbapi_hook = mock.Mock()

        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_pandas_df_mock = mock_dbapi_hook.return_value.get_pandas_df
        get_pandas_df_mock.return_value = test_df
        with NamedTemporaryFile() as f:
            temp_mock.return_value.__enter__.return_value.name = f.name

            op = SqlToS3Operator(
                query=query,
                s3_bucket=s3_bucket,
                s3_key=s3_key,
                sql_conn_id="mysql_conn_id",
                aws_conn_id="aws_conn_id",
                task_id="task_id",
                file_format="parquet",
                replace=False,
                dag=None,
            )
            op._get_hook = mock_dbapi_hook
            op.execute(None)
            mock_s3_hook.assert_called_once_with(aws_conn_id="aws_conn_id", verify=None)

            get_pandas_df_mock.assert_called_once_with(sql=query, parameters=None)

            temp_mock.assert_called_once_with(mode="rb+", suffix=".parquet")
            mock_s3_hook.return_value.load_file.assert_called_once_with(
                filename=f.name, key=s3_key, bucket_name=s3_bucket, replace=False
            )

    @mock.patch("airflow.providers.amazon.aws.transfers.sql_to_s3.NamedTemporaryFile")
    @mock.patch("airflow.providers.amazon.aws.transfers.sql_to_s3.S3Hook")
    def test_execute_json(self, mock_s3_hook, temp_mock):
        query = "query"
        s3_bucket = "bucket"
        s3_key = "key"

        mock_dbapi_hook = mock.Mock()
        test_df = pd.DataFrame({"a": "1", "b": "2"}, index=[0, 1])
        get_pandas_df_mock = mock_dbapi_hook.return_value.get_pandas_df
        get_pandas_df_mock.return_value = test_df
        with NamedTemporaryFile() as f:
            temp_mock.return_value.__enter__.return_value.name = f.name

            op = SqlToS3Operator(
                query=query,
                s3_bucket=s3_bucket,
                s3_key=s3_key,
                sql_conn_id="mysql_conn_id",
                aws_conn_id="aws_conn_id",
                task_id="task_id",
                file_format="json",
                replace=True,
                pd_kwargs={"date_format": "iso", "lines": True, "orient": "records"},
                dag=None,
            )
            op._get_hook = mock_dbapi_hook
            op.execute(None)
            mock_s3_hook.assert_called_once_with(aws_conn_id="aws_conn_id", verify=None)

            get_pandas_df_mock.assert_called_once_with(sql=query, parameters=None)

            temp_mock.assert_called_once_with(mode="r+", suffix=".json")
            mock_s3_hook.return_value.load_file.assert_called_once_with(
                filename=f.name,
                key=s3_key,
                bucket_name=s3_bucket,
                replace=True,
            )

    @parameterized.expand(
        [
            ("with-csv", {"file_format": "csv", "null_string_result": None}),
            ("with-parquet", {"file_format": "parquet", "null_string_result": "None"}),
        ]
    )
    def test_fix_dtypes(self, _, params):
        op = SqlToS3Operator(
            query="query",
            s3_bucket="s3_bucket",
            s3_key="s3_key",
            task_id="task_id",
            sql_conn_id="mysql_conn_id",
        )
        dirty_df = pd.DataFrame({"strings": ["a", "b", None], "ints": [1, 2, None]})
        op._fix_dtypes(df=dirty_df, file_format=params["file_format"])
        assert dirty_df["strings"].values[2] == params["null_string_result"]
        assert dirty_df["ints"].dtype.kind == "i"

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
