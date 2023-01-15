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

from unittest.mock import patch

import pytest
from sqlalchemy import or_

from airflow import configuration, models
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.utils import db
from airflow.utils.session import create_session


class TestS3ToSqlTransfer:
    def setup_method(self):
        configuration.conf.load_test_config()

        db.merge_conn(
            models.Connection(
                conn_id="s3_test",
                conn_type="s3",
                schema="test",
                extra='{"aws_access_key_id": "aws_access_key_id", "aws_secret_access_key":'
                ' "aws_secret_access_key"}',
            )
        )
        db.merge_conn(
            models.Connection(
                conn_id="sql_test",
                conn_type="postgres",
                host="some.host.com",
                schema="test_db",
                login="user",
                password="password",
            )
        )

        self.s3_to_sql_transfer_kwargs = {
            "task_id": "s3_to_sql_task",
            "aws_conn_id": "s3_test",
            "sql_conn_id": "sql_test",
            "s3_key": "test/test.csv",
            "s3_bucket": "testbucket",
            "table": "sql_table",
            "schema": "sql_schema",
            "skip_first_row": False,
            "commit_every": 5000,
            "column_list": "infer",
        }

        self.csv_header_line = ["A1", "A2"]
        self.csv_data_line = ["A3", "A4"]

    def _csv_generator(self):
        """This is kind of hacky.
        It will mock a CSV line that is read line by line.
        Because csv.read is a generator, the mock should be a generator as well.
        """
        yield self.csv_header_line
        yield self.csv_data_line

    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.open")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.csv.reader")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.S3Hook.download_file")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.DbApiHook.insert_rows")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.os.remove")
    def test_execute(self, mock_remove, mock_insert_rows, mock_download_file, mock_csv_reader, mock_open):

        mock_csv_reader.return_value = self._csv_generator()

        S3ToSqlOperator(**self.s3_to_sql_transfer_kwargs).execute({})

        mock_download_file.assert_called_once_with(
            key=self.s3_to_sql_transfer_kwargs["s3_key"],
            bucket_name=self.s3_to_sql_transfer_kwargs["s3_bucket"],
        )

        mock_open.assert_called_once_with(mock_download_file.return_value, newline="")

        mock_insert_rows.assert_called_once_with(
            table=self.s3_to_sql_transfer_kwargs["table"],
            schema=self.s3_to_sql_transfer_kwargs["schema"],
            commit_every=self.s3_to_sql_transfer_kwargs["commit_every"],
            rows=mock_csv_reader.return_value,
            target_fields=self.csv_header_line,
        )

        assert next(mock_csv_reader.return_value) == self.csv_data_line

        mock_remove.assert_called_once_with(mock_download_file.return_value)

    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.open")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.S3Hook.download_file")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.DbApiHook.insert_rows")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.os.remove")
    def test_execute_exception(self, mock_remove, mock_insert_rows, mock_download_file, mock_open):
        """Tests if downloaded CSV file is deleted in case an exception occurred"""
        mock_insert_rows.side_effect = Exception

        with pytest.raises(Exception):
            S3ToSqlOperator(**self.s3_to_sql_transfer_kwargs).execute({})

        mock_download_file.assert_called_once_with(
            key=self.s3_to_sql_transfer_kwargs["s3_key"],
            bucket_name=self.s3_to_sql_transfer_kwargs["s3_bucket"],
        )

        mock_remove.assert_called_once_with(mock_download_file.return_value)

    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.open")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.csv.reader")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.S3Hook.download_file")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.DbApiHook.insert_rows")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.os.remove")
    def test_execute_dont_skip_first_row(
        self, mock_remove, mock_insert_rows, mock_download_file, mock_csv_reader, mock_open
    ):
        """Test if first row is not ignored when passing skip_first_row=False and colum_list=[str]"""

        mock_csv_reader.return_value = self._csv_generator()

        s3_to_sql_transfer_kwargs = self.s3_to_sql_transfer_kwargs
        s3_to_sql_transfer_kwargs.update({"skip_first_row": False, "column_list": ["Column1"]})

        S3ToSqlOperator(**s3_to_sql_transfer_kwargs).execute({})

        mock_download_file.assert_called_once_with(
            key=self.s3_to_sql_transfer_kwargs["s3_key"],
            bucket_name=self.s3_to_sql_transfer_kwargs["s3_bucket"],
        )

        mock_insert_rows.assert_called_once_with(
            table=self.s3_to_sql_transfer_kwargs["table"],
            schema=self.s3_to_sql_transfer_kwargs["schema"],
            commit_every=self.s3_to_sql_transfer_kwargs["commit_every"],
            rows=mock_csv_reader.return_value,
            target_fields=["Column1"],
        )

        # Check if first row was skipped by retrieving the next line from our mock_read_csv generator
        assert next(mock_csv_reader.return_value) == self.csv_header_line

        mock_remove.assert_called_once_with(mock_download_file.return_value)

    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.open")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.csv.reader")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.S3Hook.download_file")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.DbApiHook.insert_rows")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.os.remove")
    def test_execute_csv_reader_kwargs(
        self, mock_remove, mock_insert_rows, mock_download_file, mock_csv_reader, mock_open
    ):
        csv_reader_kwargs = {"delimiter": ";"}
        s3_to_sql_transfer_kwargs = self.s3_to_sql_transfer_kwargs
        s3_to_sql_transfer_kwargs.update({"csv_reader_kwargs": csv_reader_kwargs})

        S3ToSqlOperator(**self.s3_to_sql_transfer_kwargs).execute({})

        mock_download_file.assert_called_once_with(
            key=self.s3_to_sql_transfer_kwargs["s3_key"],
            bucket_name=self.s3_to_sql_transfer_kwargs["s3_bucket"],
        )

        mock_csv_reader.assert_called_once_with(mock_open().__enter__(), **csv_reader_kwargs)

    def teardown_method(self):
        with create_session() as session:
            (
                session.query(models.Connection)
                .filter(or_(models.Connection.conn_id == "s3_test", models.Connection.conn_id == "sql_test"))
                .delete()
            )
