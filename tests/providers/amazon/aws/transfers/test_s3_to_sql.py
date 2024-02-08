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

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import or_

from airflow import models
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.utils import db
from airflow.utils.session import create_session

pytestmark = pytest.mark.db_test


class TestS3ToSqlTransfer:
    def setup_method(self):
        db.merge_conn(
            models.Connection(
                conn_id="s3_test",
                conn_type="aws",
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
            "column_list": ["Column1", "Column2"],
            "schema": "sql_schema",
            "commit_every": 5000,
        }

    @pytest.fixture()
    def mock_parser(self):
        return MagicMock()

    @pytest.fixture()
    def mock_bad_hook(self):
        bad_hook = MagicMock()
        del bad_hook.insert_rows
        return bad_hook

    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.NamedTemporaryFile")
    @patch("airflow.models.connection.Connection.get_hook")
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.S3Hook.get_key")
    def test_execute(self, mock_get_key, mock_hook, mock_tempfile, mock_parser):
        S3ToSqlOperator(parser=mock_parser, **self.s3_to_sql_transfer_kwargs).execute({})

        mock_get_key.assert_called_once_with(
            key=self.s3_to_sql_transfer_kwargs["s3_key"],
            bucket_name=self.s3_to_sql_transfer_kwargs["s3_bucket"],
        )

        mock_get_key.return_value.download_fileobj.assert_called_once_with(
            mock_tempfile.return_value.__enter__.return_value
        )

        mock_parser.assert_called_once_with(mock_tempfile.return_value.__enter__.return_value.name)

        mock_hook.return_value.insert_rows.assert_called_once_with(
            table=self.s3_to_sql_transfer_kwargs["table"],
            schema=self.s3_to_sql_transfer_kwargs["schema"],
            target_fields=self.s3_to_sql_transfer_kwargs["column_list"],
            rows=mock_parser.return_value,
            commit_every=self.s3_to_sql_transfer_kwargs["commit_every"],
        )

    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.NamedTemporaryFile")
    @patch("airflow.models.connection.Connection.get_hook", return_value=mock_bad_hook)
    @patch("airflow.providers.amazon.aws.transfers.s3_to_sql.S3Hook.get_key")
    def test_execute_with_bad_hook(self, mock_get_key, mock_bad_hook, mock_tempfile, mock_parser):
        with pytest.raises(AirflowException):
            S3ToSqlOperator(parser=mock_parser, **self.s3_to_sql_transfer_kwargs).execute({})

    def test_hook_params(self, mock_parser):
        op = S3ToSqlOperator(
            parser=mock_parser,
            sql_hook_params={
                "log_sql": False,
            },
            **self.s3_to_sql_transfer_kwargs,
        )
        hook = op.db_hook
        assert hook.log_sql == op.sql_hook_params["log_sql"]

    def teardown_method(self):
        with create_session() as session:
            (
                session.query(models.Connection)
                .filter(or_(models.Connection.conn_id == "s3_test", models.Connection.conn_id == "sql_test"))
                .delete()
            )
