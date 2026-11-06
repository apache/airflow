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

import logging
from datetime import datetime
from unittest import mock

from boto3.session import Session

from airflow.models.connection import Connection
from airflow.providers.teradata.transfers.s3_to_teradata import S3ToTeradataOperator

DEFAULT_DATE = datetime(2024, 1, 1)

AWS_CONN_ID = "aws_default"
TERADATA_CONN_ID = "teradata_default"
S3_SOURCE_KEY = "aws/test"
TERADATA_TABLE = "test"
TASK_ID = "transfer_file"


class TestS3ToTeradataTransfer:
    def test_init(self):
        operator = S3ToTeradataOperator(
            s3_source_key=S3_SOURCE_KEY,
            teradata_table=TERADATA_TABLE,
            aws_conn_id=AWS_CONN_ID,
            teradata_conn_id=TERADATA_CONN_ID,
            task_id=TASK_ID,
            dag=None,
        )

        assert operator.aws_conn_id == AWS_CONN_ID
        assert operator.s3_source_key == S3_SOURCE_KEY
        assert operator.teradata_conn_id == TERADATA_CONN_ID
        assert operator.teradata_table == TERADATA_TABLE
        assert operator.task_id == TASK_ID

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.teradata.hooks.teradata.TeradataHook.run")
    def test_execute(self, mock_run, mock_session, mock_connection, mock_hook):
        access_key = "aws_access_key_id"
        access_secret = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, access_secret)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = access_secret
        mock_session.return_value.token = None

        mock_connection.return_value = Connection()
        mock_hook.return_value = Connection()

        op = S3ToTeradataOperator(
            s3_source_key=S3_SOURCE_KEY,
            teradata_table=TERADATA_TABLE,
            aws_conn_id=AWS_CONN_ID,
            teradata_conn_id=TERADATA_CONN_ID,
            task_id=TASK_ID,
            dag=None,
        )
        op.execute(None)

        assert mock_run.call_count == 1

    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.teradata.hooks.teradata.TeradataHook.run")
    def test_execute_keeps_inline_credentials_out_of_the_log(
        self, mock_run, mock_session, mock_connection, mock_hook, caplog
    ):
        access_key = "aws_access_key_id"
        access_secret = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, access_secret)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = access_secret
        mock_session.return_value.token = None

        mock_connection.return_value = Connection()
        mock_hook.return_value = Connection()

        op = S3ToTeradataOperator(
            s3_source_key=S3_SOURCE_KEY,
            teradata_table=TERADATA_TABLE,
            aws_conn_id=AWS_CONN_ID,
            teradata_conn_id=TERADATA_CONN_ID,
            task_id=TASK_ID,
            dag=None,
        )
        with caplog.at_level(logging.INFO):
            op.execute(None)

        assert access_secret not in caplog.text
        assert "ACCESS_ID= '***' ACCESS_KEY= '***'" in caplog.text

        # The statement actually sent still carries the real credentials.
        sent_sql = mock_run.call_args.args[0]
        assert f"ACCESS_KEY= '{access_secret}'" in sent_sql

    @mock.patch("airflow.providers.teradata.transfers.s3_to_teradata.TeradataHook")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    def test_execute_disables_hook_sql_logging_for_inline_credentials(
        self, mock_session, mock_connection, mock_s3_conn, mock_teradata_hook
    ):
        mock_session.return_value = Session("k", "s")
        mock_session.return_value.access_key = "k"
        mock_session.return_value.secret_key = "s"
        mock_session.return_value.token = None
        mock_connection.return_value = Connection()
        mock_s3_conn.return_value = Connection()

        S3ToTeradataOperator(
            s3_source_key=S3_SOURCE_KEY,
            teradata_table=TERADATA_TABLE,
            aws_conn_id=AWS_CONN_ID,
            teradata_conn_id=TERADATA_CONN_ID,
            task_id=TASK_ID,
            dag=None,
        ).execute(None)

        assert mock_teradata_hook.return_value.log_sql is False

    @mock.patch("airflow.providers.teradata.transfers.s3_to_teradata.TeradataHook")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
    @mock.patch("airflow.models.connection.Connection")
    @mock.patch("boto3.session.Session")
    def test_execute_leaves_sql_logging_alone_with_authorization_object(
        self, mock_session, mock_connection, mock_s3_conn, mock_teradata_hook
    ):
        mock_connection.return_value = Connection()
        mock_s3_conn.return_value = Connection()

        S3ToTeradataOperator(
            s3_source_key=S3_SOURCE_KEY,
            teradata_table=TERADATA_TABLE,
            aws_conn_id=AWS_CONN_ID,
            teradata_conn_id=TERADATA_CONN_ID,
            teradata_authorization_name="auth_obj",
            task_id=TASK_ID,
            dag=None,
        ).execute(None)

        assert mock_teradata_hook.return_value.log_sql is not False
