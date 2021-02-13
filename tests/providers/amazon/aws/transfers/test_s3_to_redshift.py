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
#

import unittest
from unittest import mock

from boto3.session import Session

from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.utils.redshift import build_credentials_block
from tests.test_utils.asserts import assert_equal_ignore_multiple_spaces


class TestS3ToRedshiftTransfer(unittest.TestCase):
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.postgres.hooks.postgres.PostgresHook.run")
    def test_execute(self, mock_run, mock_session):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None

        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"
        copy_options = ""

        op = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )
        op.execute(None)

        credentials_block = build_credentials_block(mock_session.return_value)
        copy_query = op._build_copy_query(credentials_block, copy_options)

        assert mock_run.call_count == 1
        assert access_key in copy_query
        assert secret_key in copy_query
        assert_equal_ignore_multiple_spaces(self, mock_run.call_args[0][0], copy_query)

    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.postgres.hooks.postgres.PostgresHook.run")
    def test_truncate(self, mock_run, mock_session):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = None

        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"
        copy_options = ""

        op = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            truncate_table=True,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )
        op.execute(None)

        credentials_block = build_credentials_block(mock_session.return_value)
        copy_statement = op._build_copy_query(credentials_block, copy_options)

        truncate_statement = f'TRUNCATE TABLE {schema}.{table};'
        transaction = f"""
                    BEGIN;
                    {truncate_statement}
                    {copy_statement}
                    COMMIT
                    """
        assert_equal_ignore_multiple_spaces(self, mock_run.call_args[0][0], transaction)

        assert mock_run.call_count == 1

    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.postgres.hooks.postgres.PostgresHook.run")
    def test_execute_sts_token(self, mock_run, mock_session):
        access_key = "ASIA_aws_access_key_id"
        secret_key = "aws_secret_access_key"
        token = "aws_secret_token"
        mock_session.return_value = Session(access_key, secret_key, token)
        mock_session.return_value.access_key = access_key
        mock_session.return_value.secret_key = secret_key
        mock_session.return_value.token = token

        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"
        copy_options = ""

        op = S3ToRedshiftOperator(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None,
        )

        credentials_block = build_credentials_block(mock_session.return_value)
        copy_statement = op._build_copy_query(credentials_block, copy_options)
        op.execute(None)

        assert access_key in copy_statement
        assert secret_key in copy_statement
        assert token in copy_statement
        assert mock_run.call_count == 1
        assert_equal_ignore_multiple_spaces(self, mock_run.call_args[0][0], copy_statement)

    def test_template_fields_overrides(self):
        assert S3ToRedshiftOperator.template_fields == (
            's3_bucket',
            's3_key',
            'schema',
            'table',
            'copy_options',
        )
