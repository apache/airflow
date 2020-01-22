# -*- coding: utf-8 -*-
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
from parameterized import parameterized

from airflow.operators.redshift_to_s3_operator import RedshiftToS3Transfer
from tests.test_utils.asserts import assert_equal_ignore_multiple_spaces


class TestRedshiftToS3Transfer(unittest.TestCase):

    @parameterized.expand([
        [True, "key/table_"],
        [False, "key"],
    ])
    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.providers.postgres.hooks.postgres.PostgresHook.run")
    def test_execute(self, table_as_file_name, expected_s3_key, mock_run, mock_session,):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"
        unload_options = ['HEADER', ]

        RedshiftToS3Transfer(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            unload_options=unload_options,
            include_header=True,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            table_as_file_name=table_as_file_name,
            dag=None
        ).execute(None)

        unload_options = '\n\t\t\t'.join(unload_options)
        select_query = "SELECT * FROM {schema}.{table}".format(schema=schema, table=table)
        unload_query = """
                    UNLOAD ('{select_query}')
                    TO 's3://{s3_bucket}/{s3_key}'
                    with credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {unload_options};
                    """.format(select_query=select_query,
                               s3_bucket=s3_bucket,
                               s3_key=expected_s3_key,
                               access_key=access_key,
                               secret_key=secret_key,
                               unload_options=unload_options)

        assert mock_run.call_count == 1
        assert_equal_ignore_multiple_spaces(self, mock_run.call_args[0][0], unload_query)
