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

import mock
import unittest

from boto3.session import Session
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.utils.tests import assertEqualIgnoreMultipleSpaces


class TestS3ToRedshiftTransfer(unittest.TestCase):

    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.hooks.postgres_hook.PostgresHook.run")
    def test_execute(self, mock_run, mock_Session):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_Session.return_value = Session(access_key, secret_key)

        schema = "schema"
        table = "table"
        s3_bucket = "bucket"
        s3_key = "key"
        copy_options = ""

        t = S3ToRedshiftTransfer(
            schema=schema,
            table=table,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            copy_options=copy_options,
            redshift_conn_id="redshift_conn_id",
            aws_conn_id="aws_conn_id",
            task_id="task_id",
            dag=None)
        t.execute(None)

        copy_query = """
            COPY {schema}.{table}
            FROM 's3://{s3_bucket}/{s3_key}/{table}'
            with credentials
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            {copy_options};
        """.format(schema=schema,
                   table=table,
                   s3_bucket=s3_bucket,
                   s3_key=s3_key,
                   access_key=access_key,
                   secret_key=secret_key,
                   copy_options=copy_options)

        mock_run.assert_called_once()
        assertEqualIgnoreMultipleSpaces(self, mock_run.call_args[0][0], copy_query)
