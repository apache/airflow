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

import pandas as pd
from boto3.session import Session

from airflow.operators.mysql_to_s3_operator import MySQLToS3Operator


class TestMySqlToS3Operator(unittest.TestCase):

    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.hooks.mysql_hook.MySqlHook")
    def test_execute(self, mock_hook, mock_session):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        query = "query"
        s3_bucket = "bucket"
        s3_key = "key"

        op = MySQLToS3Operator(query=query,
                          s3_bucket=s3_bucket,
                          s3_key=s3_key,
                          mysql_conn_id="mysql_conn_id",
                          aws_conn_id="aws_conn_id",
                          task_id="task_id",
                          header=False,
                          index=False,
                          dag=None
                          )
        op.execute(None)

        mock_hook.assert_called_once_with(mysql_conn_id="mysql_conn_id")

        mock_hook.return_value.get_pandas_df.assert_called_once_with(query=query)

        test_df = pd.DataFrame({'a': '1', 'b': '2'}, index=[0, 1])
        mock_hook.return_value.get_pandas_df.return_value = test_df

        mock_hook.return_value.fix_int_dtypes.assert_called_once_with(test_df)

        mock_session.assert_called_once()
        mock_session.return_value = Session(access_key, secret_key)
        