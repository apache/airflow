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
from io import StringIO
from unittest import mock

import numpy as np
import pandas as pd
from boto3.session import Session

from airflow.operators.mysql_to_s3_operator import MySQLToS3Operator
from airflow.utils.tests import assertEqualIgnoreMultipleSpaces


class TestMySqlToS3Operator(unittest.TestCase):

    @mock.patch("boto3.session.Session")
    @mock.patch("airflow.hooks.mysql_hook.MySqlHook")
    def test_execute(self, mock_run, mock_session,):
        access_key = "aws_access_key_id"
        secret_key = "aws_secret_access_key"
        mock_session.return_value = Session(access_key, secret_key)
        query = "query"
        s3_bucket = "bucket"
        s3_key = "key"
        header = False
        index = False

        MySQLToS3Operator(query=query,
                          s3_bucket=s3_bucket,
                          s3_key=s3_key,
                          mysql_conn_id="mysql_conn_id",
                          aws_conn_id="aws_conn_id",
                          task_id="task_id",
                          dag=None
                          ).execute(None)

        df = mock_run.get_pandas_df(query)
        for col in df:
            if "float" in df[col].dtype.name and df[col].hasnans:
                # inspect values to determine if dtype of non-null values is int or float
                notna_series = df[col].dropna().values
                if np.isclose(notna_series, notna_series.astype(int)).all():
                    # set to dtype that retains integers and supports NaNs
                    df[col] = np.where(df[col].isnull(), None, df[col]).astype(pd.Int64Dtype)

        file_obj = StringIO()
        df.to_csv(file_obj, header=header, index=index)
        mock_session.load_file_obj(file_obj=file_obj,
                                   key=s3_key,
                                   bucket_name=s3_bucket)

        assert mock_run.call_count == 1
        assertEqualIgnoreMultipleSpaces(self, mock_run.call_args[0][0], mock_session.load_file_obj)
