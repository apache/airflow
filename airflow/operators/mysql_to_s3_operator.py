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
"""
Transfer data from MySQL into a S3 bucket.
"""
import os
import pickle
import tempfile
from typing import Optional, Union

import numpy as np
import pandas as pd

from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.decorators import apply_defaults


class MySQLToS3Operator(BaseOperator):
    """
    Saves data from an specific MySQL query into a file in S3.

    :param query: the sql query to be executed.
    :type query: str
    :param s3_bucket: bucket where the data will be stored
    :type s3_bucket: str
    :param s3_key: desired key for the file. It includes the name of the file
    :type s3_key: str
    :param mysql_conn_id: reference to a specific mysql database
    :type mysql_conn_id: str
    :param aws_conn_id: reference to a specific S3 connection
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - False: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - path/to/cert/bundle.pem: A filename of the CA cert bundle to use.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    :param header: whether to include header or not into the S3 file
    :type header: bool
    :param index: whether to include index or not into the S3 file
    :type index: bool
    """

    @apply_defaults
    def __init__(
            self,
            query: str,
            s3_bucket: str,
            s3_key: str,
            mysql_conn_id: str = 'mysql_default',
            aws_conn_id: str = 'aws_default',
            verify: Optional[Union[bool, str]] = None,
            header: Optional[bool] = False,
            index: Optional[bool] = False,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.query = query
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.mysql_conn_id = mysql_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.header = header
        self.index = index

    def _fix_int_dtypes(self, df):
        """
        Mutate DataFrame to set dtypes for int columns containing NaN values."
        """
        for col in df:
            if "float" in df[col].dtype.name and df[col].hasnans:
                # inspect values to determine if dtype of non-null values is int or float
                notna_series = df[col].dropna().values
                if np.isclose(notna_series, notna_series.astype(int)).all():
                    # set to dtype that retains integers and supports NaNs
                    df[col] = np.where(df[col].isnull(), None, df[col]).astype(pd.Int64Dtype)

    def execute(self, context):
        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        s3_conn = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        data_df = mysql_hook.get_pandas_df(self.query)
        self.log.info("Data from MySQL obtained")

        self._fix_int_dtypes(data_df)
        with tempfile.NamedTemporaryFile(suffix='.csv') as tmp_csv:
            tmp_csv.write(pickle.dumps(data_df))
            s3_conn.load_file(tmp_csv.name,
                              self.s3_bucket,
                              self.s3_key)

        if s3_conn.check_for_key(self.s3_key, bucket_name=self.s3_bucket):
            file_location = os.path.join(self.s3_bucket, self.s3_key)
            self.log.info("File saved correctly in %s", file_location)
