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

import os
from tempfile import NamedTemporaryFile
from typing import Optional, Union

import numpy as np
import pandas as pd

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.decorators import apply_defaults


class MySQLToS3Operator(BaseOperator):
    """
    Saves data from an specific MySQL query into a file in S3.

    :param query: the sql query to be executed. If you want to execute a file, place the absolute path of it,
        ending with .sql extension.
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

        - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
    :type verify: bool or str
    :param pd_csv_kwargs: arguments to include in pd.to_csv (header, index, columns...)
    :type pd_csv_kwargs: dict
    :param index: whether to have the index or not in the dataframe
    :type index: str
    :param header: whether to include header or not into the S3 file
    :type header: bool
    """

    template_fields = (
        's3_key',
        'query',
    )
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(
        self,
        *,
        query: str,
        s3_bucket: str,
        s3_key: str,
        mysql_conn_id: str = 'mysql_default',
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[bool, str]] = None,
        pd_csv_kwargs: Optional[dict] = None,
        index: Optional[bool] = False,
        header: Optional[bool] = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.mysql_conn_id = mysql_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify

        self.pd_csv_kwargs = pd_csv_kwargs or {}
        if "path_or_buf" in self.pd_csv_kwargs:
            raise AirflowException('The argument path_or_buf is not allowed, please remove it')
        if "index" not in self.pd_csv_kwargs:
            self.pd_csv_kwargs["index"] = index
        if "header" not in self.pd_csv_kwargs:
            self.pd_csv_kwargs["header"] = header

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
        with NamedTemporaryFile(mode='r+', suffix='.csv') as tmp_csv:
            data_df.to_csv(tmp_csv.name, **self.pd_csv_kwargs)
            s3_conn.load_file(filename=tmp_csv.name, key=self.s3_key, bucket_name=self.s3_bucket)

        if s3_conn.check_for_key(self.s3_key, bucket_name=self.s3_bucket):
            file_location = os.path.join(self.s3_bucket, self.s3_key)
            self.log.info("File saved correctly in %s", file_location)
