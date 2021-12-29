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
import warnings
from collections import namedtuple
from enum import Enum
from tempfile import NamedTemporaryFile
from typing import Optional, Union

import numpy as np
import pandas as pd
from typing_extensions import Literal

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook

FILE_FORMAT = Enum(
    "FILE_FORMAT",
    "CSV, PARQUET",
)

FileOptions = namedtuple('FileOptions', ['mode', 'suffix'])

FILE_OPTIONS_MAP = {
    FILE_FORMAT.CSV: FileOptions('r+', '.csv'),
    FILE_FORMAT.PARQUET: FileOptions('rb+', '.parquet'),
}


class MySQLToS3Operator(BaseOperator):
    """
    Saves data from an specific MySQL query into a file in S3.

    :param query: the sql query to be executed. If you want to execute a file, place the absolute path of it,
        ending with .sql extension. (templated)
    :type query: str
    :param s3_bucket: bucket where the data will be stored. (templated)
    :type s3_bucket: str
    :param s3_key: desired key for the file. It includes the name of the file. (templated)
    :type s3_key: str
    :param replace: whether or not to replace the file in S3 if it previously existed
    :type replace: bool
    :param mysql_conn_id: Reference to :ref:`mysql connection id <howto/connection:mysql>`.
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
    :param file_format: the destination file format, only string 'csv' or 'parquet' is accepted.
    :type file_format: str
    :param pd_kwargs: arguments to include in ``DataFrame.to_parquet()`` or
        ``DataFrame.to_csv()``. This is preferred than ``pd_csv_kwargs``.
    :type pd_kwargs: dict
    """

    template_fields = (
        's3_bucket',
        's3_key',
        'query',
    )
    template_ext = ('.sql',)
    template_fields_renderers = {
        "query": "sql",
        "pd_csv_kwargs": "json",
        "pd_kwargs": "json",
    }

    def __init__(
        self,
        *,
        query: str,
        s3_bucket: str,
        s3_key: str,
        replace: bool = False,
        mysql_conn_id: str = 'mysql_default',
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[bool, str]] = None,
        pd_csv_kwargs: Optional[dict] = None,
        index: bool = False,
        header: bool = False,
        file_format: Literal['csv', 'parquet'] = 'csv',
        pd_kwargs: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.mysql_conn_id = mysql_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.replace = replace

        if file_format == "csv":
            self.file_format = FILE_FORMAT.CSV
        else:
            self.file_format = FILE_FORMAT.PARQUET

        if pd_csv_kwargs:
            warnings.warn(
                "pd_csv_kwargs is deprecated. Please use pd_kwargs.",
                DeprecationWarning,
                stacklevel=2,
            )
        if index or header:
            warnings.warn(
                "index and header are deprecated. Please pass them via pd_kwargs.",
                DeprecationWarning,
                stacklevel=2,
            )

        self.pd_kwargs = pd_kwargs or pd_csv_kwargs or {}
        if self.file_format == FILE_FORMAT.CSV:
            if "path_or_buf" in self.pd_kwargs:
                raise AirflowException('The argument path_or_buf is not allowed, please remove it')
            if "index" not in self.pd_kwargs:
                self.pd_kwargs["index"] = index
            if "header" not in self.pd_kwargs:
                self.pd_kwargs["header"] = header
        else:
            if pd_csv_kwargs is not None:
                raise TypeError("pd_csv_kwargs may not be specified when file_format='parquet'")

    @staticmethod
    def _fix_int_dtypes(df: pd.DataFrame) -> None:
        """Mutate DataFrame to set dtypes for int columns containing NaN values."""
        for col in df:
            if "float" in df[col].dtype.name and df[col].hasnans:
                # inspect values to determine if dtype of non-null values is int or float
                notna_series = df[col].dropna().values
                if np.isclose(notna_series, notna_series.astype(int)).all():
                    # set to dtype that retains integers and supports NaNs
                    df[col] = np.where(df[col].isnull(), None, df[col])
                    df[col] = df[col].astype(pd.Int64Dtype())

    def execute(self, context) -> None:
        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        s3_conn = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        data_df = mysql_hook.get_pandas_df(self.query)
        self.log.info("Data from MySQL obtained")

        self._fix_int_dtypes(data_df)
        file_options = FILE_OPTIONS_MAP[self.file_format]
        with NamedTemporaryFile(mode=file_options.mode, suffix=file_options.suffix) as tmp_file:
            if self.file_format == FILE_FORMAT.CSV:
                data_df.to_csv(tmp_file.name, **self.pd_kwargs)
            else:
                data_df.to_parquet(tmp_file.name, **self.pd_kwargs)
            s3_conn.load_file(
                filename=tmp_file.name, key=self.s3_key, bucket_name=self.s3_bucket, replace=self.replace
            )

        if s3_conn.check_for_key(self.s3_key, bucket_name=self.s3_bucket):
            file_location = os.path.join(self.s3_bucket, self.s3_key)
            self.log.info("File saved correctly in %s", file_location)
