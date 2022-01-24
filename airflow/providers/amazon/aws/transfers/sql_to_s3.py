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

from collections import namedtuple
from enum import Enum
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Sequence, Union

import numpy as np
import pandas as pd
from typing_extensions import Literal

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.hooks.dbapi import DbApiHook
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

if TYPE_CHECKING:
    from airflow.utils.context import Context


FILE_FORMAT = Enum(
    "FILE_FORMAT",
    "CSV, PARQUET",
)

FileOptions = namedtuple('FileOptions', ['mode', 'suffix'])

FILE_OPTIONS_MAP = {
    FILE_FORMAT.CSV: FileOptions('r+', '.csv'),
    FILE_FORMAT.PARQUET: FileOptions('rb+', '.parquet'),
}


class SqlToS3Operator(BaseOperator):
    """
    Saves data from an specific SQL query into a file in S3.

    :param query: the sql query to be executed. If you want to execute a file, place the absolute path of it,
        ending with .sql extension. (templated)
    :type query: str
    :param s3_bucket: bucket where the data will be stored. (templated)
    :type s3_bucket: str
    :param s3_key: desired key for the file. It includes the name of the file. (templated)
    :type s3_key: str
    :param replace: whether or not to replace the file in S3 if it previously existed
    :type replace: bool
    :param sql_conn_id: reference to a specific database.
    :type sql_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
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
    :param file_format: the destination file format, only string 'csv' or 'parquet' is accepted.
    :type file_format: str
    :param pd_kwargs: arguments to include in ``DataFrame.to_parquet()`` or ``DataFrame.to_csv()``.
    :type pd_kwargs: dict
    """

    template_fields: Sequence[str] = (
        's3_bucket',
        's3_key',
        'query',
    )
    template_ext: Sequence[str] = ('.sql',)
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
        sql_conn_id: str,
        parameters: Union[None, Mapping, Iterable] = None,
        replace: bool = False,
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[bool, str]] = None,
        file_format: Literal['csv', 'parquet'] = 'csv',
        pd_kwargs: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.sql_conn_id = sql_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.replace = replace
        self.pd_kwargs = pd_kwargs or {}
        self.parameters = parameters

        if file_format == "csv":
            self.file_format = FILE_FORMAT.CSV
            if "path_or_buf" in self.pd_kwargs:
                raise AirflowException('The argument path_or_buf is not allowed, please remove it')
        elif file_format == "parquet":
            self.file_format = FILE_FORMAT.PARQUET
        else:
            raise AirflowException(f"The argument file_format doesn't support {file_format} value.")

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

    def execute(self, context: 'Context') -> None:
        sql_hook = self._get_hook()
        s3_conn = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        data_df = sql_hook.get_pandas_df(sql=self.query, parameters=self.parameters)
        self.log.info("Data from SQL obtained")

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

    def _get_hook(self) -> DbApiHook:
        self.log.debug("Get connection for %s", self.sql_conn_id)
        conn = BaseHook.get_connection(self.sql_conn_id)
        hook = conn.get_hook()
        if not callable(getattr(hook, 'get_pandas_df', None)):
            raise AirflowException(
                "This hook is not supported. The hook class must have get_pandas_df method."
            )
        return hook
