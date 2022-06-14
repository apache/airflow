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

import warnings
from typing import Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

warnings.warn(
    "This module is deprecated. Please use airflow.providers.amazon.aws.transfers.sql_to_s3`.",
    DeprecationWarning,
    stacklevel=2,
)


class MySQLToS3Operator(SqlToS3Operator):
    """
    This class is deprecated.
    Please use `airflow.providers.amazon.aws.transfers.sql_to_s3.SqlToS3Operator`.
    """

    template_fields_renderers = {
        "pd_csv_kwargs": "json",
    }

    def __init__(
        self,
        *,
        mysql_conn_id: str = 'mysql_default',
        pd_csv_kwargs: Optional[dict] = None,
        index: bool = False,
        header: bool = False,
        **kwargs,
    ) -> None:
        warnings.warn(
            """
            MySQLToS3Operator is deprecated.
            Please use `airflow.providers.amazon.aws.transfers.sql_to_s3.SqlToS3Operator`.
            """,
            DeprecationWarning,
            stacklevel=2,
        )

        pd_kwargs = kwargs.get('pd_kwargs', {})
        if kwargs.get('file_format', "csv") == "csv":
            if "path_or_buf" in pd_kwargs:
                raise AirflowException('The argument path_or_buf is not allowed, please remove it')
            if "index" not in pd_kwargs:
                pd_kwargs["index"] = index
            if "header" not in pd_kwargs:
                pd_kwargs["header"] = header
            kwargs["pd_kwargs"] = {**kwargs.get('pd_kwargs', {}), **pd_kwargs}
        elif pd_csv_kwargs is not None:
            raise TypeError("pd_csv_kwargs may not be specified when file_format='parquet'")

        super().__init__(sql_conn_id=mysql_conn_id, **kwargs)
