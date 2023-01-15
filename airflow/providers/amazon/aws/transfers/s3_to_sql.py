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
from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

from typing_extensions import Literal

try:
    import csv as csv
except ImportError as e:
    from airflow.exceptions import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException from e


class S3ToSqlOperator(BaseOperator):
    """
        Loads Data from S3 into a SQL Database.
        Data should be readable as CSV.

        This operator downloads a file from an S3, reads it via `csv.reader`
        and inserts the data into a SQL database using `insert_rows` method.
        All SQL hooks are supported, as long as it is of type DbApiHook

        Extra arguments can be passed to it by using csv_reader_kwargs parameter.
        (e.g. Use different quoting or delimiters)
        Here you will find a list of all kwargs
        https://docs.python.org/3/library/csv.html#csv.reader

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ToSqlOperator`

    :param schema: reference to a specific schema in SQL database
    :param table: reference to a specific table in SQL database
    :param s3_bucket: reference to a specific S3 bucket
    :param s3_key: reference to a specific S3 key
    :param sql_conn_id: reference to a specific SQL database. Must be of type DBApiHook
    :param aws_conn_id: reference to a specific S3 / AWS connection
    :param column_list: list of column names.
        Set to `infer` if column names should be read from first line of CSV file (default)
    :param skip_first_line: If first line of CSV file should be skipped.
        If `column_list` is set to 'infer', this is ignored
    :param commit_every: The maximum number of rows to insert in one
        transaction. Set to `0` to insert all rows in one transaction.
    :param csv_reader_kwargs: key word arguments to pass to csv.reader().
        This lets you control how the CSV is read.
        e.g. To use a different delimiter, pass the following dict:
        {'delimiter' : ';'}
    """

    template_fields: Sequence[str] = (
        "s3_bucket",
        "s3_key",
        "schema",
        "table",
        "column_list",
        "sql_conn_id",
    )
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        s3_key: str,
        s3_bucket: str,
        table: str,
        column_list: Literal["infer"] | list[str] | None = "infer",
        commit_every: int = 1000,
        schema: str | None = None,
        skip_first_row: bool = False,
        sql_conn_id: str = "sql_default",
        aws_conn_id: str = "aws_default",
        csv_reader_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.schema = schema
        self.aws_conn_id = aws_conn_id
        self.sql_conn_id = sql_conn_id
        self.column_list = column_list
        self.commit_every = commit_every
        self.skip_first_row = skip_first_row
        if csv_reader_kwargs:
            self.csv_reader_kwargs = csv_reader_kwargs
        else:
            self.csv_reader_kwargs = {}

    def execute(self, context: Context) -> None:

        self.log.info("Loading %s to SQL table %s...", self.s3_key, self.table)

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        self._file = s3_hook.download_file(key=self.s3_key, bucket_name=self.s3_bucket)

        hook = self._get_hook()
        try:
            # open with newline='' as recommended
            # https://docs.python.org/3/library/csv.html#csv.reader
            with open(self._file, newline="") as file:
                reader = csv.reader(file, **self.csv_reader_kwargs)

                if self.column_list == "infer":
                    self.column_list = list(next(reader))
                    self.log.info("Column Names inferred from csv: %s", self.column_list)
                elif self.skip_first_row:
                    next(reader)

                hook.insert_rows(
                    table=self.table,
                    schema=self.schema,
                    target_fields=self.column_list,
                    rows=reader,
                    commit_every=self.commit_every,
                )

        finally:
            # Remove file downloaded from s3 to be idempotent.
            os.remove(self._file)

    def _get_hook(self) -> DbApiHook:
        self.log.debug("Get connection for %s", self.sql_conn_id)
        conn = BaseHook.get_connection(self.sql_conn_id)
        hook = conn.get_hook()
        if not callable(getattr(hook, "insert_rows", None)):
            raise AirflowException(
                "This hook is not supported. The hook class must have  an `insert_rows` method."
            )
        return hook
