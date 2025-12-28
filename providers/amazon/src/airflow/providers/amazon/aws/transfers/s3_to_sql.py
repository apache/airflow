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

from collections.abc import Callable, Iterable, Sequence
from functools import cached_property
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.compat.sdk import AirflowException, BaseHook, BaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class S3ToSqlOperator(BaseOperator):
    """
    Load Data from S3 into a SQL Database.

    You need to provide a parser function that takes a filename as an input
    and returns an iterable of rows

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ToSqlOperator`

    :param schema: reference to a specific schema in SQL database
    :param table: reference to a specific table in SQL database
    :param s3_bucket: reference to a specific S3 bucket
    :param s3_key: reference to a specific S3 key
    :param sql_conn_id: reference to a specific SQL database. Must be of type DBApiHook
    :param sql_hook_params: Extra config params to be passed to the underlying hook.
        Should match the desired hook constructor params.
    :param aws_conn_id: reference to a specific S3 / AWS connection
    :param column_list: list of column names to use in the insert SQL.
    :param commit_every: The maximum number of rows to insert in one
        transaction. Set to `0` to insert all rows in one transaction.
    :param parser: parser function that takes a filepath as input and returns an iterable.
        e.g. to use a CSV parser that yields rows line-by-line, pass the following
        function:

        .. code-block:: python

            def parse_csv(filepath):
                import csv

                with open(filepath, newline="") as file:
                    yield from csv.reader(file)
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
        parser: Callable[[str], Iterable[Iterable]],
        column_list: list[str] | None = None,
        commit_every: int = 1000,
        schema: str | None = None,
        sql_conn_id: str = "sql_default",
        sql_hook_params: dict | None = None,
        aws_conn_id: str | None = "aws_default",
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
        self.parser = parser
        self.sql_hook_params = sql_hook_params

    def execute(self, context: Context) -> None:
        self.log.info("Loading %s to SQL table %s...", self.s3_key, self.table)

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_obj = s3_hook.get_key(key=self.s3_key, bucket_name=self.s3_bucket)

        with NamedTemporaryFile() as local_tempfile:
            s3_obj.download_fileobj(local_tempfile)
            local_tempfile.flush()
            local_tempfile.seek(0)

            self.db_hook.insert_rows(
                table=self.table,
                schema=self.schema,
                target_fields=self.column_list,
                rows=self.parser(local_tempfile.name),
                commit_every=self.commit_every,
            )

    @cached_property
    def db_hook(self):
        self.log.debug("Get connection for %s", self.sql_conn_id)
        conn = BaseHook.get_connection(self.sql_conn_id)
        hook = conn.get_hook(hook_params=self.sql_hook_params)
        if not callable(getattr(hook, "insert_rows", None)):
            raise AirflowException(
                "This hook is not supported. The hook class must have an `insert_rows` method."
            )
        return hook
