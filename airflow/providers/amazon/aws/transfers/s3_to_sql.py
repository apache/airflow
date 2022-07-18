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
from typing import TYPE_CHECKING, List, Optional, Sequence, Union

import pandas

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.utils.s3 import fix_int_dtypes

if TYPE_CHECKING:
    from airflow.utils.context import Context


class S3ToSqlOperator(BaseOperator):
    """
    Loads a file from S3 into a SQL table.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ToSqlOperator`

    :param s3_key: path to s3 file. (templated)
    :param destination_table: target table on sql. (templated)
    :param file_format: input file format. csv, json or parquet. (templated)
    :param file_options: file reader options.
    :param source_conn_id: source connection.
    :param destination_conn_id: destination connection.
    :param preoperator: sql statement or list of statements to be
        executed prior to loading the data. (templated)
    :param insert_args: extra params for `insert_rows` method.
    """

    template_fields: Sequence[str] = ('s3_key', 'destination_table', 'file_format', 'preoperator')
    template_ext: Sequence[str] = (
        '.sql',
        '.hql',
    )
    template_fields_renderers = {"preoperator": "sql"}
    ui_color = '#b0f07c'

    def __init__(
        self,
        *,
        s3_key: str,
        destination_table: str,
        file_format: str,
        file_options: Optional[dict] = None,
        source_conn_id: str = 'aws_default',
        destination_conn_id: str = 'sql_default',
        preoperator: Optional[Union[str, List[str]]] = None,
        insert_args: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_key = s3_key
        self.destination_table = destination_table
        self.file_format = file_format
        self.file_options = file_options or {}
        self.source_conn_id = source_conn_id
        self.destination_conn_id = destination_conn_id
        self.preoperator = preoperator
        self.insert_args = insert_args or {}

    def execute(self, context: 'Context'):
        source_hook = S3Hook(aws_conn_id=self.source_conn_id)
        destination_hook = BaseHook.get_hook(self.destination_conn_id)

        self.log.info("Extracting data from %s", self.source_conn_id)
        self.log.info("Download file from: \n %s", self.s3_key)
        file = source_hook.download_file(key=self.s3_key)
        try:
            if self.file_format == 'csv':
                df = pandas.read_csv(file, **self.file_options)
            elif self.file_format == 'parquet':
                df = pandas.read_parquet(file, **self.file_options)
            elif self.file_format == 'json':
                df = pandas.read_json(file, **self.file_options)
            else:
                raise AirflowException('File format was not found!!')
            if self.preoperator:
                run = getattr(destination_hook, 'run', None)
                if not callable(run):
                    raise RuntimeError(
                        f"Hook for connection {self.destination_conn_id!r} "
                        f"({type(destination_hook).__name__}) has no `run` method"
                    )
                self.log.info("Running pre-operator")
                self.log.info(self.preoperator)
                run(self.preoperator)
            fix_int_dtypes(df)
            self.log.info("Inserting rows into %s", self.destination_conn_id)
            df.to_sql(self.destination_table, destination_hook.get_conn(), **self.insert_args)
        finally:
            os.remove(file)
