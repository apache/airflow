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

"""This module contains operator to move data from Hive to DynamoDB."""

import json
from typing import TYPE_CHECKING, Callable, Optional, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class HiveToDynamoDBOperator(BaseOperator):
    """
    Moves data from Hive to DynamoDB, note that for now the data is loaded
    into memory before being pushed to DynamoDB, so this operator should
    be used for smallish amount of data.

    :param sql: SQL query to execute against the hive database. (templated)
    :param table_name: target DynamoDB table
    :param table_keys: partition key and sort key
    :param pre_process: implement pre-processing of source data
    :param pre_process_args: list of pre_process function arguments
    :param pre_process_kwargs: dict of pre_process function arguments
    :param region_name: aws region name (example: us-east-1)
    :param schema: hive database schema
    :param hiveserver2_conn_id: Reference to the
        :ref: `Hive Server2 thrift service connection id <howto/connection:hiveserver2>`.
    :param aws_conn_id: aws connection
    """

    template_fields: Sequence[str] = ('sql',)
    template_ext: Sequence[str] = ('.sql',)
    ui_color = '#a0e08c'

    def __init__(
        self,
        *,
        sql: str,
        table_name: str,
        table_keys: list,
        pre_process: Optional[Callable] = None,
        pre_process_args: Optional[list] = None,
        pre_process_kwargs: Optional[list] = None,
        region_name: Optional[str] = None,
        schema: str = 'default',
        hiveserver2_conn_id: str = 'hiveserver2_default',
        aws_conn_id: str = 'aws_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.table_name = table_name
        self.table_keys = table_keys
        self.pre_process = pre_process
        self.pre_process_args = pre_process_args
        self.pre_process_kwargs = pre_process_kwargs
        self.region_name = region_name
        self.schema = schema
        self.hiveserver2_conn_id = hiveserver2_conn_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context: 'Context'):
        hive = HiveServer2Hook(hiveserver2_conn_id=self.hiveserver2_conn_id)

        self.log.info('Extracting data from Hive')
        self.log.info(self.sql)

        data = hive.get_pandas_df(self.sql, schema=self.schema)
        dynamodb = DynamoDBHook(
            aws_conn_id=self.aws_conn_id,
            table_name=self.table_name,
            table_keys=self.table_keys,
            region_name=self.region_name,
        )

        self.log.info('Inserting rows into dynamodb')

        if self.pre_process is None:
            dynamodb.write_batch_data(json.loads(data.to_json(orient='records')))
        else:
            dynamodb.write_batch_data(
                self.pre_process(data=data, args=self.pre_process_args, kwargs=self.pre_process_kwargs)
            )

        self.log.info('Done.')
