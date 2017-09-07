# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.contrib.hooks.dynamodb_hook import DynamoDBHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class HiveToDynamoDBTransfer(BaseOperator):
    """
    Moves data from Hive to DynamoDB, note that for now the data is loaded
    into memory before being pushed to DynamoDB, so this operator should
    be used for smallish amount of data.

    :param sql: SQL query to execute against the hive database
    :type sql: str
    :param table_name: target DynamoDB table
    :type table_name: str
    :param table_keys: partition key and sort key
    :type table_keys: list
    :param aws_conn_id: aws connection
    :type aws_conn_id: str
    :param dynamodb_conn_id: dynamodb connection
    :type dynamodb_conn_id: str
    :param hiveserver2_conn_id: source hive connection
    :type hiveserver2_conn_id: str
    """

    template_fields = ('sql')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(
            self,
            sql,
            table_name,
            table_keys,
            pre_process=None,
            hiveserver2_conn_id='hiveserver2_default',
            aws_conn_id='aws_default',
            *args, **kwargs):
        super(HiveToDynamoDBTransfer, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table_name = table_name
        self.table_keys = table_keys
        self.pre_process = pre_process
        self.hiveserver2_conn_id = hiveserver2_conn_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        hive = HiveServer2Hook(hiveserver2_conn_id=self.hiveserver2_conn_id)

        logging.info("Extracting data from Hive")
        logging.info(self.sql)

        results = hive.get_records(self.sql)
        dynamodb = DynamoDBHook(aws_conn_id=self.aws_conn_id,
                                table_name=self.table_name, table_keys=self.table_keys)

        logging.info("Inserting rows into dynamodb")

        if self.pre_process is None:
            dynamodb.write_batch_data(results)
        else:
            dynamodb.write_batch_data(
                self.pre_process.process_data(results=results))

        logging.info("Done.")
