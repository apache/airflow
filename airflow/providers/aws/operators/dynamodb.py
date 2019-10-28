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

import json
from copy import copy
from os.path import getsize
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Dict, Optional
from uuid import uuid4

from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.providers.aws.hooks.dynamodb import AwsDynamoDBHook
from airflow.utils.decorators import apply_defaults


class HiveToDynamoDBTransferOperator(BaseOperator):
    """
    Moves data from Hive to DynamoDB, note that for now the data is loaded
    into memory before being pushed to DynamoDB, so this operator should
    be used for smallish amount of data.

    :param sql: SQL query to execute against the hive database. (templated)
    :type sql: str
    :param table_name: target DynamoDB table
    :type table_name: str
    :param table_keys: partition key and sort key
    :type table_keys: list
    :param pre_process: implement pre-processing of source data
    :type pre_process: function
    :param pre_process_args: list of pre_process function arguments
    :type pre_process_args: list
    :param pre_process_kwargs: dict of pre_process function arguments
    :type pre_process_kwargs: dict
    :param region_name: aws region name (example: us-east-1)
    :type region_name: str
    :param schema: hive database schema
    :type schema: str
    :param hiveserver2_conn_id: source hive connection
    :type hiveserver2_conn_id: str
    :param aws_conn_id: aws connection
    :type aws_conn_id: str
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(
            self,
            sql,
            table_name,
            table_keys,
            pre_process=None,
            pre_process_args=None,
            pre_process_kwargs=None,
            region_name=None,
            schema='default',
            hiveserver2_conn_id='hiveserver2_default',
            aws_conn_id='aws_default',
            *args, **kwargs):
        super().__init__(*args, **kwargs)
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

    def execute(self, context):
        hive = HiveServer2Hook(hiveserver2_conn_id=self.hiveserver2_conn_id)

        self.log.info('Extracting data from Hive')
        self.log.info(self.sql)

        data = hive.get_pandas_df(self.sql, schema=self.schema)
        dynamodb = AwsDynamoDBHook(aws_conn_id=self.aws_conn_id,
                                   table_name=self.table_name,
                                   table_keys=self.table_keys,
                                   region_name=self.region_name)

        self.log.info('Inserting rows into dynamodb')

        if self.pre_process is None:
            dynamodb.write_batch_data(
                json.loads(data.to_json(orient='records')))
        else:
            dynamodb.write_batch_data(
                self.pre_process(data=data,
                                 args=self.pre_process_args,
                                 kwargs=self.pre_process_kwargs))

        self.log.info('Done.')


def _convert_item_to_json_bytes(item):
    return (json.dumps(item) + '\n').encode('utf-8')


def _upload_file_to_s3(file_obj, bucket_name, s3_key_prefix):
    s3_client = S3Hook().get_conn()
    file_obj.seek(0)
    s3_client.upload_file(
        Filename=file_obj.name,
        Bucket=bucket_name,
        Key=s3_key_prefix + str(uuid4()),
    )


class DynamoDBToS3Operator(BaseOperator):
    """
    Replicates records from a DynamoDB table to S3.
    It scans a DynamoDB table and write the received records to a file
    on the local filesystem. It flushes the file to S3 once the file size
    exceeds the file size limit specified by the user.

    Users can also specify a filtering criteria using dynamodb_scan_kwargs
    to only replicate records that satisfy the criteria.

    To parallelize the replication, users can create multiple tasks of DynamoDBToS3Operator.
    For instance to replicate with parallelism of 2, create two tasks like:

    .. code-block::

        op1 = DynamoDBToS3Operator(
            task_id='replicator-1',
            dynamodb_table_name='hello',
            dynamodb_scan_kwargs={
                'TotalSegments': 2,
                'Segment': 0,
            },
            ...
        )

        op2 = DynamoDBToS3Operator(
            task_id='replicator-2',
            dynamodb_table_name='hello',
            dynamodb_scan_kwargs={
                'TotalSegments': 2,
                'Segment': 1,
            },
            ...
        )

    :param dynamodb_table_name: Dynamodb table to replicate data from
    :param s3_bucket_name: S3 bucket to replicate data to
    :param file_size: Flush file to s3 if file size >= file_size
    :param dynamodb_scan_kwargs: kwargs pass to <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.scan>  # noqa: E501 pylint: disable=line-too-long
    :param s3_key_prefix: Prefix of s3 object key
    :param process_func: How we transforms a dynamodb item to bytes. By default we dump the json
    """

    def __init__(self,
                 dynamodb_table_name: str,
                 s3_bucket_name: str,
                 file_size: int,
                 dynamodb_scan_kwargs: Optional[Dict[str, Any]] = None,
                 s3_key_prefix: str = '',
                 process_func: Callable[[Dict[str, Any]], bytes] = _convert_item_to_json_bytes,
                 *args, **kwargs):
        super(DynamoDBToS3Operator, self).__init__(*args, **kwargs)
        self.file_size = file_size
        self.process_func = process_func
        self.dynamodb_table_name = dynamodb_table_name
        self.dynamodb_scan_kwargs = dynamodb_scan_kwargs
        self.s3_bucket_name = s3_bucket_name
        self.s3_key_prefix = s3_key_prefix

    def execute(self, context):
        table = AwsDynamoDBHook().get_conn().Table(self.dynamodb_table_name)
        scan_kwargs = copy(self.dynamodb_scan_kwargs) if self.dynamodb_scan_kwargs else {}
        err = None
        f = NamedTemporaryFile()
        try:
            f = self._scan_dynamodb_and_upload_to_s3(f, scan_kwargs, table)
        except Exception as e:
            err = e
            raise e
        finally:
            if err is None:
                _upload_file_to_s3(f, self.s3_bucket_name, self.s3_key_prefix)
            f.close()

    def _scan_dynamodb_and_upload_to_s3(self, temp_file, scan_kwargs, table):
        while True:
            response = table.scan(**scan_kwargs)
            items = response['Items']
            for item in items:
                temp_file.write(self.process_func(item))

            if 'LastEvaluatedKey' not in response:
                # no more items to scan
                break

            last_evaluated_key = response['LastEvaluatedKey']
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key

            # Upload the file to S3 if reach file size limit
            if getsize(temp_file.name) >= self.file_size:
                _upload_file_to_s3(temp_file, self.s3_bucket_name,
                                   self.s3_key_prefix)
                temp_file.close()
                temp_file = NamedTemporaryFile()
        return temp_file
