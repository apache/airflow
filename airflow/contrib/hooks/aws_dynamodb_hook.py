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

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.aws_hook import AwsHook


class AwsDynamoDBHook(AwsHook):
    """
    Interact with AWS DynamoDB.

    :param table_keys: partition key and sort key
    :type table_keys: list
    :param table_name: target DynamoDB table
    :type table_name: str
    :param region_name: aws region name (example: us-east-1)
    :type region_name: str
    """

    def __init__(self,
                 table_keys=None,
                 table_name=None,
                 region_name=None,
                 *args, **kwargs):
        self.table_keys = table_keys
        self.table_name = table_name
        self.region_name = region_name
        super().__init__(*args, **kwargs)

    def get_conn(self):
        self.conn = self.get_resource_type('dynamodb', self.region_name)
        return self.conn

    def write_batch_data(self, items):
        """
        Write batch items to dynamodb table with provisioned throughout capacity.
        """

        dynamodb_conn = self.get_conn()

        try:
            table = dynamodb_conn.Table(self.table_name)

            with table.batch_writer(overwrite_by_pkeys=self.table_keys) as batch:
                for item in items:
                    batch.put_item(Item=item)
            return True
        except Exception as general_error:
            raise AirflowException(
                'Failed to insert items in dynamodb, error: {error}'.format(
                    error=str(general_error)
                )
            )
