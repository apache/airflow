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


"""
This module contains the AWS DynamoDB hook
"""
from typing import Iterable, List, Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class AwsDynamoDBHook(AwsBaseHook):
    """
    Interact with AWS DynamoDB.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

    :param table_keys: partition key and sort key
    :type table_keys: list
    :param table_name: target DynamoDB table
    :type table_name: str
    """

    def __init__(
        self, *args, table_keys: Optional[List] = None, table_name: Optional[str] = None, **kwargs
    ) -> None:
        self.table_keys = table_keys
        self.table_name = table_name
        kwargs["resource_type"] = "dynamodb"
        super().__init__(*args, **kwargs)

    def write_batch_data(self, items: Iterable) -> bool:
        """
        Write batch items to DynamoDB table with provisioned throughout capacity.
        """
        try:
            table = self.get_conn().Table(self.table_name)

            with table.batch_writer(overwrite_by_pkeys=self.table_keys) as batch:
                for item in items:
                    batch.put_item(Item=item)
            return True
        except Exception as general_error:
            raise AirflowException(
                "Failed to insert items in dynamodb, error: {error}".format(error=str(general_error))
            )
