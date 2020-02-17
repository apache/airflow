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
This module contains AWS Firehose hook
"""
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class AwsFirehoseHook(AwsBaseHook):
    """
    Interact with AWS Kinesis Firehose.

    :param delivery_stream: Name of the delivery stream
    :type delivery_stream: str
    :param region_name: AWS region name (example: us-east-1)
    :type region_name: str
    """

    def __init__(self, delivery_stream, region_name=None, *args, **kwargs):
        self.delivery_stream = delivery_stream
        self.region_name = region_name
        self.conn = None
        super().__init__(*args, **kwargs)

    def get_conn(self):
        """
        Returns AWS connection object.
        """

        self.conn = self.get_client_type('firehose', self.region_name)
        return self.conn

    def put_records(self, records):
        """
        Write batch records to Kinesis Firehose
        """

        firehose_conn = self.get_conn()

        response = firehose_conn.put_record_batch(
            DeliveryStreamName=self.delivery_stream,
            Records=records
        )

        return response
