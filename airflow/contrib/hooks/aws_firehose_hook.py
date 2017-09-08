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

from airflow.contrib.hooks.aws_hook import AwsHook


class AwsFirehoseHook(AwsHook):
    """
    Interact with AWS Kinesis Firehose.
    """

    def __init__(self, delivery_stream, *args, **kwargs):
        self.delivery_stream = delivery_stream
        super(AwsFirehoseHook, self).__init__(*args, **kwargs)

    def get_conn(self):
        """
        Returns AwsHook connection object.
        """

        self.conn = self.get_client_type('firehose')
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
