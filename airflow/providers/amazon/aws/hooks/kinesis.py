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
"""This module contains AWS Kinesis hooks"""
from __future__ import annotations

from typing import Iterable

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class FirehoseHook(AwsBaseHook):
    """
    Interact with AWS Kinesis Firehose.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

    :param delivery_stream: Name of the delivery stream
    """

    def __init__(self, delivery_stream: str, *args, **kwargs) -> None:
        self.delivery_stream = delivery_stream
        kwargs["client_type"] = "firehose"
        super().__init__(*args, **kwargs)

    def put_records(self, records: Iterable):
        """Write batch records to Kinesis Firehose"""
        return self.get_conn().put_record_batch(DeliveryStreamName=self.delivery_stream, Records=records)


class KinesisHook(AwsBaseHook):
    """
    Interact with AWS Kinesis Data Stream.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    :param delivery_stream: Name of the data stream

    AWS Kinesis docs: https://docs.aws.amazon.com/streams/latest/dev/introduction.html

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, delivery_stream: str, *args, **kwargs) -> None:
        self.delivery_stream = delivery_stream
        kwargs["client_type"] = "kinesis"
        super().__init__(*args, **kwargs)

    def put_records(self, records: list[dict[str, str| bytes]]):
        """
        Write records to Kinesis Data Stream

        Each record may have these keys:
            Data: bytes -
                Data blob of up to 1 MB.
            PartitionKey: str -
                A partition key is used to group data by shard within a stream.
                Partition keys are Unicode strings with a maximum length limit of 256 characters for each key.
            ExplicitHashKey: str (optional) -
                Overrides the partition key to shard mapping.

        AWS Kinesis API reference: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
        """
        return self.get_conn().put_records(StreamName=self.delivery_stream, Records=records)
