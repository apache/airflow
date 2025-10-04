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
"""This module contains AWS Firehose hook."""

from __future__ import annotations

import warnings

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.firehose import FirehoseHook as _FirehoseHook


class FirehoseHook(_FirehoseHook):
    """
    Interact with Amazon Kinesis Firehose.

    Provide thick wrapper around :external+boto3:py:class:`boto3.client("firehose") <Firehose.Client>`.

    :param delivery_stream: Name of the delivery stream

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    .. deprecated::
        This hook was moved. Import from
       :class:`airflow.providers.amazon.aws.hooks.firehose.FirehoseHook`
       instead of kinesis.py
    """

    def __init__(self, *args, **kwargs) -> None:
        warnings.warn(
            "Importing FirehoseHook from kinesis.py is deprecated "
            "and will be removed in a future release. "
            "Please import it from firehose.py instead.",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class KinesisHook(AwsBaseHook):
    """
    Interact with Amazon Kinesis.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("kinesis") <Kinesis.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "kinesis"
        super().__init__(*args, **kwargs)

    def create_stream(self, stream_name: str, shard_count: int) -> dict:
        """
        Create a Kinesis stream.

        .. seealso::
            - :external+boto3:py:meth:`Kinesis.Client.create_stream`

        :param stream_name: Name of the stream to create.
        :param shard_count: Number of shards for the stream.
        :return: Response from the create_stream call.
        """
        return self.get_conn().create_stream(StreamName=stream_name, ShardCount=shard_count)

    def put_record(
        self,
        stream_name: str,
        data: bytes,
        partition_key: str,
        explicit_hash_key: str | None = None,
        sequence_number_for_ordering: str | None = None,
    ) -> dict:
        """
        Put a single record into a Kinesis stream.

        .. seealso::
            - :external+boto3:py:meth:`Kinesis.Client.put_record`

        :param stream_name: Name of the stream to put the record into.
        :param data: The data blob to put into the record, which is base64-encoded when the blob is serialized.
        :param partition_key: Determines which shard in the stream the data record is assigned to.
        :param explicit_hash_key: The hash value used to explicitly determine the shard the data record is assigned to.
        :param sequence_number_for_ordering: Guarantees strictly increasing sequence numbers, for puts from the same client and to the same partition key.
        :return: Response from the put_record call.
        """
        params = {
            "StreamName": stream_name,
            "Data": data,
            "PartitionKey": partition_key,
        }
        if explicit_hash_key:
            params["ExplicitHashKey"] = explicit_hash_key
        if sequence_number_for_ordering:
            params["SequenceNumberForOrdering"] = sequence_number_for_ordering

        return self.get_conn().put_record(**params)

    def put_records(self, stream_name: str, records: list[dict]) -> dict:
        """
        Put multiple records into a Kinesis stream.

        .. seealso::
            - :external+boto3:py:meth:`Kinesis.Client.put_records`

        :param stream_name: Name of the stream to put the records into.
        :param records: List of records to put. Each record should be a dict with 'Data' and 'PartitionKey' keys.
        :return: Response from the put_records call.
        """
        return self.get_conn().put_records(Records=records, StreamName=stream_name)

    def delete_stream(self, stream_name: str, enforce_consumer_deletion: bool = False) -> dict:
        """
        Delete a Kinesis stream.

        .. seealso::
            - :external+boto3:py:meth:`Kinesis.Client.delete_stream`

        :param stream_name: Name of the stream to delete.
        :param enforce_consumer_deletion: If this parameter is set to true, the stream is deleted even if it has registered consumers.
        :return: Response from the delete_stream call.
        """
        return self.get_conn().delete_stream(
            StreamName=stream_name, EnforceConsumerDeletion=enforce_consumer_deletion
        )
