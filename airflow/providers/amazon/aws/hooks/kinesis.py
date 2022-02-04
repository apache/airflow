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

"""This module contains AWS Firehose hook"""
import warnings
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
        response = self.get_conn().put_record_batch(DeliveryStreamName=self.delivery_stream, Records=records)

        return response


class AwsFirehoseHook(FirehoseHook):
    """
    This hook is deprecated.
    Please use :class:`airflow.providers.amazon.aws.hooks.kinesis.FirehoseHook`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "This hook is deprecated. "
            "Please use :class:`airflow.providers.amazon.aws.hooks.kinesis.FirehoseHook`.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
