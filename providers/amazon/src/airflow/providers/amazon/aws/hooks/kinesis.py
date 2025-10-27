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
