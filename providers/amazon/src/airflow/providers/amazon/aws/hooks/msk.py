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
"""This module contains Amazon Managed Streaming for Apache Kafka hook."""

from __future__ import annotations

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class MskHook(AwsBaseHook):
    """
    Create an authenticated Amazon Managed Streaming for Apache Kafka client.

    Additional arguments, such as ``aws_conn_id`` or ``region_name``, are passed
    to :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`. Use
    ``conn`` or ``get_conn()`` to access the underlying ``boto3.client("kafka")``
    directly.
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "kafka"
        super().__init__(*args, **kwargs)
