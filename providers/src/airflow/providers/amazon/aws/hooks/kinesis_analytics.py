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
from __future__ import annotations

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class KinesisAnalyticsV2Hook(AwsBaseHook):
    """
    Interact with Amazon Kinesis Analytics V2.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("kinesisanalyticsv2") <KinesisAnalyticsV2.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    APPLICATION_START_INTERMEDIATE_STATES: tuple[str, ...] = (
        "STARTING",
        "UPDATING",
        "AUTOSCALING",
    )
    APPLICATION_START_FAILURE_STATES: tuple[str, ...] = (
        "DELETING",
        "STOPPING",
        "READY",
        "FORCE_STOPPING",
        "ROLLING_BACK",
        "MAINTENANCE",
        "ROLLED_BACK",
    )
    APPLICATION_START_SUCCESS_STATES: tuple[str, ...] = ("RUNNING",)

    APPLICATION_STOP_INTERMEDIATE_STATES: tuple[str, ...] = (
        "STARTING",
        "UPDATING",
        "AUTOSCALING",
        "RUNNING",
        "STOPPING",
        "FORCE_STOPPING",
    )
    APPLICATION_STOP_FAILURE_STATES: tuple[str, ...] = (
        "DELETING",
        "ROLLING_BACK",
        "MAINTENANCE",
        "ROLLED_BACK",
    )
    APPLICATION_STOP_SUCCESS_STATES: tuple[str, ...] = ("READY",)

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "kinesisanalyticsv2"
        super().__init__(*args, **kwargs)
