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

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "kinesisanalyticsv2"
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_waiter_details(application_name: str, operation_id: str) -> tuple[str, dict[str, str], list[str]]:
        """
        Set Waiter details for operations start/stop/update applications.

        describe_application_operation API provides more detailed information for starting, updating, or
        stopping the application. However, it requires a minimum of boto3==1.34.134. In this case, we will
        fall back to using the default `describe_application` API to monitor the starting, updating, or
        stopping of the application.

        """
        waiter_name = "application_operation_complete" if operation_id else "application_describe_complete"
        waiter_args = {
            "ApplicationName": application_name,
            **({"OperationId": operation_id} if operation_id else {}),
        }

        status_queries = [
            "ApplicationOperationInfoDetails.OperationStatus"
            if operation_id
            else "ApplicationDetail.ApplicationStatus"
        ]

        return waiter_name, waiter_args, status_queries
