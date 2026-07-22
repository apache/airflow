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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger


class QuickSightIngestionCompletedTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when an Amazon QuickSight SPICE ingestion is complete.

    :param data_set_id: ID of the dataset used in the ingestion.
    :param ingestion_id: ID for the ingestion.
    :param aws_account_id: The ID of the AWS account that owns the dataset.
    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 30)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 60)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates.
    :param botocore_config: Configuration dictionary (key-values) for botocore client.
    """

    def __init__(
        self,
        *,
        data_set_id: str,
        ingestion_id: str,
        aws_account_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
    ) -> None:
        super().__init__(
            serialized_fields={
                "data_set_id": data_set_id,
                "ingestion_id": ingestion_id,
                "aws_account_id": aws_account_id,
            },
            waiter_name="ingestion_complete",
            waiter_args={
                "AwsAccountId": aws_account_id,
                "DataSetId": data_set_id,
                "IngestionId": ingestion_id,
            },
            failure_message="Amazon QuickSight SPICE ingestion failed.",
            status_message="Status of Amazon QuickSight SPICE ingestion is",
            status_queries=["Ingestion.IngestionStatus", "Ingestion.ErrorInfo"],
            return_key="ingestion_id",
            return_value=ingestion_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
            verify=verify,
            botocore_config=botocore_config,
        )

    def hook(self) -> AwsGenericHook:
        return QuickSightHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )
