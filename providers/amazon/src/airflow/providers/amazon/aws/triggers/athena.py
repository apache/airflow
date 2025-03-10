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

from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class AthenaTrigger(AwsBaseWaiterTrigger):
    """
    Trigger for AthenaOperator.

    The trigger will asynchronously poll the boto3 API

    :param query_execution_id:  ID of the Athena query execution to watch
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        query_execution_id: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str | None,
        **kwargs,
    ):
        super().__init__(
            serialized_fields={"query_execution_id": query_execution_id},
            waiter_name="query_complete",
            waiter_args={"QueryExecutionId": query_execution_id},
            failure_message=f"Error while waiting for query {query_execution_id} to complete",
            status_message=f"Query execution id: {query_execution_id}",
            status_queries=["QueryExecution.Status"],
            return_value=query_execution_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return AthenaHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )
