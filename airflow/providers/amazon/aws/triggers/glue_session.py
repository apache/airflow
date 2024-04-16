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

from airflow.providers.amazon.aws.hooks.glue_session import GlueSessionHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class GlueSessionReadyTrigger(AwsBaseWaiterTrigger):
    """
    Watches for a glue session, triggers when it is ready.

    :param session_id: glue session id
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        session_id: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str | None,
        region_name: str | None = None,
        **kwargs,
    ):
        super().__init__(
            serialized_fields={"session_id": session_id},
            waiter_name="session_ready",
            waiter_args={"Id": session_id},
            failure_message="Failure while waiting for session to be ready",
            status_message="Session is not ready yet",
            status_queries=["Session.Status", "failures"],
            return_key="id",
            return_value=session_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return GlueSessionHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
