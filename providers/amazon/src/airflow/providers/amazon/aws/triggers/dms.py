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

from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class DmsReplicationTerminalStatusTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when an AWS DMS Serverless replication is in a terminal state.

    :param replication_config_arn: The ARN of the replication config.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        replication_config_arn: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
    ) -> None:
        super().__init__(
            serialized_fields={"replication_config_arn": replication_config_arn},
            waiter_name="replication_terminal_status",
            waiter_delay=waiter_delay,
            waiter_args={"Filters": [{"Name": "replication-config-arn", "Values": [replication_config_arn]}]},
            waiter_max_attempts=waiter_max_attempts,
            failure_message="Replication failed to reach terminal status.",
            status_message="Status replication is",
            status_queries=["Replications[0].Status"],
            return_key="replication_config_arn",
            return_value=replication_config_arn,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return DmsHook(
            self.aws_conn_id,
            verify=self.verify,
            config=self.botocore_config,
        )


class DmsReplicationConfigDeletedTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when an AWS DMS Serverless replication config is deleted.

    :param replication_config_arn: The ARN of the replication config.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        replication_config_arn: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
    ) -> None:
        super().__init__(
            serialized_fields={"replication_config_arn": replication_config_arn},
            waiter_name="replication_config_deleted",
            waiter_delay=waiter_delay,
            waiter_args={"Filters": [{"Name": "replication-config-arn", "Values": [replication_config_arn]}]},
            waiter_max_attempts=waiter_max_attempts,
            failure_message="Replication config failed to be deleted.",
            status_message="Status replication config is",
            status_queries=["ReplicationConfigs[0].Status"],
            return_key="replication_config_arn",
            return_value=replication_config_arn,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return DmsHook(
            self.aws_conn_id,
            verify=self.verify,
            config=self.botocore_config,
        )


class DmsReplicationCompleteTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when an AWS DMS Serverless replication completes.

    :param replication_config_arn: The ARN of the replication config.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        replication_config_arn: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
    ) -> None:
        super().__init__(
            serialized_fields={"replication_config_arn": replication_config_arn},
            waiter_name="replication_complete",
            waiter_delay=waiter_delay,
            waiter_args={"Filters": [{"Name": "replication-config-arn", "Values": [replication_config_arn]}]},
            waiter_max_attempts=waiter_max_attempts,
            failure_message="Replication failed to complete.",
            status_message="Status replication is",
            status_queries=["Replications[0].Status"],
            return_key="replication_config_arn",
            return_value=replication_config_arn,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return DmsHook(
            self.aws_conn_id,
            verify=self.verify,
            config=self.botocore_config,
        )


class DmsReplicationStoppedTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when an AWS DMS Serverless replication is stopped.

    :param replication_config_arn: The ARN of the replication config.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        replication_config_arn: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
    ) -> None:
        super().__init__(
            serialized_fields={"replication_config_arn": replication_config_arn},
            waiter_name="replication_stopped",
            waiter_delay=waiter_delay,
            waiter_args={"Filters": [{"Name": "replication-config-arn", "Values": [replication_config_arn]}]},
            waiter_max_attempts=waiter_max_attempts,
            failure_message="Replication failed to stop.",
            status_message="Status replication is",
            status_queries=["Replications[0].Status"],
            return_key="replication_config_arn",
            return_value=replication_config_arn,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return DmsHook(
            self.aws_conn_id,
            verify=self.verify,
            config=self.botocore_config,
        )


class DmsReplicationDeprovisionedTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when an AWS DMS Serverless replication is de-provisioned.

    :param replication_config_arn: The ARN of the replication config.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        replication_config_arn: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
    ) -> None:
        super().__init__(
            serialized_fields={"replication_config_arn": replication_config_arn},
            waiter_name="replication_deprovisioned",
            waiter_delay=waiter_delay,
            waiter_args={"Filters": [{"Name": "replication-config-arn", "Values": [replication_config_arn]}]},
            waiter_max_attempts=waiter_max_attempts,
            failure_message="Replication failed to deprovision.",
            status_message="Status replication is",
            status_queries=["Replications[0].ProvisionData.ProvisionState"],
            return_key="replication_config_arn",
            return_value=replication_config_arn,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return DmsHook(
            self.aws_conn_id,
            verify=self.verify,
            config=self.botocore_config,
        )
