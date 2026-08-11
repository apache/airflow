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

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger
from airflow.providers.amazon.aws.utils.waiter_with_logging import async_wait
from airflow.triggers.base import BaseTrigger, TriggerEvent

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
            status_queries=["Replications[0].Status", "Replications[0].FailureMessages"],
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


class DmsTaskModifyCompleteTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when a DMS classic replication task modification completes.

    :param replication_task_arn: The ARN of the replication task.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param verify: Whether or not to verify SSL certificates.
    :param botocore_config: Configuration dictionary (key-values) for botocore client.
    """

    def __init__(
        self,
        replication_task_arn: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
    ) -> None:
        super().__init__(
            serialized_fields={"replication_task_arn": replication_task_arn},
            waiter_name="replication_task_modified",
            waiter_delay=waiter_delay,
            waiter_args={
                "Filters": [{"Name": "replication-task-arn", "Values": [replication_task_arn]}],
                "WithoutSettings": True,
            },
            waiter_max_attempts=waiter_max_attempts,
            failure_message="Replication task modification failed to complete.",
            status_message="Status replication task is",
            status_queries=["ReplicationTasks[0].Status"],
            return_key="replication_task_arn",
            return_value=replication_task_arn,
            aws_conn_id=aws_conn_id,
            verify=verify,
            botocore_config=botocore_config,
        )

    def hook(self) -> AwsGenericHook:
        return DmsHook(
            self.aws_conn_id,
            verify=self.verify,
            config=self.botocore_config,
        )


class DmsTableReloadCompleteTrigger(BaseTrigger):
    """
    Trigger when AWS DMS finishes reloading or validating a set of tables.

    :param replication_task_arn: The ARN of the replication task.
    :param tables_to_reload: Tables being reloaded, including schema and table names.
    :param reload_option: The reload operation whose completion state should be monitored.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region name.
    :param verify: Whether or not to verify SSL certificates.
    :param botocore_config: Configuration dictionary (key-values) for botocore client.
    """

    def __init__(
        self,
        *,
        replication_task_arn: str,
        tables_to_reload: list[dict[str, str]],
        reload_option: str = "data-reload",
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
    ) -> None:
        super().__init__()
        self.replication_task_arn = replication_task_arn
        self.tables_to_reload = tables_to_reload
        self.reload_option = reload_option
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "replication_task_arn": self.replication_task_arn,
                "tables_to_reload": self.tables_to_reload,
                "reload_option": self.reload_option,
                "waiter_delay": self.waiter_delay,
                "waiter_max_attempts": self.waiter_max_attempts,
                "aws_conn_id": self.aws_conn_id,
                "region_name": self.region_name,
                "verify": self.verify,
                "botocore_config": self.botocore_config,
            },
        )

    def _build_waiter_args(self, table: dict[str, str]) -> dict[str, Any]:
        return {
            "ReplicationTaskArn": self.replication_task_arn,
            "Filters": [
                {"Name": "schema-name", "Values": [table["SchemaName"]]},
                {"Name": "table-name", "Values": [table["TableName"]]},
            ],
        }

    def _get_waiter_config(self) -> tuple[str, str, str]:
        if self.reload_option == "validate-only":
            return (
                "table_validation_complete",
                "validation",
                "TableStatistics[0].ValidationState",
            )
        return (
            "table_reload_complete",
            "reload",
            "TableStatistics[0].TableState",
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll table statistics until all requested operations finish."""
        hook = DmsHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )

        try:
            async with await hook.get_async_conn() as client:
                waiter_name, operation_name, status_query = self._get_waiter_config()
                for table in self.tables_to_reload:
                    waiter = hook.get_waiter(
                        waiter_name,
                        deferrable=True,
                        client=client,
                    )
                    table_name = f"{table['SchemaName']}.{table['TableName']}"
                    await async_wait(
                        waiter,
                        self.waiter_delay,
                        self.waiter_max_attempts,
                        self._build_waiter_args(table),
                        f"DMS table {operation_name} failed for {table_name}.",
                        f"Status of DMS table {operation_name} {table_name} is",
                        [status_query],
                    )
        except AirflowException as error:
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": str(error),
                    "replication_task_arn": self.replication_task_arn,
                }
            )
        else:
            yield TriggerEvent(
                {
                    "status": "success",
                    "replication_task_arn": self.replication_task_arn,
                }
            )
