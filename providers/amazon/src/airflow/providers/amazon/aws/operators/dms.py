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
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.configuration import conf
from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.dms import (
    DmsReplicationCompleteTrigger,
    DmsReplicationConfigDeletedTrigger,
    DmsReplicationDeprovisionedTrigger,
    DmsReplicationStoppedTrigger,
    DmsReplicationTerminalStatusTrigger,
)
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException
from airflow.utils.context import Context

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DmsCreateTaskOperator(AwsBaseOperator[DmsHook]):
    """
    Creates AWS DMS replication task.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsCreateTaskOperator`

    :param replication_task_id: Replication task id
    :param source_endpoint_arn: Source endpoint ARN
    :param target_endpoint_arn: Target endpoint ARN
    :param replication_instance_arn: Replication instance ARN
    :param table_mappings: Table mappings
    :param migration_type: Migration type ('full-load'|'cdc'|'full-load-and-cdc'), full-load by default.
    :param create_task_kwargs: Extra arguments for DMS replication task creation.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields(
        "replication_task_id",
        "source_endpoint_arn",
        "target_endpoint_arn",
        "replication_instance_arn",
        "table_mappings",
        "migration_type",
        "create_task_kwargs",
    )
    template_fields_renderers: ClassVar[dict] = {
        "table_mappings": "json",
        "create_task_kwargs": "json",
    }

    def __init__(
        self,
        *,
        replication_task_id: str,
        source_endpoint_arn: str,
        target_endpoint_arn: str,
        replication_instance_arn: str,
        table_mappings: dict,
        migration_type: str = "full-load",
        create_task_kwargs: dict | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.replication_task_id = replication_task_id
        self.source_endpoint_arn = source_endpoint_arn
        self.target_endpoint_arn = target_endpoint_arn
        self.replication_instance_arn = replication_instance_arn
        self.migration_type = migration_type
        self.table_mappings = table_mappings
        self.create_task_kwargs = create_task_kwargs or {}

    def execute(self, context: Context):
        """
        Create AWS DMS replication task from Airflow.

        :return: replication task arn
        """
        task_arn = self.hook.create_replication_task(
            replication_task_id=self.replication_task_id,
            source_endpoint_arn=self.source_endpoint_arn,
            target_endpoint_arn=self.target_endpoint_arn,
            replication_instance_arn=self.replication_instance_arn,
            migration_type=self.migration_type,
            table_mappings=self.table_mappings,
            **self.create_task_kwargs,
        )
        self.log.info("DMS replication task(%s) is ready.", self.replication_task_id)

        return task_arn


class DmsDeleteTaskOperator(AwsBaseOperator[DmsHook]):
    """
    Deletes AWS DMS replication task.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsDeleteTaskOperator`

    :param replication_task_arn: Replication task ARN
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields("replication_task_arn")

    def __init__(self, *, replication_task_arn: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.replication_task_arn = replication_task_arn

    def execute(self, context: Context):
        """
        Delete AWS DMS replication task from Airflow.

        :return: replication task arn
        """
        self.hook.delete_replication_task(replication_task_arn=self.replication_task_arn)
        self.log.info("DMS replication task(%s) has been deleted.", self.replication_task_arn)


class DmsDescribeTasksOperator(AwsBaseOperator[DmsHook]):
    """
    Describes AWS DMS replication tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsDescribeTasksOperator`

    :param describe_tasks_kwargs: Describe tasks command arguments
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields("describe_tasks_kwargs")
    template_fields_renderers: ClassVar[dict[str, str]] = {"describe_tasks_kwargs": "json"}

    def __init__(self, *, describe_tasks_kwargs: dict | None = None, **kwargs):
        super().__init__(**kwargs)
        self.describe_tasks_kwargs = describe_tasks_kwargs or {}

    def execute(self, context: Context) -> tuple[str | None, list]:
        """
        Describe AWS DMS replication tasks from Airflow.

        :return: Marker and list of replication tasks
        """
        return self.hook.describe_replication_tasks(**self.describe_tasks_kwargs)


class DmsStartTaskOperator(AwsBaseOperator[DmsHook]):
    """
    Starts AWS DMS replication task.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsStartTaskOperator`

    :param replication_task_arn: Replication task ARN
    :param start_replication_task_type: Replication task start type (default='start-replication')
        ('start-replication'|'resume-processing'|'reload-target')
    :param start_task_kwargs: Extra start replication task arguments
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields(
        "replication_task_arn",
        "start_replication_task_type",
        "start_task_kwargs",
    )
    template_fields_renderers = {"start_task_kwargs": "json"}

    def __init__(
        self,
        *,
        replication_task_arn: str,
        start_replication_task_type: str = "start-replication",
        start_task_kwargs: dict | None = None,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.replication_task_arn = replication_task_arn
        self.start_replication_task_type = start_replication_task_type
        self.start_task_kwargs = start_task_kwargs or {}
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context):
        """Start AWS DMS replication task from Airflow."""
        self.hook.start_replication_task(
            replication_task_arn=self.replication_task_arn,
            start_replication_task_type=self.start_replication_task_type,
            **self.start_task_kwargs,
        )
        self.log.info("DMS replication task(%s) is starting.", self.replication_task_arn)


class DmsStopTaskOperator(AwsBaseOperator[DmsHook]):
    """
    Stops AWS DMS replication task.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsStopTaskOperator`

    :param replication_task_arn: Replication task ARN
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields("replication_task_arn")

    def __init__(self, *, replication_task_arn: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.replication_task_arn = replication_task_arn

    def execute(self, context: Context):
        """Stop AWS DMS replication task from Airflow."""
        self.hook.stop_replication_task(replication_task_arn=self.replication_task_arn)
        self.log.info("DMS replication task(%s) is stopping.", self.replication_task_arn)


class DmsDescribeReplicationConfigsOperator(AwsBaseOperator[DmsHook]):
    """
    Describes AWS DMS Serverless replication configurations.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsDescribeReplicationConfigsOperator`

    :param describe_config_filter: Filters block for filtering results.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
    """

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields("filter")
    template_fields_renderers = {"filter": "json"}

    def __init__(
        self,
        *,
        filter: list[dict] | None = None,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.filter = filter

    def execute(self, context: Context) -> list:
        """
        Describe AWS DMS replication configurations.

        :return: List of replication configurations
        """
        return self.hook.describe_replication_configs(filters=self.filter)


class DmsCreateReplicationConfigOperator(AwsBaseOperator[DmsHook]):
    """
    Creates an AWS DMS Serverless replication configuration.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsCreateReplicationConfigOperator`

    :param replication_config_id: Unique identifier used to create a ReplicationConfigArn.
    :param source_endpoint_arn: ARN of the source endpoint
    :param target_endpoint_arn: ARN of the target endpoint
    :param compute_config: Parameters for provisioning an DMS Serverless replication.
    :param replication_type: type of DMS Serverless replication
    :param table_mappings: JSON table mappings
    :param tags: Key-value tag pairs
    :param additional_config_kwargs: Additional configuration parameters for DMS Serverless replication. Passed directly to the API
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
    """

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields(
        "replication_config_id",
        "source_endpoint_arn",
        "target_endpoint_arn",
        "compute_config",
        "replication_type",
        "table_mappings",
    )

    template_fields_renderers = {"compute_config": "json", "tableMappings": "json"}

    def __init__(
        self,
        *,
        replication_config_id: str,
        source_endpoint_arn: str,
        target_endpoint_arn: str,
        compute_config: dict[str, Any],
        replication_type: str,
        table_mappings: str,
        additional_config_kwargs: dict | None = None,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(
            aws_conn_id=aws_conn_id,
            **kwargs,
        )

        self.replication_config_id = replication_config_id
        self.source_endpoint_arn = source_endpoint_arn
        self.target_endpoint_arn = target_endpoint_arn
        self.compute_config = compute_config
        self.replication_type = replication_type
        self.table_mappings = table_mappings
        self.additional_config_kwargs = additional_config_kwargs or {}

    def execute(self, context: Context) -> str:
        resp = self.hook.create_replication_config(
            replication_config_id=self.replication_config_id,
            source_endpoint_arn=self.source_endpoint_arn,
            target_endpoint_arn=self.target_endpoint_arn,
            compute_config=self.compute_config,
            replication_type=self.replication_type,
            table_mappings=self.table_mappings,
            additional_config_kwargs=self.additional_config_kwargs,
        )

        self.log.info("DMS replication config(%s) has been created.", self.replication_config_id)
        return resp


class DmsDeleteReplicationConfigOperator(AwsBaseOperator[DmsHook]):
    """
    Deletes an AWS DMS Serverless replication configuration.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsDeleteReplicationConfigOperator`

    :param replication_config_arn: ARN of the replication config
    :param wait_for_completion: If True, waits for the replication config to be deleted before returning.
        If False, the operator will return immediately after the request is made.
    :param deferrable: Run the operator in deferrable mode.
    :param waiter_delay: The number of seconds to wait between retries (default: 60).
    :param waiter_max_attempts: The maximum number of attempts to be made (default: 60).
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
    """

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields("replication_config_arn")

    VALID_STATES = ["failed", "stopped", "created"]
    DELETING_STATES = ["deleting"]
    TERMINAL_PROVISION_STATES = ["deprovisioned", ""]

    def __init__(
        self,
        *,
        replication_config_arn: str,
        wait_for_completion: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        waiter_delay: int = 60,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(
            aws_conn_id=aws_conn_id,
            **kwargs,
        )

        self.replication_config_arn = replication_config_arn
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def execute(self, context: Context) -> None:
        results = self.hook.describe_replications(
            filters=[{"Name": "replication-config-arn", "Values": [self.replication_config_arn]}]
        )

        current_state = results[0].get("Status", "")
        self.log.info(
            "Current state of replication config(%s) is %s.", self.replication_config_arn, current_state
        )
        # replication must be deprovisioned before deleting
        provision_status = self.hook.get_provision_status(replication_config_arn=self.replication_config_arn)

        if self.deferrable:
            if current_state.lower() not in self.VALID_STATES:
                self.log.info("Deferring until terminal status reached.")
                self.defer(
                    trigger=DmsReplicationTerminalStatusTrigger(
                        replication_config_arn=self.replication_config_arn,
                        waiter_delay=self.waiter_delay,
                        waiter_max_attempts=self.waiter_max_attempts,
                        aws_conn_id=self.aws_conn_id,
                    ),
                    method_name="retry_execution",
                )
            if provision_status not in self.TERMINAL_PROVISION_STATES:  # not deprovisioned:
                self.log.info("Deferring until deprovisioning completes.")
                self.defer(
                    trigger=DmsReplicationDeprovisionedTrigger(
                        replication_config_arn=self.replication_config_arn,
                        waiter_delay=self.waiter_delay,
                        waiter_max_attempts=self.waiter_max_attempts,
                        aws_conn_id=self.aws_conn_id,
                    ),
                    method_name="retry_execution",
                )

        self.hook.get_waiter("replication_terminal_status").wait(
            Filters=[{"Name": "replication-config-arn", "Values": [self.replication_config_arn]}],
            WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
        )
        self.hook.delete_replication_config(self.replication_config_arn)
        self.handle_delete_wait()

    def handle_delete_wait(self):
        if self.wait_for_completion:
            if self.deferrable:
                self.log.info("Deferring until replication config is deleted.")
                self.defer(
                    trigger=DmsReplicationConfigDeletedTrigger(
                        replication_config_arn=self.replication_config_arn,
                        waiter_delay=self.waiter_delay,
                        waiter_max_attempts=self.waiter_max_attempts,
                        aws_conn_id=self.aws_conn_id,
                    ),
                    method_name="execute_complete",
                )
            else:
                self.hook.get_waiter("replication_config_deleted").wait(
                    Filters=[{"Name": "replication-config-arn", "Values": [self.replication_config_arn]}],
                    WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
                )
                self.log.info("DMS replication config(%s) deleted.", self.replication_config_arn)

    def execute_complete(self, context, event=None):
        self.replication_config_arn = event.get("replication_config_arn")
        self.log.info("DMS replication config(%s) deleted.", self.replication_config_arn)

    def retry_execution(self, context, event=None):
        self.replication_config_arn = event.get("replication_config_arn")
        self.log.info("Retrying replication config(%s) deletion.", self.replication_config_arn)
        self.execute(context)


class DmsDescribeReplicationsOperator(AwsBaseOperator[DmsHook]):
    """
    Describes AWS DMS Serverless replications.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsDescribeReplicationsOperator`

    :param filter: Filters block for filtering results.

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
    """

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields("filter")
    template_fields_renderer = {"filter": "json"}

    def __init__(
        self,
        *,
        filter: list[dict[str, Any]] | None = None,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(
            aws_conn_id=aws_conn_id,
            **kwargs,
        )

        self.filter = filter

    def execute(self, context: Context) -> list[dict[str, Any]]:
        """
        Describe AWS DMS replications.

        :return: Replications
        """
        return self.hook.describe_replications(self.filter)


class DmsStartReplicationOperator(AwsBaseOperator[DmsHook]):
    """
    Starts an AWS DMS Serverless replication.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsStartReplicationOperator`

    :param replication_config_arn: ARN of the replication config
    :param replication_start_type: Type of replication.
    :param cdc_start_time: Start time of CDC
    :param cdc_start_pos: Indicates when to start CDC.
    :param cdc_stop_pos: Indicates when to stop CDC.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
    """

    RUNNING_STATES = ["running"]
    STARTABLE_STATES = ["stopped", "failed", "created"]
    TERMINAL_STATES = ["failed", "stopped", "created"]
    TERMINAL_PROVISION_STATES = ["deprovisioned", ""]

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields(
        "replication_config_arn", "replication_start_type", "cdc_start_time", "cdc_start_pos", "cdc_stop_pos"
    )

    def __init__(
        self,
        *,
        replication_config_arn: str,
        replication_start_type: str,
        cdc_start_time: datetime | str | None = None,
        cdc_start_pos: str | None = None,
        cdc_stop_pos: str | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(
            aws_conn_id=aws_conn_id,
            **kwargs,
        )

        self.replication_config_arn = replication_config_arn
        self.replication_start_type = replication_start_type
        self.cdc_start_time = cdc_start_time
        self.cdc_start_pos = cdc_start_pos
        self.cdc_stop_pos = cdc_stop_pos
        self.deferrable = deferrable
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.wait_for_completion = wait_for_completion

        if self.cdc_start_time and self.cdc_start_pos:
            raise AirflowException("Only one of cdc_start_time or cdc_start_pos should be provided.")

    def execute(self, context: Context):
        result = self.hook.describe_replications(
            filters=[{"Name": "replication-config-arn", "Values": [self.replication_config_arn]}]
        )

        current_status = result[0].get("Status", "")
        provision_status = self.hook.get_provision_status(replication_config_arn=self.replication_config_arn)

        if provision_status == "deprovisioning":
            # wait for deprovisioning to complete before start/restart
            self.log.info(
                "Replication is deprovisioning. Must wait for deprovisioning before running replication"
            )
            if self.deferrable:
                self.log.info("Deferring until deprovisioning completes.")
                self.defer(
                    trigger=DmsReplicationDeprovisionedTrigger(
                        replication_config_arn=self.replication_config_arn,
                        waiter_delay=self.waiter_delay,
                        waiter_max_attempts=self.waiter_max_attempts,
                        aws_conn_id=self.aws_conn_id,
                    ),
                    method_name="retry_execution",
                )
            else:
                self.hook.get_waiter("replication_deprovisioned").wait(
                    Filters=[{"Name": "replication-config-arn", "Values": [self.replication_config_arn]}],
                    WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
                )
                provision_status = self.hook.get_provision_status(
                    replication_config_arn=self.replication_config_arn
                )
                self.log.info("Replication deprovisioning complete. Provision status: %s", provision_status)

        if (
            current_status.lower() in self.STARTABLE_STATES
            and provision_status in self.TERMINAL_PROVISION_STATES
        ):
            resp = self.hook.start_replication(
                replication_config_arn=self.replication_config_arn,
                start_replication_type=self.replication_start_type,
                cdc_start_time=self.cdc_start_time,
                cdc_start_pos=self.cdc_start_pos,
                cdc_stop_pos=self.cdc_stop_pos,
            )

            current_status = resp.get("Replication", {}).get("Status", "Unknown")
            self.log.info(
                "Replication(%s) started with status %s.",
                self.replication_config_arn,
                current_status,
            )

            if self.wait_for_completion:
                self.log.info("Waiting for %s replication to complete.", self.replication_config_arn)

                if self.deferrable:
                    self.log.info("Deferring until %s replication completes.", self.replication_config_arn)
                    self.defer(
                        trigger=DmsReplicationCompleteTrigger(
                            replication_config_arn=self.replication_config_arn,
                            waiter_delay=self.waiter_delay,
                            waiter_max_attempts=self.waiter_max_attempts,
                            aws_conn_id=self.aws_conn_id,
                        ),
                        method_name="execute_complete",
                    )

                self.hook.get_waiter("replication_complete").wait(
                    Filters=[{"Name": "replication-config-arn", "Values": [self.replication_config_arn]}],
                    WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
                )
                self.log.info("Replication(%s) has completed.", self.replication_config_arn)

        else:
            self.log.info("Replication(%s) is not in startable state.", self.replication_config_arn)
            self.log.info("Status: %s Provision status: %s", current_status, provision_status)

    def execute_complete(self, context, event=None):
        self.replication_config_arn = event.get("replication_config_arn")
        self.log.info("Replication(%s) has completed.", self.replication_config_arn)

    def retry_execution(self, context, event=None):
        self.replication_config_arn = event.get("replication_config_arn")
        self.log.info("Retrying replication %s.", self.replication_config_arn)
        self.execute(context)


class DmsStopReplicationOperator(AwsBaseOperator[DmsHook]):
    """
    Stops an AWS DMS Serverless replication.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsStopReplicationOperator`

    :param replication_config_arn: ARN of the replication config
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
    """

    STOPPED_STATES = ["stopped"]
    NON_STOPPABLE_STATES = ["stopped"]

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields("replication_config_arn")

    def __init__(
        self,
        *,
        replication_config_arn: str,
        wait_for_completion: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(
            aws_conn_id=aws_conn_id,
            **kwargs,
        )

        self.replication_config_arn = replication_config_arn
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute(self, context: Context) -> None:
        results = self.hook.describe_replications(
            filters=[{"Name": "replication-config-arn", "Values": [self.replication_config_arn]}]
        )

        current_state = results[0].get("Status", "")
        self.log.info(
            "Current state of replication config(%s) is %s.", self.replication_config_arn, current_state
        )

        if current_state.lower() in self.STOPPED_STATES:
            self.log.info("DMS replication config(%s) is already stopped.", self.replication_config_arn)
        else:
            resp = self.hook.stop_replication(self.replication_config_arn)
            status = resp.get("Replication", {}).get("Status", "Unknown")
            self.log.info(
                "Stopping DMS replication config(%s). Current status: %s", self.replication_config_arn, status
            )

            if self.wait_for_completion:
                self.log.info("Waiting for %s replication to stop.", self.replication_config_arn)

                if self.deferrable:
                    self.log.info("Deferring until %s replication stops.", self.replication_config_arn)
                    self.defer(
                        trigger=DmsReplicationStoppedTrigger(
                            replication_config_arn=self.replication_config_arn,
                            waiter_delay=self.waiter_delay,
                            waiter_max_attempts=self.waiter_max_attempts,
                            aws_conn_id=self.aws_conn_id,
                        ),
                        method_name="execute_complete",
                    )
                self.hook.get_waiter("replication_stopped").wait(
                    Filters=[{"Name": "replication-config-arn", "Values": [self.replication_config_arn]}],
                    WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
                )

    def execute_complete(self, context, event=None):
        self.replication_config_arn = event.get("replication_config_arn")
        self.log.info("Replication(%s) has stopped.", self.replication_config_arn)
