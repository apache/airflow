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
from typing import Any, ClassVar

from airflow.providers.amazon.aws.hooks.dms import DmsHook, DmsTaskWaiterStatus
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.dms import (
    DmsReplicationCompleteTrigger,
    DmsReplicationConfigDeletedTrigger,
    DmsReplicationDeprovisionedTrigger,
    DmsReplicationStoppedTrigger,
    DmsReplicationTerminalStatusTrigger,
    DmsTaskStoppedTrigger,
)
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException, Context, conf


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


class DmsModifyTaskOperator(AwsBaseOperator[DmsHook]):
    """
    Modifies an existing AWS DMS replication task.

    If the task is not already stopped, set ``stop_task_before=True`` to stop it first.
    After modification, set ``restart_task_after=True`` to restart it automatically.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DmsModifyTaskOperator`

    :param replication_task_arn: Replication task ARN
    :param table_mappings: New table mappings. If not provided, existing mappings are kept.
    :param migration_type: Migration type ('full-load'|'cdc'|'full-load-and-cdc').
        If not provided, existing type is kept.
    :param replication_task_settings: Task settings dict. If not provided, existing settings are kept.
    :param cdc_start_time: Start time for CDC.
    :param cdc_start_position: Indicates when to start CDC (checkpoint or LSN/SCN format).
        Mutually exclusive with cdc_start_time.
    :param cdc_stop_position: Indicates when to stop CDC.
    :param stop_task_before: If True, stop the task before modifying if it is not already stopped.
    :param restart_task_after: If True, restart the task after modifying.
    :param start_replication_task_type: Start type used when restarting the task.
        One of 'start-replication', 'resume-processing', or 'reload-target'.
        Defaults to 'resume-processing'. Only used when ``restart_task_after=True``.
    :param wait_for_completion: If True, wait for the task to be stopped before modifying
        when the task is not already in a stopped state. Requires ``stop_task_before=True``
        or the task to already be stopping. If False, raises if the task is not stopped.
    :param deferrable: Run the operator in deferrable mode.
    :param waiter_delay: Seconds between waiter polls (default: 30).
    :param waiter_max_attempts: Maximum waiter poll attempts (default: 60).
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

    STOPPED_STATES = ("stopped", "ready", "failed", "created")

    aws_hook_class = DmsHook
    template_fields: Sequence[str] = aws_template_fields(
        "replication_task_arn",
        "table_mappings",
        "migration_type",
        "replication_task_settings",
        "cdc_start_time",
        "cdc_start_position",
        "cdc_stop_position",
        "start_replication_task_type",
    )
    template_fields_renderers: ClassVar[dict] = {
        "table_mappings": "json",
        "replication_task_settings": "json",
    }

    def __init__(
        self,
        *,
        replication_task_arn: str,
        table_mappings: dict | None = None,
        migration_type: str | None = None,
        replication_task_settings: dict | None = None,
        cdc_start_time: datetime | None = None,
        cdc_start_position: str | None = None,
        cdc_stop_position: str | None = None,
        stop_task_before: bool = False,
        restart_task_after: bool = True,
        start_replication_task_type: str = "resume-processing",
        wait_for_completion: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if cdc_start_time and cdc_start_position:
            raise ValueError("Only one of cdc_start_time or cdc_start_position can be provided.")
        self.replication_task_arn = replication_task_arn
        self.table_mappings = table_mappings
        self.migration_type = migration_type
        self.replication_task_settings = replication_task_settings
        self.cdc_start_time = cdc_start_time
        self.cdc_start_position = cdc_start_position
        self.cdc_stop_position = cdc_stop_position
        self.stop_task_before = stop_task_before
        self.restart_task_after = restart_task_after
        self.start_replication_task_type = start_replication_task_type
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def execute(self, context: Context) -> dict:
        tasks = self.hook.find_replication_tasks_by_arn(
            replication_task_arn=self.replication_task_arn, without_settings=True
        )
        if not tasks:
            raise AirflowException(f"Replication task {self.replication_task_arn} not found.")

        current_status = tasks[0].get("Status", "").lower()
        self.log.info(
            "Current status of replication task(%s) is '%s'.", self.replication_task_arn, current_status
        )

        if current_status == "modifying":
            # boto3 stopped/ready waiters treat 'modifying' as a terminal failure — use poll loop.
            if not self.wait_for_completion:
                raise AirflowException(
                    f"Replication task {self.replication_task_arn} is already being modified. "
                    f"Set wait_for_completion=True to wait for it to finish."
                )
            self._wait_until_not_modifying()
        elif current_status not in self.STOPPED_STATES:
            if not self.stop_task_before:
                raise AirflowException(
                    f"Replication task {self.replication_task_arn} is in state '{current_status}' "
                    f"and must be stopped before modification. Set stop_task_before=True to stop it "
                    f"automatically, or stop the task first."
                )
            if current_status == "starting":
                # DMS rejects StopReplicationTask while the task is still starting up.
                self.log.info(
                    "Replication task(%s) is starting, waiting for it to reach 'running'.",
                    self.replication_task_arn,
                )
                self._wait_for_status("running")
            self.log.info("Stopping replication task(%s).", self.replication_task_arn)
            self.hook.stop_replication_task(replication_task_arn=self.replication_task_arn)
            self.log.info("Waiting for replication task(%s) to stop.", self.replication_task_arn)
            if self.deferrable:
                self.defer(
                    trigger=DmsTaskStoppedTrigger(
                        replication_task_arn=self.replication_task_arn,
                        waiter_delay=self.waiter_delay,
                        waiter_max_attempts=self.waiter_max_attempts,
                        aws_conn_id=self.aws_conn_id,
                    ),
                    method_name="execute_complete",
                )
            self.hook.wait_for_task_status(
                replication_task_arn=self.replication_task_arn,
                status=DmsTaskWaiterStatus.STOPPED,
            )

        return self._do_modify()

    def execute_complete(self, context: Context, event: dict | None = None) -> dict:
        validated_event = validate_execute_complete_event(event)
        if validated_event["status"] != "success":
            raise AirflowException(f"Error waiting for replication task to stop: {validated_event}")
        self.replication_task_arn = validated_event["replication_task_arn"]
        return self._do_modify()

    def _wait_until_not_modifying(self) -> None:
        """
        Poll until the task leaves the 'modifying' state.

        boto3 waiters for replication_task_stopped and replication_task_ready both
        treat 'modifying' as a terminal failure, so a polling loop is necessary.
        """
        import time

        for _ in range(self.waiter_max_attempts):
            tasks = self.hook.find_replication_tasks_by_arn(
                replication_task_arn=self.replication_task_arn, without_settings=True
            )
            status = tasks[0].get("Status", "").lower() if tasks else ""
            if status != "modifying":
                self.log.info(
                    "Replication task(%s) finished modifying, current status: '%s'.",
                    self.replication_task_arn,
                    status,
                )
                return
            self.log.info(
                "Replication task(%s) still modifying, waiting %ds ...",
                self.replication_task_arn,
                self.waiter_delay,
            )
            time.sleep(self.waiter_delay)
        raise AirflowException(
            f"Replication task {self.replication_task_arn} did not finish modifying "
            f"after {self.waiter_max_attempts} attempts."
        )

    def _wait_for_status(self, target_status: str) -> None:
        import time

        for _ in range(self.waiter_max_attempts):
            tasks = self.hook.find_replication_tasks_by_arn(
                replication_task_arn=self.replication_task_arn, without_settings=True
            )
            status = tasks[0].get("Status", "").lower() if tasks else ""
            if status == target_status:
                self.log.info(
                    "Replication task(%s) reached status '%s'.", self.replication_task_arn, status
                )
                return
            self.log.info(
                "Replication task(%s) status '%s', waiting for '%s' (%ds) ...",
                self.replication_task_arn,
                status,
                target_status,
                self.waiter_delay,
            )
            time.sleep(self.waiter_delay)
        raise AirflowException(
            f"Replication task {self.replication_task_arn} did not reach status '{target_status}' "
            f"after {self.waiter_max_attempts} attempts."
        )

    def _do_modify(self) -> dict:
        result = self.hook.modify_replication_task(
            replication_task_arn=self.replication_task_arn,
            table_mappings=self.table_mappings,
            migration_type=self.migration_type,
            replication_task_settings=self.replication_task_settings,
            cdc_start_time=self.cdc_start_time,
            cdc_start_position=self.cdc_start_position,
            cdc_stop_position=self.cdc_stop_position,
        )
        self.log.info("DMS replication task(%s) has been modified.", self.replication_task_arn)
        if self.restart_task_after:
            # modify_replication_task is async — poll until it exits 'modifying' before restarting.
            self._wait_until_not_modifying()
            self.log.info(
                "Restarting replication task(%s) with type '%s'.",
                self.replication_task_arn,
                self.start_replication_task_type,
            )
            self.hook.start_replication_task(
                replication_task_arn=self.replication_task_arn,
                start_replication_task_type=self.start_replication_task_type,
            )
        return result


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
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(f"Error deleting DMS replication config: {validated_event}")

        self.replication_config_arn = validated_event.get("replication_config_arn")
        self.log.info("DMS replication config(%s) deleted.", self.replication_config_arn)

    def retry_execution(self, context, event=None):
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(f"Error waiting for DMS replication config: {validated_event}")

        self.replication_config_arn = validated_event.get("replication_config_arn")
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
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(f"Error in DMS replication: {validated_event}")

        self.replication_config_arn = validated_event.get("replication_config_arn")
        self.log.info("Replication(%s) has completed.", self.replication_config_arn)

    def retry_execution(self, context, event=None):
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(f"Error waiting for DMS replication: {validated_event}")

        self.replication_config_arn = validated_event.get("replication_config_arn")
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
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(f"Error stopping DMS replication: {validated_event}")

        self.replication_config_arn = validated_event.get("replication_config_arn")
        self.log.info("Replication(%s) has stopped.", self.replication_config_arn)
