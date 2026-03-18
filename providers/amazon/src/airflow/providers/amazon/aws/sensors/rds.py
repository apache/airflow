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
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.rds import RdsHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.amazon.aws.utils.rds import RdsDbType
from airflow.providers.common.compat.sdk import AirflowException, AirflowNotFoundException

if TYPE_CHECKING:
    from airflow.sdk import Context


class RdsBaseSensor(AwsBaseSensor[RdsHook]):
    """Base operator that implements common functions for all sensors."""

    aws_hook_class = RdsHook
    ui_color = "#ddbb77"
    ui_fgcolor = "#ffffff"

    def __init__(self, *args, hook_params: dict | None = None, **kwargs):
        self.hook_params = hook_params or {}
        self.target_statuses: list[str] = []
        super().__init__(*args, **kwargs)


class RdsSnapshotExistenceSensor(RdsBaseSensor):
    """
    Waits for RDS snapshot with a specific status.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:RdsSnapshotExistenceSensor`

    :param db_type: Type of the DB - either "instance" or "cluster"
    :param db_snapshot_identifier: The identifier for the DB snapshot
    :param target_statuses: Target status of snapshot
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

    template_fields: Sequence[str] = aws_template_fields(
        "db_snapshot_identifier",
        "target_statuses",
    )

    def __init__(
        self,
        *,
        db_type: str,
        db_snapshot_identifier: str,
        target_statuses: list[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.db_type = RdsDbType(db_type)
        self.db_snapshot_identifier = db_snapshot_identifier
        self.target_statuses = target_statuses or ["available"]

    def poke(self, context: Context):
        self.log.info(
            "Poking for statuses : %s\nfor snapshot %s", self.target_statuses, self.db_snapshot_identifier
        )
        try:
            if self.db_type.value == "instance":
                state = self.hook.get_db_snapshot_state(self.db_snapshot_identifier)
            else:
                state = self.hook.get_db_cluster_snapshot_state(self.db_snapshot_identifier)
        except AirflowNotFoundException:
            return False
        return state in self.target_statuses


class RdsExportTaskExistenceSensor(RdsBaseSensor):
    """
    Waits for RDS export task with a specific status.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:RdsExportTaskExistenceSensor`

    :param export_task_identifier: A unique identifier for the snapshot export task.
    :param target_statuses: Target status of export task
    :param error_statuses: Target error status of export task to fail the sensor
    """

    template_fields: Sequence[str] = aws_template_fields(
        "export_task_identifier", "target_statuses", "error_statuses"
    )

    def __init__(
        self,
        *,
        export_task_identifier: str,
        target_statuses: list[str] | None = None,
        error_statuses: list[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.export_task_identifier = export_task_identifier
        self.target_statuses = target_statuses or [
            "starting",
            "in_progress",
            "complete",
            "canceling",
            "canceled",
        ]
        self.error_statuses = error_statuses or ["failed"]

    def poke(self, context: Context):
        self.log.info(
            "Poking for statuses : %s\nfor export task %s", self.target_statuses, self.export_task_identifier
        )
        try:
            state = self.hook.get_export_task_state(self.export_task_identifier)
            if state in self.error_statuses:
                raise AirflowException(
                    f"Export task {self.export_task_identifier} failed with status {state}"
                )

        except AirflowNotFoundException:
            return False
        return state in self.target_statuses


class RdsDbSensor(RdsBaseSensor):
    """
    Waits for an RDS instance or cluster to enter one of a number of states.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:RdsDbSensor`

    :param db_type: Type of the DB - either "instance" or "cluster" (default: 'instance')
    :param db_identifier: The AWS identifier for the DB
    :param target_statuses: Target status of DB
    """

    template_fields: Sequence[str] = aws_template_fields(
        "db_identifier",
        "db_type",
        "target_statuses",
    )

    def __init__(
        self,
        *,
        db_identifier: str,
        db_type: RdsDbType | str = RdsDbType.INSTANCE,
        target_statuses: list[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.db_identifier = db_identifier
        self.target_statuses = target_statuses or ["available"]
        self.db_type = db_type

    def poke(self, context: Context):
        db_type = RdsDbType(self.db_type)
        self.log.info(
            "Poking for statuses : %s\nfor db instance %s", self.target_statuses, self.db_identifier
        )
        try:
            if db_type == RdsDbType.INSTANCE:
                state = self.hook.get_db_instance_state(self.db_identifier)
            else:
                state = self.hook.get_db_cluster_state(self.db_identifier)
        except AirflowNotFoundException:
            return False
        return state in self.target_statuses


__all__ = [
    "RdsExportTaskExistenceSensor",
    "RdsDbSensor",
    "RdsSnapshotExistenceSensor",
]
