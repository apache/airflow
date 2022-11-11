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

from typing import TYPE_CHECKING, Sequence

from botocore.exceptions import ClientError

from airflow import AirflowException
from airflow.providers.amazon.aws.hooks.rds import RdsHook
from airflow.providers.amazon.aws.utils.rds import RdsDbType
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RdsBaseSensor(BaseSensorOperator):
    """Base operator that implements common functions for all sensors"""

    ui_color = "#ddbb77"
    ui_fgcolor = "#ffffff"

    def __init__(self, *args, aws_conn_id: str = "aws_conn_id", hook_params: dict | None = None, **kwargs):
        hook_params = hook_params or {}
        self.hook = RdsHook(aws_conn_id=aws_conn_id, **hook_params)
        self.target_statuses: list[str] = []
        super().__init__(*args, **kwargs)

    def _describe_item(self, item_type: str, item_name: str) -> list:
        if item_type == "instance_snapshot":
            db_snaps = self.hook.conn.describe_db_snapshots(DBSnapshotIdentifier=item_name)
            return db_snaps["DBSnapshots"]
        elif item_type == "cluster_snapshot":
            cl_snaps = self.hook.conn.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=item_name)
            return cl_snaps["DBClusterSnapshots"]
        elif item_type == "export_task":
            exports = self.hook.conn.describe_export_tasks(ExportTaskIdentifier=item_name)
            return exports["ExportTasks"]
        elif item_type == "db_instance":
            instances = self.hook.conn.describe_db_instances(DBInstanceIdentifier=item_name)
            return instances["DBInstances"]
        elif item_type == "db_cluster":
            clusters = self.hook.conn.describe_db_clusters(DBClusterIdentifier=item_name)
            return clusters["DBClusters"]
        else:
            raise AirflowException(f"Method for {item_type} is not implemented")

    def _check_item(self, item_type: str, item_name: str) -> bool:
        """Get certain item from `_describe_item()` and check its status"""
        if item_type == "db_instance":
            status_field = "DBInstanceStatus"
        else:
            status_field = "Status"
        try:
            items = self._describe_item(item_type, item_name)
        except ClientError:
            return False
        else:
            return bool(items) and any(
                map(lambda status: items[0][status_field].lower() == status, self.target_statuses)
            )


class RdsSnapshotExistenceSensor(RdsBaseSensor):
    """
    Waits for RDS snapshot with a specific status.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:RdsSnapshotExistenceSensor`

    :param db_type: Type of the DB - either "instance" or "cluster"
    :param db_snapshot_identifier: The identifier for the DB snapshot
    :param target_statuses: Target status of snapshot
    """

    template_fields: Sequence[str] = (
        "db_snapshot_identifier",
        "target_statuses",
    )

    def __init__(
        self,
        *,
        db_type: str,
        db_snapshot_identifier: str,
        target_statuses: list[str] | None = None,
        aws_conn_id: str = "aws_conn_id",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.db_type = RdsDbType(db_type)
        self.db_snapshot_identifier = db_snapshot_identifier
        self.target_statuses = target_statuses or ["available"]

    def poke(self, context: Context):
        self.log.info(
            "Poking for statuses : %s\nfor snapshot %s", self.target_statuses, self.db_snapshot_identifier
        )
        if self.db_type.value == "instance":
            return self._check_item(item_type="instance_snapshot", item_name=self.db_snapshot_identifier)
        else:
            return self._check_item(item_type="cluster_snapshot", item_name=self.db_snapshot_identifier)


class RdsExportTaskExistenceSensor(RdsBaseSensor):
    """
    Waits for RDS export task with a specific status.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:RdsExportTaskExistenceSensor`

    :param export_task_identifier: A unique identifier for the snapshot export task.
    :param target_statuses: Target status of export task
    """

    template_fields: Sequence[str] = (
        "export_task_identifier",
        "target_statuses",
    )

    def __init__(
        self,
        *,
        export_task_identifier: str,
        target_statuses: list[str] | None = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)

        self.export_task_identifier = export_task_identifier
        self.target_statuses = target_statuses or [
            "starting",
            "in_progress",
            "complete",
            "canceling",
            "canceled",
        ]

    def poke(self, context: Context):
        self.log.info(
            "Poking for statuses : %s\nfor export task %s", self.target_statuses, self.export_task_identifier
        )
        return self._check_item(item_type="export_task", item_name=self.export_task_identifier)


class RdsDbSensor(RdsBaseSensor):
    """
    Waits for an RDS instance or cluster to enter one of a number of states

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:RdsDbSensor`

    :param db_type: Type of the DB - either "instance" or "cluster" (default: 'instance')
    :param db_identifier: The AWS identifier for the DB
    :param target_statuses: Target status of DB
    """

    template_fields: Sequence[str] = (
        "db_identifier",
        "db_type",
        "target_statuses",
    )

    def __init__(
        self,
        *,
        db_identifier: str,
        db_type: str = "instance",
        target_statuses: list[str] | None = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.db_identifier = db_identifier
        self.target_statuses = target_statuses or ["available"]
        self.db_type = RdsDbType(db_type)

    def poke(self, context: Context):
        self.log.info(
            "Poking for statuses : %s\nfor db instance %s", self.target_statuses, self.db_identifier
        )
        item_type = self._check_item_type()
        return self._check_item(item_type=item_type, item_name=self.db_identifier)

    def _check_item_type(self):
        if self.db_type == RdsDbType.CLUSTER:
            return "db_cluster"
        return "db_instance"


__all__ = [
    "RdsExportTaskExistenceSensor",
    "RdsDbSensor",
    "RdsSnapshotExistenceSensor",
]
