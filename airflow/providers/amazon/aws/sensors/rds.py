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

from typing import TYPE_CHECKING, List, Optional, Sequence

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

    def __init__(self, *args, aws_conn_id: str = "aws_conn_id", hook_params: Optional[dict] = None, **kwargs):
        hook_params = hook_params or {}
        self.hook = RdsHook(aws_conn_id=aws_conn_id, **hook_params)
        self.target_statuses: List[str] = []
        super().__init__(*args, **kwargs)

    def _describe_db_snapshot(self, db_snapshot_identifier):
        """Returns information about target item: snapshot"""
        db_snapshots = self.hook.conn.describe_db_snapshots(DBSnapshotIdentifier=db_snapshot_identifier)
        return db_snapshots['DBSnapshots']

    def _describe_db_cluster_snapshot(self, db_snapshot_identifier):
        """Returns information about target item: snapshot"""
        db_cluster_snapshots = self.hook.conn.describe_db_cluster_snapshots(
            DBClusterSnapshotIdentifier=db_snapshot_identifier,
        )
        return db_cluster_snapshots['DBClusterSnapshots']

    def _describe_export_task(self, export_task_identifier) -> list:
        """Returns information about target item: export task"""
        response = self.hook.conn.describe_export_tasks(ExportTaskIdentifier=export_task_identifier)
        return response['ExportTasks']

    def _check_item(self, item_type: str, item_name: str) -> bool:
        """Get certain item from `_describe_item()` and check it status"""
        if item_type == 'instance_snapshot':
            describe_item = self._describe_db_snapshot
        elif item_type == 'cluster_snapshot':
            describe_item = self._describe_db_cluster_snapshot
        elif item_type == 'export_task':
            describe_item = self._describe_export_task
        else:
            raise AirflowException(f"Method for {item_type} is not implemented")

        try:
            item = describe_item(item_name)
        except ClientError:
            return False
        else:
            return bool(item) and any(map(lambda s: item[0]['Status'] == s, self.target_statuses))


class RdsSnapshotExistenceSensor(RdsBaseSensor):
    """
    Waits for RDS snapshot with a specific status.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsSnapshotExistenceSensor`

    :param db_type: Type of the DB - either "instance" or "cluster"
    :param db_snapshot_identifier: The identifier for the DB snapshot
    :param target_statuses: Target status of snapshot
    """

    template_fields: Sequence[str] = (
        'db_snapshot_identifier',
        'target_status',
    )

    def __init__(
        self,
        *,
        db_type: str,
        db_snapshot_identifier: str,
        target_statuses: Optional[List[str]] = None,
        aws_conn_id: str = "aws_conn_id",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.db_type = RdsDbType(db_type)
        self.db_snapshot_identifier = db_snapshot_identifier
        self.target_statuses = target_statuses or ['available']

    def poke(self, context: 'Context'):
        self.log.info(
            'Poking for statuses : %s\nfor snapshot %s', self.target_statuses, self.db_snapshot_identifier
        )
        if self.db_type.value == "instance":
            return self._check_item(item_type='instance_snapshot', item_name=self.db_snapshot_identifier)
        else:
            return self._check_item(item_type='cluster_snapshot', item_name=self.db_snapshot_identifier)


class RdsExportTaskExistenceSensor(RdsBaseSensor):
    """
    Waits for RDS export task with a specific status.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsExportTaskExistenceSensor`

    :param export_task_identifier: A unique identifier for the snapshot export task.
    :param target_statuses: Target status of export task
    """

    template_fields: Sequence[str] = (
        'export_task_identifier',
        'target_status',
    )

    def __init__(
        self,
        *,
        export_task_identifier: str,
        target_statuses: Optional[List[str]] = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)

        self.export_task_identifier = export_task_identifier
        self.target_statuses = target_statuses or ['available']

    def poke(self, context: 'Context'):
        self.log.info(
            'Poking for statuses : %s\nfor export task %s', self.target_statuses, self.export_task_identifier
        )
        return self._check_item(item_type='export_task', item_name=self.export_task_identifier)


__all__ = [
    "RdsExportTaskExistenceSensor",
    "RdsSnapshotExistenceSensor",
]
