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

    def _describe_item(self, **kwargs) -> list:
        """Returns information about target item: snapshot or task"""
        raise NotImplementedError

    def _check_item(self, **kwargs) -> bool:
        """Get certain item from `_describe_item()` and check it status"""
        try:
            item = self._describe_item()
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
    :type db_type: RDSDbType
    :param db_identifier: The identifier of the instance or cluster that you want to create the snapshot of
    :type db_identifier: str
    :param db_snapshot_identifier: The identifier for the DB snapshot
    :type db_snapshot_identifier: str
    :param target_statuses: Target status of snapshot
    :type target_statuses: List[str]
    """

    template_fields: Sequence[str] = (
        'db_identifier',
        'db_snapshot_identifier',
        'target_status',
    )

    def __init__(
        self,
        *,
        db_type: str,
        db_identifier: str,
        db_snapshot_identifier: str,
        target_statuses: Optional[List[str]] = None,
        aws_conn_id: str = "aws_conn_id",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.db_type = RdsDbType(db_type)
        self.db_identifier = db_identifier
        self.db_snapshot_identifier = db_snapshot_identifier
        self.target_statuses = target_statuses or ['available']

    def _describe_item(self, **kwargs) -> list:
        """Returns snapshot info"""
        if self.db_type.value == "instance":
            db_snapshots = self.hook.conn.describe_db_snapshots(
                DBInstanceIdentifier=self.db_identifier,
                DBSnapshotIdentifier=self.db_snapshot_identifier,
                **kwargs,
            )
            return db_snapshots['DBSnapshots']
        else:
            db_cluster_snapshots = self.hook.conn.describe_db_cluster_snapshots(
                DBClusterIdentifier=self.db_identifier,
                DBClusterSnapshotIdentifier=self.db_snapshot_identifier,
                **kwargs,
            )
            return db_cluster_snapshots['DBClusterSnapshots']

    def poke(self, context: 'Context'):
        self.log.info(
            'Poking for statuses : %s\nfor snapshot %s', self.target_statuses, self.db_snapshot_identifier
        )
        return self._check_item()


class RdsExportTaskExistenceSensor(RdsBaseSensor):
    """
    Waits for RDS export task with a specific status.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsExportTaskExistenceSensor`

    :param export_task_identifier: A unique identifier for the snapshot export task.
    :type export_task_identifier: str
    :param source_arn: The Amazon Resource Name (ARN) of the snapshot to export to Amazon S3.
    :type source_arn: str
    :param target_statuses: Target status of export task
    :type target_statuses: List[str]
    """

    template_fields: Sequence[str] = (
        'export_task_identifier',
        'source_arn',
        'target_status',
    )

    def __init__(
        self,
        *,
        export_task_identifier: str,
        source_arn: str,
        target_statuses: Optional[List[str]] = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)

        self.export_task_identifier = export_task_identifier
        self.source_arn = source_arn
        self.target_statuses = target_statuses or ['available']

    def _describe_item(self, **kwargs) -> list:
        response = self.hook.conn.describe_export_tasks(
            ExportTaskIdentifier=self.export_task_identifier,
            SourceArn=self.source_arn,
        )
        return response['ExportTasks']

    def poke(self, context: 'Context'):
        self.log.info(
            'Poking for statuses : %s\nfor export task %s', self.target_statuses, self.export_task_identifier
        )
        return self._check_item()


__all__ = [
    "RdsExportTaskExistenceSensor",
    "RdsSnapshotExistenceSensor",
]
