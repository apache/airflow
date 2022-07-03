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

import json
from enum import Enum

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class DmsTaskWaiterStatus(str, Enum):
    """Available AWS DMS Task Waiter statuses."""

    DELETED = 'deleted'
    READY = 'ready'
    RUNNING = 'running'
    STOPPED = 'stopped'


class DmsHook(AwsBaseHook):
    """Interact with AWS Database Migration Service."""

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        kwargs['client_type'] = 'dms'
        super().__init__(*args, **kwargs)

    def describe_replication_tasks(self, **kwargs):
        """
        Describe replication tasks

        :return: Marker and list of replication tasks
        :rtype: (Optional[str], list)
        """
        dms_client = self.get_conn()
        response = dms_client.describe_replication_tasks(**kwargs)

        return response.get('Marker'), response.get('ReplicationTasks', [])

    def find_replication_tasks_by_arn(self, replication_task_arn: str, without_settings: bool | None = False):
        """
        Find and describe replication tasks by task ARN
        :param replication_task_arn: Replication task arn
        :param without_settings: Indicates whether to return task information with settings.

        :return: list of replication tasks that match the ARN
        """
        _, tasks = self.describe_replication_tasks(
            Filters=[
                {
                    'Name': 'replication-task-arn',
                    'Values': [replication_task_arn],
                }
            ],
            WithoutSettings=without_settings,
        )

        return tasks

    def get_task_status(self, replication_task_arn: str) -> str | None:
        """
        Retrieve task status.

        :param replication_task_arn: Replication task ARN
        :return: Current task status
        """
        replication_tasks = self.find_replication_tasks_by_arn(
            replication_task_arn=replication_task_arn,
            without_settings=True,
        )

        if len(replication_tasks) == 1:
            status = replication_tasks[0]['Status']
            self.log.info('Replication task with ARN(%s) has status "%s".', replication_task_arn, status)
            return status
        else:
            self.log.info('Replication task with ARN(%s) is not found.', replication_task_arn)
            return None

    def create_replication_task(
        self,
        replication_task_id: str,
        source_endpoint_arn: str,
        target_endpoint_arn: str,
        replication_instance_arn: str,
        migration_type: str,
        table_mappings: dict,
        **kwargs,
    ) -> str:
        """
        Create DMS replication task

        :param replication_task_id: Replication task id
        :param source_endpoint_arn: Source endpoint ARN
        :param target_endpoint_arn: Target endpoint ARN
        :param replication_instance_arn: Replication instance ARN
        :param table_mappings: Table mappings
        :param migration_type: Migration type ('full-load'|'cdc'|'full-load-and-cdc'), full-load by default.
        :return: Replication task ARN
        """
        dms_client = self.get_conn()
        create_task_response = dms_client.create_replication_task(
            ReplicationTaskIdentifier=replication_task_id,
            SourceEndpointArn=source_endpoint_arn,
            TargetEndpointArn=target_endpoint_arn,
            ReplicationInstanceArn=replication_instance_arn,
            MigrationType=migration_type,
            TableMappings=json.dumps(table_mappings),
            **kwargs,
        )

        replication_task_arn = create_task_response['ReplicationTask']['ReplicationTaskArn']
        self.wait_for_task_status(replication_task_arn, DmsTaskWaiterStatus.READY)

        return replication_task_arn

    def start_replication_task(
        self,
        replication_task_arn: str,
        start_replication_task_type: str,
        **kwargs,
    ):
        """
        Starts replication task.

        :param replication_task_arn: Replication task ARN
        :param start_replication_task_type: Replication task start type (default='start-replication')
            ('start-replication'|'resume-processing'|'reload-target')
        """
        dms_client = self.get_conn()
        dms_client.start_replication_task(
            ReplicationTaskArn=replication_task_arn,
            StartReplicationTaskType=start_replication_task_type,
            **kwargs,
        )

    def stop_replication_task(self, replication_task_arn):
        """
        Stops replication task.

        :param replication_task_arn: Replication task ARN
        """
        dms_client = self.get_conn()
        dms_client.stop_replication_task(ReplicationTaskArn=replication_task_arn)

    def delete_replication_task(self, replication_task_arn):
        """
        Starts replication task deletion and waits for it to be deleted

        :param replication_task_arn: Replication task ARN
        """
        dms_client = self.get_conn()
        dms_client.delete_replication_task(ReplicationTaskArn=replication_task_arn)

        self.wait_for_task_status(replication_task_arn, DmsTaskWaiterStatus.DELETED)

    def wait_for_task_status(self, replication_task_arn: str, status: DmsTaskWaiterStatus):
        """
        Waits for replication task to reach status.
        Supported statuses: deleted, ready, running, stopped.

        :param status: Status to wait for
        :param replication_task_arn: Replication task ARN
        """
        if not isinstance(status, DmsTaskWaiterStatus):
            raise TypeError('Status must be an instance of DmsTaskWaiterStatus')

        dms_client = self.get_conn()
        waiter = dms_client.get_waiter(f'replication_task_{status}')
        waiter.wait(
            Filters=[
                {
                    'Name': 'replication-task-arn',
                    'Values': [
                        replication_task_arn,
                    ],
                },
            ],
            WithoutSettings=True,
        )
