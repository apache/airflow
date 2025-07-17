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
from datetime import datetime
from enum import Enum
from typing import Any

from botocore.exceptions import ClientError
from dateutil import parser

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class DmsTaskWaiterStatus(str, Enum):
    """Available AWS DMS Task Waiter statuses."""

    DELETED = "deleted"
    READY = "ready"
    RUNNING = "running"
    STOPPED = "stopped"


class DmsHook(AwsBaseHook):
    """
    Interact with AWS Database Migration Service (DMS).

    Provide thin wrapper around
    :external+boto3:py:class:`boto3.client("dms") <DatabaseMigrationService.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        kwargs["client_type"] = "dms"
        super().__init__(*args, **kwargs)

    def describe_replication_tasks(self, **kwargs) -> tuple[str | None, list]:
        """
        Describe replication tasks.

        .. seealso::
            - :external+boto3:py:meth:`DatabaseMigrationService.Client.describe_replication_tasks`

        :return: Marker and list of replication tasks
        """
        dms_client = self.get_conn()
        response = dms_client.describe_replication_tasks(**kwargs)

        return response.get("Marker"), response.get("ReplicationTasks", [])

    def find_replication_tasks_by_arn(self, replication_task_arn: str, without_settings: bool | None = False):
        """
        Find and describe replication tasks by task ARN.

        .. seealso::
            - :external+boto3:py:meth:`DatabaseMigrationService.Client.describe_replication_tasks`

        :param replication_task_arn: Replication task arn
        :param without_settings: Indicates whether to return task information with settings.
        :return: list of replication tasks that match the ARN
        """
        _, tasks = self.describe_replication_tasks(
            Filters=[
                {
                    "Name": "replication-task-arn",
                    "Values": [replication_task_arn],
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
            status = replication_tasks[0]["Status"]
            self.log.info('Replication task with ARN(%s) has status "%s".', replication_task_arn, status)
            return status
        self.log.info("Replication task with ARN(%s) is not found.", replication_task_arn)
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
        Create DMS replication task.

        .. seealso::
            - :external+boto3:py:meth:`DatabaseMigrationService.Client.create_replication_task`

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

        replication_task_arn = create_task_response["ReplicationTask"]["ReplicationTaskArn"]
        self.wait_for_task_status(replication_task_arn, DmsTaskWaiterStatus.READY)

        return replication_task_arn

    def start_replication_task(
        self,
        replication_task_arn: str,
        start_replication_task_type: str,
        **kwargs,
    ):
        """
        Start replication task.

        .. seealso::
            - :external+boto3:py:meth:`DatabaseMigrationService.Client.start_replication_task`

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
        Stop replication task.

        .. seealso::
            - :external+boto3:py:meth:`DatabaseMigrationService.Client.stop_replication_task`

        :param replication_task_arn: Replication task ARN
        """
        dms_client = self.get_conn()
        dms_client.stop_replication_task(ReplicationTaskArn=replication_task_arn)

    def delete_replication_task(self, replication_task_arn):
        """
        Start replication task deletion and waits for it to be deleted.

        .. seealso::
            - :external+boto3:py:meth:`DatabaseMigrationService.Client.delete_replication_task`

        :param replication_task_arn: Replication task ARN
        """
        dms_client = self.get_conn()
        dms_client.delete_replication_task(ReplicationTaskArn=replication_task_arn)

        self.wait_for_task_status(replication_task_arn, DmsTaskWaiterStatus.DELETED)

    def wait_for_task_status(self, replication_task_arn: str, status: DmsTaskWaiterStatus):
        """
        Wait for replication task to reach status; supported statuses: deleted, ready, running, stopped.

        :param status: Status to wait for
        :param replication_task_arn: Replication task ARN
        """
        if not isinstance(status, DmsTaskWaiterStatus):
            raise TypeError("Status must be an instance of DmsTaskWaiterStatus")

        dms_client = self.get_conn()
        waiter = dms_client.get_waiter(f"replication_task_{status.value}")
        waiter.wait(
            Filters=[
                {
                    "Name": "replication-task-arn",
                    "Values": [
                        replication_task_arn,
                    ],
                },
            ],
            WithoutSettings=True,
        )

    def describe_replication_configs(self, filters: list[dict] | None = None, **kwargs) -> list[dict]:
        """
        Return list of serverless replication configs.

        .. seealso::
            - :external+boto3:py:meth:`DatabaseMigrationService.Client.describe_replication_configs`

        :param filters: List of filter objects
        :return: List of replication tasks
        """
        filters = filters if filters is not None else []

        try:
            resp = self.conn.describe_replication_configs(Filters=filters, **kwargs)
            return resp.get("ReplicationConfigs", [])
        except Exception as ex:
            self.log.error("Error while describing replication configs: %s", str(ex))
            raise ex

    def create_replication_config(
        self,
        replication_config_id: str,
        source_endpoint_arn: str,
        target_endpoint_arn: str,
        compute_config: dict[str, Any],
        replication_type: str,
        table_mappings: str,
        additional_config_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ):
        """
        Create an AWS DMS Serverless configuration that can be used to start an DMS Serverless replication.

        .. seealso::
            - :external+boto3:py:meth:`DatabaseMigrationService.Client.create_replication_config`

        :param replicationConfigId: Unique identifier used to create a ReplicationConfigArn.
        :param sourceEndpointArn: ARN of the source endpoint
        :param targetEndpointArn: ARN of the target endpoint
        :param computeConfig: Parameters for provisioning an DMS Serverless replication.
        :param replicationType: type of DMS Serverless replication
        :param tableMappings: JSON table mappings
        :param tags: Key-value tag pairs
        :param resourceId: Unique value or name that you set for a given resource that can be used to construct an Amazon Resource Name (ARN) for that resource.
        :param supplementalSettings: JSON settings for specifying supplemental data
        :param replicationSettings: JSON settings for DMS Serverless replications

        :return: ReplicationConfigArn

        """
        if additional_config_kwargs is None:
            additional_config_kwargs = {}
        try:
            resp = self.conn.create_replication_config(
                ReplicationConfigIdentifier=replication_config_id,
                SourceEndpointArn=source_endpoint_arn,
                TargetEndpointArn=target_endpoint_arn,
                ComputeConfig=compute_config,
                ReplicationType=replication_type,
                TableMappings=table_mappings,
                **additional_config_kwargs,
            )
            arn = resp.get("ReplicationConfig", {}).get("ReplicationConfigArn")
            self.log.info("Successfully created replication config: %s", arn)
            return arn

        except ClientError as err:
            err_str = (
                f"Error: {err.get('Error', '').get('Code', '')}: {err.get('Error', '').get('Message', '')}"
            )
            self.log.error("Error while creating replication config: %s", err_str)
            raise err

    def describe_replications(self, filters: list[dict[str, Any]] | None = None, **kwargs) -> list[dict]:
        """
        Return list of serverless replications.

        .. seealso::
            - :external+boto3:py:meth:`DatabaseMigrationService.Client.describe_replications`

        :param filters: List of filter objects
        :return: List of replications
        """
        filters = filters if filters is not None else []
        try:
            resp = self.conn.describe_replications(Filters=filters, **kwargs)
            return resp.get("Replications", [])
        except Exception as ex:
            self.log.error("Error while describing replications: %s", str(ex))
            raise ex

    def delete_replication_config(
        self, replication_config_arn: str, delay: int = 60, max_attempts: int = 120
    ):
        """
        Delete an AWS DMS Serverless configuration.

        .. seealso::
            - :external+boto3:py:meth:`DatabaseMigrationService.Client.delete_replication_config`

        :param replication_config_arn: ReplicationConfigArn
        """
        try:
            self.log.info("Deleting replication config: %s", replication_config_arn)

            self.conn.delete_replication_config(ReplicationConfigArn=replication_config_arn)

        except ClientError as err:
            err_str = (
                f"Error: {err.get('Error', '').get('Code', '')}: {err.get('Error', '').get('Message', '')}"
            )
            self.log.error("Error while deleting replication config: %s", err_str)
            raise err

    def start_replication(
        self,
        replication_config_arn: str,
        start_replication_type: str,
        cdc_start_time: datetime | str | None = None,
        cdc_start_pos: str | None = None,
        cdc_stop_pos: str | None = None,
    ):
        additional_args: dict[str, Any] = {}

        if cdc_start_time:
            additional_args["CdcStartTime"] = (
                cdc_start_time if isinstance(cdc_start_time, datetime) else parser.parse(cdc_start_time)
            )
        if cdc_start_pos:
            additional_args["CdcStartPosition"] = cdc_start_pos
        if cdc_stop_pos:
            additional_args["CdcStopPosition"] = cdc_stop_pos

        try:
            resp = self.conn.start_replication(
                ReplicationConfigArn=replication_config_arn,
                StartReplicationType=start_replication_type,
                **additional_args,
            )

            return resp
        except Exception as ex:
            self.log.error("Error while starting replication: %s", str(ex))
            raise ex

    def stop_replication(self, replication_config_arn: str):
        resp = self.conn.stop_replication(ReplicationConfigArn=replication_config_arn)
        return resp

    def get_provision_status(self, replication_config_arn: str) -> str:
        """Get the provisioning status for a serverless replication."""
        result = self.describe_replications(
            filters=[{"Name": "replication-config-arn", "Values": [replication_config_arn]}]
        )

        provision_status = result[0].get("ProvisionData", {}).get("ProvisionState", "")
        return provision_status
