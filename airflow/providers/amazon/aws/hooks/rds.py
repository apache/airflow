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
"""Interact with AWS RDS."""
from __future__ import annotations

import time
from typing import TYPE_CHECKING, Callable

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

if TYPE_CHECKING:
    from mypy_boto3_rds import RDSClient  # noqa


class RdsHook(AwsGenericHook["RDSClient"]):
    """
    Interact with AWS RDS using proper client from the boto3 library.

    Hook attribute `conn` has all methods that listed in documentation

    .. seealso::
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html
        - https://docs.aws.amazon.com/rds/index.html

    Additional arguments (such as ``aws_conn_id`` or ``region_name``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook`

    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "rds"
        super().__init__(*args, **kwargs)

    def get_db_snapshot_state(self, snapshot_id: str) -> str:
        """
        Get the current state of a DB instance snapshot.

        :param snapshot_id: The ID of the target DB instance snapshot
        :return: Returns the status of the DB snapshot as a string (eg. "available")
        :rtype: str
        :raises AirflowNotFoundException: If the DB instance snapshot does not exist.
        """
        try:
            response = self.conn.describe_db_snapshots(DBSnapshotIdentifier=snapshot_id)
        except self.conn.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "DBSnapshotNotFound":
                raise AirflowNotFoundException(e)
            raise e
        return response["DBSnapshots"][0]["Status"].lower()

    def wait_for_db_snapshot_state(
        self, snapshot_id: str, target_state: str, check_interval: int = 30, max_attempts: int = 40
    ) -> None:
        """
        Polls :py:meth:`RDS.Client.describe_db_snapshots` until the target state is reached.
        An error is raised after a max number of attempts.

        :param snapshot_id: The ID of the target DB instance snapshot
        :param target_state: Wait until this state is reached
        :param check_interval: The amount of time in seconds to wait between attempts
        :param max_attempts: The maximum number of attempts to be made
        """

        def poke():
            return self.get_db_snapshot_state(snapshot_id)

        target_state = target_state.lower()
        if target_state in ("available", "deleted", "completed"):
            waiter = self.conn.get_waiter(f"db_snapshot_{target_state}")  # type: ignore
            waiter.wait(
                DBSnapshotIdentifier=snapshot_id,
                WaiterConfig={"Delay": check_interval, "MaxAttempts": max_attempts},
            )
        else:
            self._wait_for_state(poke, target_state, check_interval, max_attempts)
            self.log.info("DB snapshot '%s' reached the '%s' state", snapshot_id, target_state)

    def get_db_cluster_snapshot_state(self, snapshot_id: str) -> str:
        """
        Get the current state of a DB cluster snapshot.

        :param snapshot_id: The ID of the target DB cluster.
        :return: Returns the status of the DB cluster snapshot as a string (eg. "available")
        :rtype: str
        :raises AirflowNotFoundException: If the DB cluster snapshot does not exist.
        """
        try:
            response = self.conn.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=snapshot_id)
        except self.conn.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "DBClusterSnapshotNotFoundFault":
                raise AirflowNotFoundException(e)
            raise e
        return response["DBClusterSnapshots"][0]["Status"].lower()

    def wait_for_db_cluster_snapshot_state(
        self, snapshot_id: str, target_state: str, check_interval: int = 30, max_attempts: int = 40
    ) -> None:
        """
        Polls :py:meth:`RDS.Client.describe_db_cluster_snapshots` until the target state is reached.
        An error is raised after a max number of attempts.

        :param snapshot_id: The ID of the target DB cluster snapshot
        :param target_state: Wait until this state is reached
        :param check_interval: The amount of time in seconds to wait between attempts
        :param max_attempts: The maximum number of attempts to be made

        .. seealso::
            A list of possible values for target_state:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.describe_db_cluster_snapshots
        """

        def poke():
            return self.get_db_cluster_snapshot_state(snapshot_id)

        target_state = target_state.lower()
        if target_state in ("available", "deleted"):
            waiter = self.conn.get_waiter(f"db_cluster_snapshot_{target_state}")  # type: ignore
            waiter.wait(
                DBClusterSnapshotIdentifier=snapshot_id,
                WaiterConfig={"Delay": check_interval, "MaxAttempts": max_attempts},
            )
        else:
            self._wait_for_state(poke, target_state, check_interval, max_attempts)
            self.log.info("DB cluster snapshot '%s' reached the '%s' state", snapshot_id, target_state)

    def get_export_task_state(self, export_task_id: str) -> str:
        """
        Gets the current state of an RDS snapshot export to Amazon S3.

        :param export_task_id: The identifier of the target snapshot export task.
        :return: Returns the status of the snapshot export task as a string (eg. "canceled")
        :rtype: str
        :raises AirflowNotFoundException: If the export task does not exist.
        """
        try:
            response = self.conn.describe_export_tasks(ExportTaskIdentifier=export_task_id)
        except self.conn.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ExportTaskNotFoundFault":
                raise AirflowNotFoundException(e)
            raise e
        return response["ExportTasks"][0]["Status"].lower()

    def wait_for_export_task_state(
        self, export_task_id: str, target_state: str, check_interval: int = 30, max_attempts: int = 40
    ) -> None:
        """
        Polls :py:meth:`RDS.Client.describe_export_tasks` until the target state is reached.
        An error is raised after a max number of attempts.

        :param export_task_id: The identifier of the target snapshot export task.
        :param target_state: Wait until this state is reached
        :param check_interval: The amount of time in seconds to wait between attempts
        :param max_attempts: The maximum number of attempts to be made

        .. seealso::
            A list of possible values for target_state:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.describe_export_tasks
        """

        def poke():
            return self.get_export_task_state(export_task_id)

        target_state = target_state.lower()
        self._wait_for_state(poke, target_state, check_interval, max_attempts)
        self.log.info("export task '%s' reached the '%s' state", export_task_id, target_state)

    def get_event_subscription_state(self, subscription_name: str) -> str:
        """
        Gets the current state of an RDS snapshot export to Amazon S3.

        :param subscription_name: The name of the target RDS event notification subscription.
        :return: Returns the status of the event subscription as a string (eg. "active")
        :rtype: str
        :raises AirflowNotFoundException: If the event subscription does not exist.
        """
        try:
            response = self.conn.describe_event_subscriptions(SubscriptionName=subscription_name)
        except self.conn.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "SubscriptionNotFoundFault":
                raise AirflowNotFoundException(e)
            raise e
        return response["EventSubscriptionsList"][0]["Status"].lower()

    def wait_for_event_subscription_state(
        self, subscription_name: str, target_state: str, check_interval: int = 30, max_attempts: int = 40
    ) -> None:
        """
        Polls :py:meth:`RDS.Client.describe_event_subscriptions` until the target state is reached.
        An error is raised after a max number of attempts.

        :param subscription_name: The name of the target RDS event notification subscription.
        :param target_state: Wait until this state is reached
        :param check_interval: The amount of time in seconds to wait between attempts
        :param max_attempts: The maximum number of attempts to be made

        .. seealso::
            A list of possible values for target_state:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.describe_event_subscriptions
        """

        def poke():
            return self.get_event_subscription_state(subscription_name)

        target_state = target_state.lower()
        self._wait_for_state(poke, target_state, check_interval, max_attempts)
        self.log.info("event subscription '%s' reached the '%s' state", subscription_name, target_state)

    def get_db_instance_state(self, db_instance_id: str) -> str:
        """
        Get the current state of a DB instance.

        :param snapshot_id: The ID of the target DB instance.
        :return: Returns the status of the DB instance as a string (eg. "available")
        :rtype: str
        :raises AirflowNotFoundException: If the DB instance does not exist.
        """
        try:
            response = self.conn.describe_db_instances(DBInstanceIdentifier=db_instance_id)
        except self.conn.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "DBInstanceNotFoundFault":
                raise AirflowNotFoundException(e)
            raise e
        return response["DBInstances"][0]["DBInstanceStatus"].lower()

    def wait_for_db_instance_state(
        self, db_instance_id: str, target_state: str, check_interval: int = 30, max_attempts: int = 40
    ) -> None:
        """
        Polls :py:meth:`RDS.Client.describe_db_instances` until the target state is reached.
        An error is raised after a max number of attempts.

        :param db_instance_id: The ID of the target DB instance.
        :param target_state: Wait until this state is reached
        :param check_interval: The amount of time in seconds to wait between attempts
        :param max_attempts: The maximum number of attempts to be made

        .. seealso::
            For information about DB instance statuses, see Viewing DB instance status in the Amazon RDS
            User Guide.
            https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/accessing-monitoring.html#Overview.DBInstance.Status
        """

        def poke():
            return self.get_db_instance_state(db_instance_id)

        target_state = target_state.lower()
        if target_state in ("available", "deleted"):
            waiter = self.conn.get_waiter(f"db_instance_{target_state}")  # type: ignore
            waiter.wait(
                DBInstanceIdentifier=db_instance_id,
                WaiterConfig={"Delay": check_interval, "MaxAttempts": max_attempts},
            )
        else:
            self._wait_for_state(poke, target_state, check_interval, max_attempts)
            self.log.info("DB cluster snapshot '%s' reached the '%s' state", db_instance_id, target_state)

    def get_db_cluster_state(self, db_cluster_id: str) -> str:
        """
        Get the current state of a DB cluster.

        :param snapshot_id: The ID of the target DB cluster.
        :return: Returns the status of the DB cluster as a string (eg. "available")
        :rtype: str
        :raises AirflowNotFoundException: If the DB cluster does not exist.
        """
        try:
            response = self.conn.describe_db_clusters(DBClusterIdentifier=db_cluster_id)
        except self.conn.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "DBClusterNotFoundFault":
                raise AirflowNotFoundException(e)
            raise e
        return response["DBClusters"][0]["Status"].lower()

    def wait_for_db_cluster_state(
        self, db_cluster_id: str, target_state: str, check_interval: int = 30, max_attempts: int = 40
    ) -> None:
        """
        Polls :py:meth:`RDS.Client.describe_db_clusters` until the target state is reached.
        An error is raised after a max number of attempts.

        :param db_cluster_id: The ID of the target DB cluster.
        :param target_state: Wait until this state is reached
        :param check_interval: The amount of time in seconds to wait between attempts
        :param max_attempts: The maximum number of attempts to be made

        .. seealso::
            For information about DB instance statuses, see Viewing DB instance status in the Amazon RDS
            User Guide.
            https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/accessing-monitoring.html#Overview.DBInstance.Status
        """

        def poke():
            return self.get_db_cluster_state(db_cluster_id)

        target_state = target_state.lower()
        if target_state in ("available", "deleted"):
            waiter = self.conn.get_waiter(f"db_cluster_{target_state}")  # type: ignore
            waiter.wait(
                DBClusterIdentifier=db_cluster_id,
                WaiterConfig={"Delay": check_interval, "MaxAttempts": max_attempts},
            )
        else:
            self._wait_for_state(poke, target_state, check_interval, max_attempts)
            self.log.info("DB cluster snapshot '%s' reached the '%s' state", db_cluster_id, target_state)

    def _wait_for_state(
        self,
        poke: Callable[..., str],
        target_state: str,
        check_interval: int,
        max_attempts: int,
    ) -> None:
        """
        Polls the poke function for the current state until it reaches the target_state.

        :param poke: A function that returns the current state of the target resource as a string.
        :param target_state: Wait until this state is reached
        :param check_interval: The amount of time in seconds to wait between attempts
        :param max_attempts: The maximum number of attempts to be made
        """
        state = poke()
        tries = 1
        while state != target_state:
            self.log.info("Current state is %s", state)
            if tries >= max_attempts:
                raise AirflowException("Max attempts exceeded")
            time.sleep(check_interval)
            state = poke()
            tries += 1
