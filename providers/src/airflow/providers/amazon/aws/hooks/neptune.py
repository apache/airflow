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

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class NeptuneHook(AwsBaseHook):
    """
    Interact with Amazon Neptune.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    AVAILABLE_STATES = ["available"]
    STOPPED_STATES = ["stopped"]
    ERROR_STATES = [
        "cloning-failed",
        "inaccessible-encryption-credentials",
        "inaccessible-encryption-credentials-recoverable",
        "migration-failed",
    ]

    def __init__(self, *args, **kwargs):
        kwargs["client_type"] = "neptune"
        super().__init__(*args, **kwargs)

    def wait_for_cluster_availability(
        self, cluster_id: str, delay: int = 30, max_attempts: int = 60
    ) -> str:
        """
        Wait for Neptune cluster to start.

        :param cluster_id: The ID of the cluster to wait for.
        :param delay: Time in seconds to delay between polls.
        :param max_attempts: Maximum number of attempts to poll for completion.
        :return: The status of the cluster.
        """
        self.get_waiter("cluster_available").wait(
            DBClusterIdentifier=cluster_id,
            WaiterConfig={"Delay": delay, "MaxAttempts": max_attempts},
        )

        status = self.get_cluster_status(cluster_id)
        self.log.info(
            "Finished waiting for cluster %s. Status is now %s", cluster_id, status
        )

        return status

    def wait_for_cluster_stopped(
        self, cluster_id: str, delay: int = 30, max_attempts: int = 60
    ) -> str:
        """
        Wait for Neptune cluster to stop.

        :param cluster_id: The ID of the cluster to wait for.
        :param delay: Time in seconds to delay between polls.
        :param max_attempts: Maximum number of attempts to poll for completion.
        :return: The status of the cluster.
        """
        self.get_waiter("cluster_stopped").wait(
            DBClusterIdentifier=cluster_id,
            WaiterConfig={"Delay": delay, "MaxAttempts": max_attempts},
        )

        status = self.get_cluster_status(cluster_id)
        self.log.info(
            "Finished waiting for cluster %s. Status is now %s", cluster_id, status
        )

        return status

    def get_cluster_status(self, cluster_id: str) -> str:
        """
        Get the status of a Neptune cluster.

        :param cluster_id: The ID of the cluster to get the status of.
        :return: The status of the cluster.
        """
        return self.conn.describe_db_clusters(DBClusterIdentifier=cluster_id)[
            "DBClusters"
        ][0]["Status"]

    def get_db_instance_status(self, instance_id: str) -> str:
        """
        Get the status of a Neptune instance.

        :param instance_id: The ID of the instance to get the status of.
        :return: The status of the instance.
        """
        return self.conn.describe_db_instances(DBInstanceIdentifier=instance_id)[
            "DBInstances"
        ][0]["DBInstanceStatus"]

    def wait_for_cluster_instance_availability(
        self, cluster_id: str, delay: int = 30, max_attempts: int = 60
    ) -> None:
        """
        Wait for Neptune instances in a cluster to be available.

        :param cluster_id: The cluster ID of the instances to wait for.
        :param delay: Time in seconds to delay between polls.
        :param max_attempts: Maximum number of attempts to poll for completion.
        :return: The status of the instances.
        """
        filters = [{"Name": "db-cluster-id", "Values": [cluster_id]}]
        self.log.info("Waiting for instances in cluster %s.", cluster_id)
        self.get_waiter("db_instance_available").wait(
            Filters=filters, WaiterConfig={"Delay": delay, "MaxAttempts": max_attempts}
        )
        self.log.info("Finished waiting for instances in cluster %s.", cluster_id)
