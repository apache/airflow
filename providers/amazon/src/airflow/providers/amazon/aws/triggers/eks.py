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

import datetime
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.eks import EksHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger
from airflow.providers.amazon.aws.utils.waiter_with_logging import async_wait
from airflow.providers.cncf.kubernetes.triggers.pod import KubernetesPodTrigger
from airflow.providers.common.compat.sdk import AirflowException
from airflow.triggers.base import TriggerEvent

if TYPE_CHECKING:
    from pendulum import DateTime

    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class EksCreateClusterTrigger(AwsBaseWaiterTrigger):
    """
    Trigger for EksCreateClusterOperator.

    The trigger will asynchronously wait for the cluster to be created.

    :param cluster_name: The name of the EKS cluster
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: Which AWS region the connection should use.
         If this is None or empty then the default boto3 behaviour is used.
    """

    def __init__(
        self,
        cluster_name: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str | None,
        region_name: str | None = None,
    ):
        super().__init__(
            serialized_fields={"cluster_name": cluster_name, "region_name": region_name},
            waiter_name="cluster_active",
            waiter_args={"name": cluster_name},
            failure_message="Error checking Eks cluster",
            status_message="Eks cluster status is",
            status_queries=["cluster.status"],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def hook(self) -> AwsGenericHook:
        return EksHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

    async def run(self):
        async with await self.hook().get_async_conn() as client:
            waiter = client.get_waiter(self.waiter_name)
            try:
                await async_wait(
                    waiter,
                    self.waiter_delay,
                    self.attempts,
                    self.waiter_args,
                    self.failure_message,
                    self.status_message,
                    self.status_queries,
                )
            except AirflowException as exception:
                self.log.error("Error creating cluster: %s", exception)
                yield TriggerEvent({"status": "failed"})
            else:
                yield TriggerEvent({"status": "success"})


class EksPodTrigger(KubernetesPodTrigger):
    """
    KubernetesPodTrigger for EKS that generates fresh kubeconfig with new credentials.

    When ``EksPodOperator`` defers, the kubeconfig stored in ``config_dict`` contains
    an exec command that references a temporary credentials file. That file is cleaned
    up when the operator's context managers exit (on deferral). By the time the trigger
    runs — whether in a real triggerer process or inline via ``dag.test()`` — the file
    is gone.

    This trigger solves the problem by regenerating the kubeconfig with fresh AWS
    credentials before executing. The temporary files are kept alive for the entire
    duration of the trigger's ``run()`` method.

    :param eks_cluster_name: The name of the Amazon EKS Cluster.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region: Which AWS region the connection should use.
    """

    def __init__(
        self,
        *,
        eks_cluster_name: str,
        aws_conn_id: str | None = None,
        region: str | None = None,
        pod_name: str,
        pod_namespace: str,
        trigger_start_time: datetime.datetime,
        base_container_name: str,
        kubernetes_conn_id: str | None = None,
        connection_extras: dict | None = None,
        poll_interval: float = 2,
        cluster_context: str | None = None,
        config_dict: dict | None = None,
        in_cluster: bool | None = None,
        get_logs: bool = True,
        startup_timeout: int = 120,
        startup_check_interval: float = 5,
        schedule_timeout: int = 120,
        on_finish_action: str = "delete_pod",
        on_kill_action: str = "delete_pod",
        termination_grace_period: int | None = None,
        last_log_time: DateTime | None = None,
        logging_interval: int | None = None,
        trigger_kwargs: dict | None = None,
    ):
        super().__init__(
            pod_name=pod_name,
            pod_namespace=pod_namespace,
            trigger_start_time=trigger_start_time,
            base_container_name=base_container_name,
            kubernetes_conn_id=kubernetes_conn_id,
            connection_extras=connection_extras,
            poll_interval=poll_interval,
            cluster_context=cluster_context,
            config_dict=config_dict,
            in_cluster=in_cluster,
            get_logs=get_logs,
            startup_timeout=startup_timeout,
            startup_check_interval=startup_check_interval,
            schedule_timeout=schedule_timeout,
            on_finish_action=on_finish_action,
            on_kill_action=on_kill_action,
            termination_grace_period=termination_grace_period,
            last_log_time=last_log_time,
            logging_interval=logging_interval,
            trigger_kwargs=trigger_kwargs,
        )
        self.eks_cluster_name = eks_cluster_name
        self._aws_conn_id = aws_conn_id
        self.region = region

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize EksPodTrigger arguments and classpath."""
        _, kwargs = super().serialize()
        kwargs["eks_cluster_name"] = self.eks_cluster_name
        kwargs["aws_conn_id"] = self._aws_conn_id
        kwargs["region"] = self.region
        return (
            "airflow.providers.amazon.aws.triggers.eks.EksPodTrigger",
            kwargs,
        )

    async def run(self):
        """Generate fresh kubeconfig, then delegate to the parent trigger."""
        from airflow.utils import yaml

        eks_hook = EksHook(
            aws_conn_id=self._aws_conn_id,
            region_name=self.region,
        )
        session = eks_hook.get_session()
        credentials_obj = session.get_credentials()
        if credentials_obj is None:
            raise RuntimeError(
                "Unable to retrieve AWS credentials for EKS trigger. "
                "Credentials may have expired or not been configured."
            )
        credentials = credentials_obj.get_frozen_credentials()

        # Create fresh credential and kubeconfig files.  The context managers
        # keep the temp files alive for the entire duration of the trigger.
        with eks_hook._secure_credential_context(
            credentials.access_key, credentials.secret_key, credentials.token
        ) as credentials_file:
            with eks_hook.generate_config_file(
                eks_cluster_name=self.eks_cluster_name,
                pod_namespace=self.pod_namespace,
                credentials_file=credentials_file,
            ) as config_file_path:
                # Reading a small local temp file created by the context manager above.
                # Blocking I/O is acceptable here as the file is tiny and local.
                from pathlib import Path

                self.config_dict = yaml.safe_load(
                    Path(config_file_path).read_text()  # noqa: ASYNC240
                )

                # Invalidate any previously cached hook so the new config_dict
                # is picked up when the parent creates the AsyncKubernetesHook.
                self.__dict__.pop("hook", None)

                async for event in super().run():
                    yield event


class EksDeleteClusterTrigger(AwsBaseWaiterTrigger):
    """
    Trigger for EksDeleteClusterOperator.

    The trigger will asynchronously wait for the cluster to be deleted. If there are
    any nodegroups or fargate profiles associated with the cluster, they will be deleted
    before the cluster is deleted.

    :param cluster_name: The name of the EKS cluster
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: Which AWS region the connection should use.
         If this is None or empty then the default boto3 behaviour is used.
    :param force_delete_compute: If True, any nodegroups or fargate profiles associated
        with the cluster will be deleted before the cluster is deleted.
    """

    def __init__(
        self,
        cluster_name,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str | None,
        region_name: str | None,
        force_delete_compute: bool,
    ):
        self.cluster_name = cluster_name
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.force_delete_compute = force_delete_compute

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "cluster_name": self.cluster_name,
                "waiter_delay": str(self.waiter_delay),
                "waiter_max_attempts": str(self.waiter_max_attempts),
                "aws_conn_id": self.aws_conn_id,
                "region_name": self.region_name,
                "force_delete_compute": self.force_delete_compute,
            },
        )

    def hook(self) -> AwsGenericHook:
        return EksHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

    async def run(self):
        async with await self.hook().get_async_conn() as client:
            waiter = client.get_waiter("cluster_deleted")
            if self.force_delete_compute:
                await self.delete_any_nodegroups(client=client)
                await self.delete_any_fargate_profiles(client=client)
            try:
                await client.delete_cluster(name=self.cluster_name)
            except ClientError as ex:
                if ex.response.get("Error").get("Code") == "ResourceNotFoundException":
                    pass
                else:
                    raise
            await async_wait(
                waiter=waiter,
                waiter_delay=int(self.waiter_delay),
                waiter_max_attempts=int(self.waiter_max_attempts),
                args={"name": self.cluster_name},
                failure_message="Error deleting cluster",
                status_message="Status of cluster is",
                status_args=["cluster.status"],
            )

        yield TriggerEvent({"status": "deleted"})

    async def delete_any_nodegroups(self, client) -> None:
        """
        Delete all EKS Nodegroups for a provided Amazon EKS Cluster.

        All the EKS Nodegroups are deleted simultaneously. We wait for
        all Nodegroups to be deleted before returning.
        """
        nodegroups = await client.list_nodegroups(clusterName=self.cluster_name)
        if nodegroups.get("nodegroups", None):
            self.log.info("Deleting nodegroups")
            waiter = self.hook().get_waiter("all_nodegroups_deleted", deferrable=True, client=client)
            for group in nodegroups["nodegroups"]:
                await client.delete_nodegroup(clusterName=self.cluster_name, nodegroupName=group)
            await async_wait(
                waiter=waiter,
                waiter_delay=int(self.waiter_delay),
                waiter_max_attempts=int(self.waiter_max_attempts),
                args={"clusterName": self.cluster_name},
                failure_message=f"Error deleting nodegroup for cluster {self.cluster_name}",
                status_message="Deleting nodegroups associated with the cluster",
                status_args=["nodegroups"],
            )
            self.log.info("All nodegroups deleted")
        else:
            self.log.info("No nodegroups associated with cluster %s", self.cluster_name)

    async def delete_any_fargate_profiles(self, client) -> None:
        """
        Delete all EKS Fargate profiles for a provided Amazon EKS Cluster.

        EKS Fargate profiles must be deleted one at a time, so we must wait
        for one to be deleted before sending the next delete command.
        """
        fargate_profiles = await client.list_fargate_profiles(clusterName=self.cluster_name)
        if fargate_profiles.get("fargateProfileNames"):
            self.log.info("Waiting for Fargate profiles to delete.  This will take some time.")
            for profile in fargate_profiles["fargateProfileNames"]:
                await client.delete_fargate_profile(clusterName=self.cluster_name, fargateProfileName=profile)
                await async_wait(
                    waiter=client.get_waiter("fargate_profile_deleted"),
                    waiter_delay=int(self.waiter_delay),
                    waiter_max_attempts=int(self.waiter_max_attempts),
                    args={"clusterName": self.cluster_name, "fargateProfileName": profile},
                    failure_message=f"Error deleting fargate profile for cluster {self.cluster_name}",
                    status_message="Status of fargate profile is",
                    status_args=["fargateProfile.status"],
                )
            self.log.info("All Fargate profiles deleted")
        else:
            self.log.info("No Fargate profiles associated with cluster %s", self.cluster_name)


class EksCreateFargateProfileTrigger(AwsBaseWaiterTrigger):
    """
    Asynchronously wait for the fargate profile to be created.

    :param cluster_name: The name of the EKS cluster
    :param fargate_profile_name: The name of the fargate profile
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        cluster_name: str,
        fargate_profile_name: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str | None,
        region_name: str | None = None,
    ):
        super().__init__(
            serialized_fields={"cluster_name": cluster_name, "fargate_profile_name": fargate_profile_name},
            waiter_name="fargate_profile_active",
            waiter_args={"clusterName": cluster_name, "fargateProfileName": fargate_profile_name},
            failure_message="Failure while creating Fargate profile",
            status_message="Fargate profile not created yet",
            status_queries=["fargateProfile.status"],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def hook(self) -> AwsGenericHook:
        return EksHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)


class EksDeleteFargateProfileTrigger(AwsBaseWaiterTrigger):
    """
    Asynchronously wait for the fargate profile to be deleted.

    :param cluster_name: The name of the EKS cluster
    :param fargate_profile_name: The name of the fargate profile
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        cluster_name: str,
        fargate_profile_name: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str | None,
        region_name: str | None = None,
    ):
        super().__init__(
            serialized_fields={"cluster_name": cluster_name, "fargate_profile_name": fargate_profile_name},
            waiter_name="fargate_profile_deleted",
            waiter_args={"clusterName": cluster_name, "fargateProfileName": fargate_profile_name},
            failure_message="Failure while deleting Fargate profile",
            status_message="Fargate profile not deleted yet",
            status_queries=["fargateProfile.status"],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def hook(self) -> AwsGenericHook:
        return EksHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)


class EksCreateNodegroupTrigger(AwsBaseWaiterTrigger):
    """
    Trigger for EksCreateNodegroupOperator.

    The trigger will asynchronously poll the boto3 API and wait for the
    nodegroup to be in the state specified by the waiter.

    :param cluster_name: The name of the EKS cluster associated with the node group.
    :param nodegroup_name: The name of the nodegroup to check.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    """

    def __init__(
        self,
        cluster_name: str,
        nodegroup_name: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str | None,
        region_name: str | None = None,
    ):
        super().__init__(
            serialized_fields={
                "cluster_name": cluster_name,
                "nodegroup_name": nodegroup_name,
                "region_name": region_name,
            },
            waiter_name="nodegroup_active",
            waiter_args={"clusterName": cluster_name, "nodegroupName": nodegroup_name},
            failure_message="Error creating nodegroup",
            status_message="Nodegroup status is",
            status_queries=["nodegroup.status", "nodegroup.health.issues"],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def hook(self) -> AwsGenericHook:
        return EksHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)


class EksDeleteNodegroupTrigger(AwsBaseWaiterTrigger):
    """
    Trigger for EksDeleteNodegroupOperator.

    The trigger will asynchronously poll the boto3 API and wait for the
    nodegroup to be in the state specified by the waiter.

    :param cluster_name: The name of the EKS cluster associated with the node group.
    :param nodegroup_name: The name of the nodegroup to check.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    """

    def __init__(
        self,
        cluster_name: str,
        nodegroup_name: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str | None,
        region_name: str | None = None,
    ):
        super().__init__(
            serialized_fields={"cluster_name": cluster_name, "nodegroup_name": nodegroup_name},
            waiter_name="nodegroup_deleted",
            waiter_args={"clusterName": cluster_name, "nodegroupName": nodegroup_name},
            failure_message="Error deleting nodegroup",
            status_message="Nodegroup status is",
            status_queries=["nodegroup.status", "nodegroup.health.issues"],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def hook(self) -> AwsGenericHook:
        return EksHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
