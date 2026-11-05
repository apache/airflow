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
"""AwsEksExecutor: run Airflow tasks as pods on an Amazon EKS cluster."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.executors.eks.utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    AllEksConfigKeys,
    get_cluster_name_env_var,
)
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

_CNCF_KUBERNETES_REQUIRED = (
    "AwsEksExecutor requires the apache-airflow-providers-cncf-kubernetes version that the "
    "Amazon provider's cncf.kubernetes extra installs: "
    "pip install 'apache-airflow-providers-amazon[cncf.kubernetes]'"
)

try:
    from kubernetes.client.rest import ApiException

    from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import KubernetesExecutor
except ImportError as e:
    raise AirflowOptionalProviderFeatureException(_CNCF_KUBERNETES_REQUIRED) from e

# Check for the feature, not the version, since unreleased trees report the previous version.
if not hasattr(KubernetesExecutor, "client_factory"):
    raise AirflowOptionalProviderFeatureException(_CNCF_KUBERNETES_REQUIRED)

_FACTORY_MODULE = "airflow.providers.amazon.aws.executors.eks._client_factory"


class AwsEksExecutor(KubernetesExecutor):
    """
    A KubernetesExecutor that authenticates against an Amazon EKS cluster.

    Builds the Kubernetes client from ``[aws_eks_executor]`` configuration and keeps the
    short-lived EKS token fresh in-process. All pod-level behaviour comes unchanged from the
    KubernetesExecutor and its ``[kubernetes_executor]`` configuration.
    """

    supports_multi_team: bool = True
    client_factory = f"{_FACTORY_MODULE}._get_eks_kube_client"
    async_client_factory = f"{_FACTORY_MODULE}._get_eks_async_kube_client"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Needs team_name, which super().__init__ sets.
        self._validate_eks_config()

    def start(self) -> None:
        """Call this when the Executor is run for the first time by the scheduler."""
        super().start()
        check_health = self.conf.getboolean(
            CONFIG_GROUP_NAME,
            AllEksConfigKeys.CHECK_HEALTH_ON_STARTUP,
            fallback=CONFIG_DEFAULTS[AllEksConfigKeys.CHECK_HEALTH_ON_STARTUP],
        )

        if not check_health:
            return

        self.log.info("Starting EKS Executor and determining health...")
        try:
            self.check_health()
        except RuntimeError:
            self.log.error("Stopping the Airflow Scheduler from starting until the issue is resolved.")
            raise

    def check_health(self) -> None:
        """
        Make a test Kubernetes API call to check the health of the EKS Executor.

        Building the client only proves the cluster exists; without this, a missing EKS access
        entry or RBAC binding only shows up later as a watcher error loop.
        """
        if TYPE_CHECKING:
            assert self.kube_client
        namespace = self.kube_config.kube_namespace
        try:
            self.kube_client.list_namespaced_pod(namespace, limit=1)
        except ApiException as e:
            raise RuntimeError(
                f"EKS Executor health check has failed because: cannot list pods in namespace {namespace} "
                f"({e.status} {e.reason}). The IAM principal behind the AWS connection needs an EKS access "
                "entry (or aws-auth mapping) on the cluster that allows create, get, list, watch, patch and "
                f"delete on pods and get on pods/log in namespace {namespace}. See 'Grant access to the "
                "cluster' in the AwsEksExecutor docs."
            ) from e
        self.log.info("EKS Executor health check has succeeded.")

    def _validate_eks_config(self) -> None:
        if not self.conf.get(CONFIG_GROUP_NAME, AllEksConfigKeys.CLUSTER_NAME, fallback=None):
            raise ValueError(
                f"AwsEksExecutor requires [{CONFIG_GROUP_NAME}] cluster_name to be set. "
                f"Set it in airflow.cfg or with {get_cluster_name_env_var(self.team_name)}."
            )
