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

import os
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.executors.eks.utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    AllEksConfigKeys,
)
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, conf

_CNCF_KUBERNETES_REQUIRED = "AwsEksExecutor requires apache-airflow-providers-cncf-kubernetes>=10.24.0"

try:
    from kubernetes.client.rest import ApiException

    from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import KubernetesExecutor
    from airflow.providers.cncf.kubernetes.get_provider_info import (
        get_provider_info as get_cncf_kubernetes_provider_info,
    )
except ImportError as e:
    raise AirflowOptionalProviderFeatureException(_CNCF_KUBERNETES_REQUIRED) from e

# Check for the option, not the version, since unreleased trees report the previous version.
if "client_factory" not in (
    get_cncf_kubernetes_provider_info().get("config", {}).get("kubernetes_executor", {}).get("options", {})
):
    raise AirflowOptionalProviderFeatureException(_CNCF_KUBERNETES_REQUIRED)

# Import paths, because cncf.kubernetes re-resolves the factories in each process (see _client_factory).
_FACTORY_MODULE = "airflow.providers.amazon.aws.executors.eks._client_factory"
_CLIENT_FACTORY_PATH = f"{_FACTORY_MODULE}._get_eks_kube_client"
_ASYNC_CLIENT_FACTORY_PATH = f"{_FACTORY_MODULE}._get_eks_async_kube_client"


class AwsEksExecutor(KubernetesExecutor):
    """
    A KubernetesExecutor that authenticates against an Amazon EKS cluster.

    Builds the Kubernetes client from ``[aws_eks_executor]`` configuration and keeps the
    short-lived EKS token fresh in-process. All pod-level behaviour comes unchanged from the
    KubernetesExecutor and its ``[kubernetes_executor]`` configuration.
    """

    # Like the KubernetesExecutor, teams share the cluster from the global config and get their
    # own team-scoped [kubernetes_executor] settings.
    supports_multi_team: bool = True

    def __init__(self, *args, **kwargs):
        self._validate_eks_config()
        super().__init__(*args, **kwargs)
        # After super().__init__, which sets team_name; clients are only built later, in start().
        self._ensure_client_factory()

    def start(self) -> None:
        """Call this when the Executor is run for the first time by the scheduler."""
        super().start()
        check_health = conf.getboolean(
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

    @staticmethod
    def _validate_eks_config() -> None:
        if not conf.get(CONFIG_GROUP_NAME, AllEksConfigKeys.CLUSTER_NAME, fallback=None):
            raise ValueError(f"AwsEksExecutor requires [{CONFIG_GROUP_NAME}] cluster_name to be set")

    def _ensure_client_factory(self) -> None:
        # cncf.kubernetes looks a team's factory up in the team's own config only, with no
        # fallback to the global section, so a team executor has to set the team-scoped one.
        team_name = self.team_name
        team_kwargs = {"team_name": team_name} if team_name else {}
        # Team-scoped variables are named AIRFLOW__<TEAM>___<SECTION>__<KEY>.
        env_var_prefix = f"AIRFLOW__{team_name.upper()}___" if team_name else "AIRFLOW__"
        for key, path in (
            ("client_factory", _CLIENT_FACTORY_PATH),
            ("async_client_factory", _ASYNC_CLIENT_FACTORY_PATH),
        ):
            configured = conf.get("kubernetes_executor", key, fallback=None, **team_kwargs)
            if configured and configured != path:
                raise ValueError(
                    f"AwsEksExecutor sets [kubernetes_executor] {key} itself, so leave it unset; "
                    f"it is set to {configured}"
                )
            if not configured:
                # Environment variable rather than an in-memory conf.set so the setting also
                # reaches the pod watcher subprocess, which re-reads configuration under the
                # spawn start method.
                os.environ[f"{env_var_prefix}KUBERNETES_EXECUTOR__{key.upper()}"] = path
