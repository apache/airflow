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
from http import HTTPStatus
from typing import TYPE_CHECKING

from airflow.executors.executor_loader import ExecutorLoader
from airflow.providers.amazon.aws.executors.eks.utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    AllEksConfigKeys,
)
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_2_PLUS
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, conf

if TYPE_CHECKING:
    from kubernetes.client import models as k8s

    from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types import KubernetesJob

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

_FACTORY_MODULE = "airflow.providers.amazon.aws.executors.eks._client_factory"
_CLIENT_FACTORY_PATH = f"{_FACTORY_MODULE}._get_eks_kube_client"
_ASYNC_CLIENT_FACTORY_PATH = f"{_FACTORY_MODULE}._get_eks_async_kube_client"
# Airflow 2 hybrid executors that wrap a KubernetesExecutor.
_HYBRID_KUBERNETES_EXECUTORS = ("CeleryKubernetesExecutor", "LocalKubernetesExecutor")


class AwsEksExecutor(KubernetesExecutor):
    """
    A KubernetesExecutor that authenticates against an Amazon EKS cluster.

    Builds the Kubernetes client from ``[aws_eks_executor]`` configuration and keeps the
    short-lived EKS token fresh in-process. All pod-level behaviour comes unchanged from the
    KubernetesExecutor and its ``[kubernetes_executor]`` configuration.
    """

    supports_multi_team: bool = True

    def __init__(self, *args, **kwargs):
        self._validate_eks_config()
        super().__init__(*args, **kwargs)
        # Both need team_name, which super().__init__ sets.
        self._check_no_kubernetes_executor_in_scope()
        self._ensure_client_factory()

    def start(self) -> None:
        """Call this when the Executor is run for the first time by the scheduler."""
        super().start()
        if TYPE_CHECKING:
            assert self.kube_scheduler
        # Both pod creation paths build every worker pod here, so check the image before it is sent.
        build_pod_request = self.kube_scheduler._build_pod_request

        def _build_pod_request_with_image(job: KubernetesJob) -> k8s.V1Pod:
            pod = build_pod_request(job)
            self._check_worker_image(pod)
            return pod

        self.kube_scheduler._build_pod_request = _build_pod_request_with_image  # type: ignore[method-assign,assignment]

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

    def _check_worker_image(self, pod: k8s.V1Pod) -> None:
        if pod.spec.containers and pod.spec.containers[0].image:
            return
        section = (
            f"team {self.team_name}'s [kubernetes_executor]" if self.team_name else "[kubernetes_executor]"
        )
        annotations = pod.metadata.annotations or {}
        # Raised as the 422 the API server would return, so KubernetesExecutor fails the task.
        raise ApiException(
            status=HTTPStatus.UNPROCESSABLE_ENTITY,
            reason=(
                f"Task {annotations.get('dag_id')}.{annotations.get('task_id')} has no worker image, so its "
                "pod was not created. Set an image in one of three ways: on the base container of the pod "
                f"template ({section} pod_template_file, or the task's executor_config pod_template_file), "
                f"in the task's executor_config pod_override, or with {section} "
                "worker_container_repository and worker_container_tag."
            ),
        )

    @staticmethod
    def _validate_eks_config() -> None:
        if not conf.get(CONFIG_GROUP_NAME, AllEksConfigKeys.CLUSTER_NAME, fallback=None):
            raise ValueError(
                f"AwsEksExecutor requires [{CONFIG_GROUP_NAME}] cluster_name to be set. "
                "Set it in airflow.cfg or with AIRFLOW__AWS_EKS_EXECUTOR__CLUSTER_NAME."
            )

    def _check_no_kubernetes_executor_in_scope(self) -> None:
        executor_names = (
            ExecutorLoader.get_executor_names(validate_teams=False)
            if AIRFLOW_V_3_2_PLUS
            else ExecutorLoader.get_executor_names()
        )
        for executor_name in executor_names:
            # ExecutorName has no team_name before Airflow 3.1.
            if getattr(executor_name, "team_name", None) != self.team_name:
                continue
            executor_cls, _ = ExecutorLoader.import_executor_cls(executor_name)
            if issubclass(executor_cls, AwsEksExecutor) or not (
                issubclass(executor_cls, KubernetesExecutor)
                or executor_cls.__name__ in _HYBRID_KUBERNETES_EXECUTORS
            ):
                continue
            scope = f"team {self.team_name}" if self.team_name else "the global scope"
            raise ValueError(
                f"AwsEksExecutor can't run alongside {executor_cls.__name__} in {scope}. AwsEksExecutor "
                f"sets the [kubernetes_executor] client factory for {scope}, so {executor_cls.__name__} "
                "would silently send its pods to the EKS cluster. Configure the two executors for "
                "different teams, or don't use them together. "
                "See https://github.com/apache/airflow/issues/74844"
            )

    def _ensure_client_factory(self) -> None:
        # A team's factory is looked up in team config only, with no global fallback.
        team_name = self.team_name
        team_kwargs = {"team_name": team_name} if team_name else {}
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
                # An env var, not conf.set, so the spawned pod watcher process sees it too. A plain
                # KubernetesExecutor in the same scope reads it as well, which
                # _check_no_kubernetes_executor_in_scope refuses; replacing this env var is tracked at
                # https://github.com/apache/airflow/issues/74844
                os.environ[f"{env_var_prefix}KUBERNETES_EXECUTOR__{key.upper()}"] = path
