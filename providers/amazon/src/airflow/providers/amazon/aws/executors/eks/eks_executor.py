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

from airflow.providers.common.compat.sdk import conf

try:
    from kubernetes.client.rest import ApiException

    from airflow.providers.cncf.kubernetes import __version__ as cncf_kubernetes_version
    from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import KubernetesExecutor
    from airflow.providers.cncf.kubernetes.get_provider_info import (
        get_provider_info as get_cncf_kubernetes_provider_info,
    )
except ImportError as e:
    raise ImportError(
        "AwsEksExecutor requires the cncf.kubernetes provider; install it with "
        "pip install 'apache-airflow-providers-amazon[cncf.kubernetes]'"
    ) from e

from airflow.providers.amazon.aws.executors.eks._client_factory import CONFIG_GROUP_NAME

# Import paths, because cncf.kubernetes re-resolves the factories in each process (see _client_factory).
_FACTORY_MODULE = "airflow.providers.amazon.aws.executors.eks._client_factory"
_CLIENT_FACTORY_PATH = f"{_FACTORY_MODULE}._get_eks_kube_client"
_ASYNC_CLIENT_FACTORY_PATH = f"{_FACTORY_MODULE}._get_eks_async_kube_client"
# TODO: confirm once the cncf.kubernetes release carrying the client_factory seam is cut.
# Only used in the error message below. The check looks for the client_factory option instead of
# comparing versions, because an unreleased source tree still reports the previous release.
MIN_CNCF_KUBERNETES_VERSION = "10.24.0"


class AwsEksExecutor(KubernetesExecutor):
    """
    A KubernetesExecutor that authenticates against an Amazon EKS cluster.

    Builds the Kubernetes client from ``[aws_eks_executor]`` configuration and keeps the
    short-lived EKS token fresh in-process. All pod-level behaviour comes unchanged from the
    KubernetesExecutor and its ``[kubernetes_executor]`` configuration.
    """

    # The client factories read the un-prefixed [aws_eks_executor] section, so every team
    # would land on the same cluster.
    supports_multi_team: bool = False

    def __init__(self, *args, **kwargs):
        self._validate_eks_config()
        self._require_client_factory_support()
        self._ensure_client_factory()
        super().__init__(*args, **kwargs)

    def start(self) -> None:
        super().start()
        self._check_health()

    def _check_health(self) -> None:
        # Building the client only proves the cluster exists; without this, a missing EKS access
        # entry or RBAC binding only shows up later as a watcher error loop.
        if TYPE_CHECKING:
            assert self.kube_client
        namespace = self.kube_config.kube_namespace
        try:
            self.kube_client.list_namespaced_pod(namespace, limit=1)
        except ApiException as e:
            raise RuntimeError(
                f"AwsEksExecutor health check failed: cannot list pods in namespace {namespace} "
                f"({e.status} {e.reason}). Check the EKS access entry and RBAC for the executor's IAM role."
            ) from e
        self.log.info("AwsEksExecutor health check succeeded.")

    @staticmethod
    def _require_client_factory_support() -> None:
        # A cncf.kubernetes without the seam imports fine but never reads client_factory, so it
        # would build a default client from kubeconfig and fail later as an authentication error.
        provider_config = get_cncf_kubernetes_provider_info().get("config", {})
        if "client_factory" not in provider_config.get("kubernetes_executor", {}).get("options", {}):
            raise ImportError(
                "AwsEksExecutor requires apache-airflow-providers-cncf-kubernetes>="
                f"{MIN_CNCF_KUBERNETES_VERSION}, which honors the [kubernetes_executor] "
                f"client_factory setting. The installed {cncf_kubernetes_version} ignores it."
            )

    @staticmethod
    def _validate_eks_config() -> None:
        if not conf.get(CONFIG_GROUP_NAME, "cluster_name", fallback=None):
            raise ValueError(f"AwsEksExecutor requires [{CONFIG_GROUP_NAME}] cluster_name to be set")

    @staticmethod
    def _ensure_client_factory() -> None:
        for key, path in (
            ("client_factory", _CLIENT_FACTORY_PATH),
            ("async_client_factory", _ASYNC_CLIENT_FACTORY_PATH),
        ):
            configured = conf.get("kubernetes_executor", key, fallback=None)
            if configured and configured != path:
                raise ValueError(
                    f"AwsEksExecutor sets [kubernetes_executor] {key} itself, so leave it unset; "
                    f"it is set to {configured}"
                )
            if not configured:
                # Environment variable rather than an in-memory conf.set so the setting also
                # reaches the pod watcher subprocess, which re-reads configuration under the
                # spawn start method.
                os.environ[f"AIRFLOW__KUBERNETES_EXECUTOR__{key.upper()}"] = path
