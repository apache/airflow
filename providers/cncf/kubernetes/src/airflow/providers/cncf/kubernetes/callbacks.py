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

from enum import Enum
from typing import TYPE_CHECKING, TypeAlias

import kubernetes.client as k8s
import kubernetes_asyncio.client as async_k8s

if TYPE_CHECKING:
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    from airflow.sdk import Context

client_type: TypeAlias = k8s.CoreV1Api | async_k8s.CoreV1Api


class ExecutionMode(str, Enum):
    """Enum class for execution mode."""

    SYNC = "sync"
    ASYNC = "async"


class KubernetesPodOperatorCallback:
    """
    `KubernetesPodOperator` callbacks methods.

    Currently, the callbacks methods are not called in the async mode, this support will be added
    in the future.
    """

    @staticmethod
    def on_sync_client_creation(*, client: k8s.CoreV1Api, operator: KubernetesPodOperator, **kwargs) -> None:
        """
        Invoke this callback after creating the sync client.

        :param client: the created `kubernetes.client.CoreV1Api` client.
        """
        pass

    @staticmethod
    def on_pod_manifest_created(
        *,
        pod_request: k8s.V1Pod,
        client: client_type,
        mode: str,
        operator: KubernetesPodOperator,
        context: Context,
        **kwargs,
    ) -> None:
        """
        Invoke this callback after KPO creates the V1Pod manifest but before the pod is created.

        :param pod_request: the kubernetes pod manifest
        :param client: the Kubernetes client that can be used in the callback.
        :param mode: the current execution mode, it's one of (`sync`, `async`).
        """
        pass

    @staticmethod
    def on_pod_creation(
        *,
        pod: k8s.V1Pod,
        client: client_type,
        mode: str,
        operator: KubernetesPodOperator,
        context: Context,
        **kwargs,
    ) -> None:
        """
        Invoke this callback after creating the pod.

        :param pod: the created pod.
        :param client: the Kubernetes client that can be used in the callback.
        :param mode: the current execution mode, it's one of (`sync`, `async`).
        """
        pass

    @staticmethod
    def on_pod_starting(
        *,
        pod: k8s.V1Pod,
        client: client_type,
        mode: str,
        operator: KubernetesPodOperator,
        context: Context,
        **kwargs,
    ) -> None:
        """
        Invoke this callback when the pod starts.

        :param pod: the started pod.
        :param client: the Kubernetes client that can be used in the callback.
        :param mode: the current execution mode, it's one of (`sync`, `async`).
        """
        pass

    @staticmethod
    def on_pod_completion(
        *,
        pod: k8s.V1Pod,
        client: client_type,
        mode: str,
        operator: KubernetesPodOperator,
        context: Context,
        **kwargs,
    ) -> None:
        """
        Invoke this callback when the pod completes.

        :param pod: the completed pod.
        :param client: the Kubernetes client that can be used in the callback.
        :param mode: the current execution mode, it's one of (`sync`, `async`).
        """
        pass

    @staticmethod
    def on_pod_teardown(
        *,
        pod: k8s.V1Pod,
        client: client_type,
        mode: str,
        operator: KubernetesPodOperator,
        context: Context,
        **kwargs,
    ) -> None:
        """
        Invoke this callback after all pod completion callbacks but before the pod is deleted.

        :param pod: the completed pod.
        :param client: the Kubernetes client that can be used in the callback.
        :param mode: the current execution mode, it's one of (`sync`, `async`).
        """
        pass

    @staticmethod
    def on_pod_cleanup(
        *,
        pod: k8s.V1Pod,
        client: client_type,
        mode: str,
        operator: KubernetesPodOperator,
        context: Context,
        **kwargs,
    ):
        """
        Invoke this callback after cleaning/deleting the pod.

        :param pod: the completed pod.
        :param client: the Kubernetes client that can be used in the callback.
        :param mode: the current execution mode, it's one of (`sync`, `async`).
        """
        pass

    @staticmethod
    def on_operator_resuming(
        *,
        pod: k8s.V1Pod,
        event: dict,
        client: client_type,
        mode: str,
        operator: KubernetesPodOperator,
        context: Context,
        **kwargs,
    ) -> None:
        """
        Invoke this callback when resuming the `KubernetesPodOperator` from deferred state.

        :param pod: the current state of the pod.
        :param event: the returned event from the Trigger.
        :param client: the Kubernetes client that can be used in the callback.
        :param mode: the current execution mode, it's one of (`sync`, `async`).
        """
        pass

    @staticmethod
    def progress_callback(*, line: str, client: client_type, mode: str, **kwargs) -> None:
        """
        Invoke this callback to process pod container logs.

        :param line: the read line of log.
        :param client: the Kubernetes client that can be used in the callback.
        :param mode: the current execution mode, it's one of (`sync`, `async`).
        """
        pass
