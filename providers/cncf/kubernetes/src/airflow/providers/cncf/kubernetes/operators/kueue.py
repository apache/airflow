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
"""Manage a Kubernetes Kueue."""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from kubernetes.utils import FailToCreateError

from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.providers.cncf.kubernetes.triggers.kueue import (
    JOB_NAME_LABEL,
    JOB_STATE_MESSAGES,
    KubernetesKueueJobTrigger,
    KueueJobState,
    build_job_event,
    collect_xcom_results,
    get_job_state,
    get_succeeded_pod_xcoms,
)
from airflow.providers.cncf.kubernetes.version_compat import AIRFLOW_V_3_1_PLUS
from airflow.providers.common.compat.sdk import AirflowException

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk import BaseOperator
else:
    from airflow.models import BaseOperator

if TYPE_CHECKING:
    from airflow.sdk import Context


class KubernetesInstallKueueOperator(BaseOperator):
    """
    Installs a Kubernetes Kueue.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:KubernetesInstallKueueOperator`

    :param kueue_version: The Kubernetes Kueue version to install.
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the Kubernetes cluster.
    """

    template_fields: Sequence[str] = (
        "kueue_version",
        "kubernetes_conn_id",
    )

    def __init__(self, kueue_version: str, kubernetes_conn_id: str = "kubernetes_default", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kubernetes_conn_id = kubernetes_conn_id
        self.kueue_version = kueue_version

    @property
    def _kueue_yaml_url(self) -> str:
        return (
            f"https://github.com/kubernetes-sigs/kueue/releases/download/{self.kueue_version}/manifests.yaml"
        )

    @cached_property
    def hook(self) -> KubernetesHook:
        return KubernetesHook(conn_id=self.kubernetes_conn_id)

    def execute(self, context):
        yaml_objects = self.hook.get_yaml_content_from_file(kueue_yaml_url=self._kueue_yaml_url)
        try:
            self.hook.apply_from_yaml_file(yaml_objects=yaml_objects)
        except FailToCreateError as ex:
            error_bodies = []
            for e in ex.api_exceptions:
                try:
                    if e.body:
                        error_bodies.append(json.loads(e.body))
                    else:
                        # If no body content, use reason as the message
                        reason = getattr(e, "reason", "Unknown")
                        error_bodies.append({"message": reason, "reason": reason})
                except (json.JSONDecodeError, ValueError, TypeError):
                    # If the body is a string (e.g., in a 429 error), it can't be parsed as JSON.
                    # Use the body directly as the message instead.
                    error_bodies.append({"message": e.body, "reason": getattr(e, "reason", "Unknown")})
            if next((e for e in error_bodies if e.get("reason") == "AlreadyExists"), None):
                self.log.info("Kueue is already enabled for the cluster")

            if errors := [e for e in error_bodies if e.get("reason") != "AlreadyExists"]:
                error_message = "\n".join(
                    e.get("message") or e.get("body") or f"Unknown error: {e.get('reason', 'Unknown')}"
                    for e in errors
                )
                raise AirflowException(error_message)
            return

        self.hook.check_kueue_deployment_running(name="kueue-controller-manager", namespace="kueue-system")
        self.log.info("Kueue installed successfully!")


class KubernetesStartKueueJobOperator(KubernetesJobOperator):
    """
    Executes a Kubernetes Job in Kueue.

    With ``wait_until_job_complete=True``, the operator polls the Job itself until it is finished, instead
    of waiting on the pods found when the Job started: Kueue keeps the Job suspended without pods until it
    admits it, and may preempt it (suspending it again and deleting its pods) then admit it again any
    number of times. Logs and XCom are read from the pods the Job has once it is finished.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:KubernetesStartKueueJobOperator`

    :param queue_name: The name of the Queue in the cluster
    """

    template_fields = tuple({"queue_name"} | set(KubernetesJobOperator.template_fields))

    def __init__(self, queue_name: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.queue_name = queue_name

        self.suspend: bool
        if self.suspend is False:
            raise AirflowException(
                "The `suspend` parameter can't be False. If you want to use Kueue for running Job"
                " in a Kubernetes cluster, set the `suspend` parameter to True.",
            )
        if self.suspend is None:
            self.log.info(
                "You have not set parameter `suspend` in class %s. "
                "For running a Job in Kueue the `suspend` parameter has been set to True.",
                self.__class__.__name__,
            )
            self.suspend = True
        self.labels.update({"kueue.x-k8s.io/queue-name": self.queue_name})
        self.annotations.update({"kueue.x-k8s.io/queue-name": self.queue_name})

    def execute(self, context: Context):
        if not self.wait_until_job_complete:
            return super().execute(context)

        self.name = self._set_name(self.name)
        self.job_request_obj = self.build_job_request_obj(context)
        self.job = self.create_job(job_request_obj=self.job_request_obj)  # must set `self.job` for `on_kill`

        ti = context["ti"]
        ti.xcom_push(key="job_name", value=self.job.metadata.name)
        ti.xcom_push(key="job_namespace", value=self.job.metadata.namespace)

        if self.deferrable:
            self.defer(
                trigger=KubernetesKueueJobTrigger(
                    job_name=self.job.metadata.name,
                    job_namespace=self.job.metadata.namespace,
                    base_container_name=self.base_container_name,
                    kubernetes_conn_id=self.kubernetes_conn_id,
                    cluster_context=self.cluster_context,
                    config_file=self.config_file,
                    in_cluster=self.in_cluster,
                    poll_interval=self.job_poll_interval,
                    do_xcom_push=self.do_xcom_push,
                ),
                method_name="execute_complete",
            )

        return self.execute_complete(
            context, self._wait_until_job_finished(self.job.metadata.name, self.job.metadata.namespace)
        )

    def _wait_until_job_finished(self, job_name: str, namespace: str) -> dict[str, Any]:
        """Poll the Job until it is finished, and return the event ``KubernetesKueueJobTrigger`` yields."""
        state: KueueJobState | None = None
        xcom_results: dict[str, str] = {}
        while True:
            job = self.hook.get_job_status(job_name=job_name, namespace=namespace)
            if (new_state := get_job_state(job)) != state:
                state = new_state
                self.log.info("The job '%s' %s", job_name, JOB_STATE_MESSAGES[state])
            if state == KueueJobState.FINISHED:
                return build_job_event(job, xcom_results)
            if state == KueueJobState.RUNNING and self.do_xcom_push:
                collect_xcom_results(
                    self.client, self.pod_manager, job_name, namespace, self.base_container_name, xcom_results
                )
            time.sleep(self.job_poll_interval)

    def execute_complete(self, context: Context, event: dict, **kwargs):
        # The pods found before a preemption by Kueue were deleted: read the ones the Job has now
        pods = self.client.list_namespaced_pod(
            namespace=event["namespace"], label_selector=f"{JOB_NAME_LABEL}={event['name']}"
        ).items
        for pod in pods:
            self.log_matching_pod(pod=pod, context=context)
        event = {
            **event,
            "pod_names": [pod.metadata.name for pod in pods],
            "pod_namespace": event["namespace"],
            "xcom_result": get_succeeded_pod_xcoms(pods, event["xcom_result"]),
        }
        return super().execute_complete(context, event, **kwargs)
