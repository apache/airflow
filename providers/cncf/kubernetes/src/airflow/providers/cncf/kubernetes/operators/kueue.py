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
from typing import TYPE_CHECKING, Any, cast

from kubernetes.client import models as k8s
from kubernetes.client.rest import ApiException
from kubernetes.utils import FailToCreateError
from urllib3.exceptions import HTTPError

from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import generic_api_retry
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.providers.cncf.kubernetes.triggers.kueue import (
    JOB_STATE_MESSAGES,
    KubernetesKueueJobTrigger,
    KueueJobState,
    build_job_event,
    collect_xcom_results,
    get_job_state,
    get_succeeded_pod_xcoms,
    list_job_pods,
)
from airflow.providers.cncf.kubernetes.utils.pod_manager import (
    OnFinishAction,
    OnKillAction,
    XComRetrievalError,
)
from airflow.providers.cncf.kubernetes.version_compat import AIRFLOW_V_3_1_PLUS
from airflow.providers.common.compat.sdk import AirflowException

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk import BaseOperator
else:
    from airflow.models import BaseOperator

# The ``error`` of a deferral timeout in Airflow 3 (``TriggerFailureReason.TRIGGER_TIMEOUT``) and in
# Airflow 2. Matched as text: Airflow 2 raises the generic ``TaskDeferralError`` for any trigger
# failure, and ``TaskDeferralTimeout`` moved to another module in Airflow 3.2.
_DEFERRAL_TIMEOUT_ERRORS = ("Trigger timeout", "Trigger/execution timeout")

if TYPE_CHECKING:
    from pendulum import DateTime

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

    With ``wait_until_job_complete=True``, the operator polls the Job itself rather than its pods: the Job
    has no pods until Kueue admits it, and loses its running pods when Kueue preempts it.

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

        if self.parallelism is not None and self.parallelism < 1:
            raise ValueError("parallelism cannot be less than 1 with `wait_until_job_complete=True`.")
        self.name = self._set_name(self.name)
        self.job_request_obj = self.build_job_request_obj(context)
        self.job = self.create_job(job_request_obj=self.job_request_obj)  # must set `self.job` for `on_kill`
        metadata = cast("k8s.V1ObjectMeta", self.job.metadata)
        job_name, namespace = cast("str", metadata.name), cast("str", metadata.namespace)

        ti = context["ti"]
        ti.xcom_push(key="job_name", value=job_name)
        ti.xcom_push(key="job_namespace", value=namespace)

        if self.deferrable:
            self.defer(
                trigger=KubernetesKueueJobTrigger(
                    job_name=job_name,
                    job_namespace=namespace,
                    base_container_name=self.base_container_name,
                    kubernetes_conn_id=self.kubernetes_conn_id,
                    cluster_context=self.cluster_context,
                    config_file=self.config_file,
                    in_cluster=self.in_cluster,
                    poll_interval=self.job_poll_interval,
                    do_xcom_push=self.do_xcom_push,
                    on_kill_propagation_policy=self.on_kill_propagation_policy,
                    termination_grace_period=self.termination_grace_period,
                    on_kill_action=self.on_kill_action.value,
                ),
                method_name="execute_complete",
                # Airflow 3 does not apply execution_timeout to the time spent deferred
                timeout=self.execution_timeout,
            )

        return self.execute_complete(context, self._wait_until_job_finished(job_name, namespace))

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
                    self.client, self.pod_manager, job, self.base_container_name, xcom_results
                )
            time.sleep(self.job_poll_interval)

    def execute_complete(self, context: Context, event: dict, **kwargs):
        try:
            return self._finish_task(context, event, **kwargs)
        finally:
            self._apply_on_finish_action_to_job(event)

    def _finish_task(self, context: Context, event: dict, **kwargs):
        if not isinstance(event.get("xcom_result"), dict):
            # Only KubernetesKueueJobTrigger gives "xcom_result" as a dict (by pod name): this event is from
            # KubernetesJobTrigger, for a task deferred before the provider upgrade
            return super().execute_complete(context, event, **kwargs)
        pods = sorted(
            list_job_pods(
                self.client,
                event["namespace"],
                event["pod_label_selector"],
                event["job"]["metadata"]["uid"],
            ),
            key=lambda pod: pod.metadata.name,
        )
        for pod in pods:
            self.log_matching_pod(pod=pod, context=context)
        if self.get_logs and event["status"] == "error":
            # super() raises for a failed Job before it writes the logs of the pods
            for pod in pods:
                self._write_logs(pod)
        xcom_result = get_succeeded_pod_xcoms(pods, event["xcom_result"])
        result = super().execute_complete(
            context,
            {
                **event,
                "pod_names": [pod.metadata.name for pod in pods],
                "pod_namespace": event["namespace"],
                "xcom_result": xcom_result,
            },
            **kwargs,
        )
        # Only reached for a successful Job: super() raises for a failed one
        succeeded = (event["job"].get("status") or {}).get("succeeded") or 0
        if self.do_xcom_push and len(xcom_result) < succeeded:
            raise XComRetrievalError(
                f"Read the XComs of only {len(xcom_result)} of the {succeeded} pods that succeeded in "
                f"the job '{event['name']}': the other pods were deleted, or their XComs could not be read"
            )
        return result

    def process_pod_deletion(self, pod: k8s.V1Pod, *, reraise=True) -> bool:
        """Never delete a pod of the Job: ``on_finish_action`` applies to the Job, which owns its pods."""
        return False

    def _apply_on_finish_action_to_job(self, event: dict) -> None:
        """Delete or keep the finished Job depending ``on_finish_action``."""
        delete = self.on_finish_action == OnFinishAction.DELETE_POD or (
            self.on_finish_action == OnFinishAction.DELETE_SUCCEEDED_POD and event["status"] == "success"
        )
        if not delete:
            return
        self.log.info(
            "Deleting the job '%s' (on_finish_action=%s)", event["name"], self.on_finish_action.value
        )
        try:
            self._delete_finished_job(event["name"], event["namespace"])
        except (HTTPError, ApiException) as e:
            self.log.warning("Could not delete the job '%s': %s", event["name"], e)

    @generic_api_retry
    def _delete_finished_job(self, name: str, namespace: str) -> None:
        try:
            # Not on_kill_propagation_policy: "Orphan" would leave the pods of the finished Job behind
            self.job_client.delete_namespaced_job(
                name=name, namespace=namespace, propagation_policy="Background"
            )
        except ApiException as e:
            if e.status != 404:
                raise

    def resume_execution(self, next_method: str, next_kwargs: dict[str, Any] | None, context: Context):
        if next_method == "__fail__" and (next_kwargs or {}).get("error") in _DEFERRAL_TIMEOUT_ERRORS:
            self._delete_job_after_deferral_timeout(context)
        return super().resume_execution(next_method, next_kwargs, context)

    def _delete_job_after_deferral_timeout(self, context: Context) -> None:
        """
        Apply ``on_kill`` to the Job when the deferral timed out, as Airflow does when the sync wait times out.

        ``on_kill`` is not called for a deferred task, and a Job that Kueue never admits would otherwise
        stay in its queue, and be run if Kueue admits it later.
        """
        ti = context["ti"]
        # Without map_indexes, xcom_pull returns the values of every map index of a mapped task
        name = ti.xcom_pull(task_ids=ti.task_id, key="job_name", map_indexes=ti.map_index)
        namespace = ti.xcom_pull(task_ids=ti.task_id, key="job_namespace", map_indexes=ti.map_index)
        if not name:
            return
        self.log.info("The task timed out while waiting for the job '%s'", name)
        # on_kill applies to self.job: on_kill_action, on_kill_propagation_policy, termination_grace_period
        self.job = k8s.V1Job(metadata=k8s.V1ObjectMeta(name=name, namespace=namespace))
        try:
            self.on_kill()
        except ApiException as e:
            self.log.warning("Could not delete the job '%s': %s", name, e)

    def on_kill(self) -> None:
        """
        Delete the Job, unless ``on_kill_action`` is ``keep_pod``.

        A kept Job stays in its queue, and Kueue may still run it.
        """
        job_name = cast("k8s.V1ObjectMeta", self.job.metadata).name if self.job else None
        if self.on_kill_action == OnKillAction.KEEP_POD:
            self._killed = True
            if job_name:
                self.log.info(
                    "Keeping the job '%s' since on_kill_action is set to %r",
                    job_name,
                    self.on_kill_action.value,
                )
            return
        if job_name:
            self.log.info("Deleting the job '%s'", job_name)
        super().on_kill()

    def _write_logs(self, pod: k8s.V1Pod, follow: bool = False, since_time: DateTime | None = None) -> None:
        """Write the logs of the ``container_logs`` of a pod of the finished Job."""
        # Failed pods of earlier attempts may have no readable logs left, e.g. once evicted
        try:
            self.pod_manager.fetch_requested_container_logs(pod=pod, containers=self.container_logs)
        except (HTTPError, ApiException) as e:
            self.log.warning(
                "Could not read the logs of pod %s: %s", pod.metadata.name if pod.metadata else None, e
            )
