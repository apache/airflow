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

import contextlib
import json
import multiprocessing
import time
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any

from kubernetes import client, watch
from kubernetes.client.rest import ApiException
from urllib3.exceptions import ReadTimeoutError

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types import (
    ADOPTED,
    ALL_NAMESPACES,
    POD_EXECUTOR_DONE_KEY,
)
from airflow.providers.cncf.kubernetes.kube_client import get_kube_client
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import (
    annotations_for_logging_task_metadata,
    annotations_to_key,
    create_unique_id,
)
from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.singleton import Singleton
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from kubernetes.client import Configuration, models as k8s

    from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types import (
        KubernetesJobType,
        KubernetesResultsType,
        KubernetesWatchType,
    )


class ResourceVersion(metaclass=Singleton):
    """Singleton for tracking resourceVersion from Kubernetes."""

    resource_version: dict[str, str] = {}


class KubernetesJobWatcher(multiprocessing.Process, LoggingMixin):
    """Watches for Kubernetes jobs."""

    def __init__(
        self,
        namespace: str,
        watcher_queue: Queue[KubernetesWatchType],
        resource_version: str | None,
        scheduler_job_id: str,
        kube_config: Configuration,
    ):
        super().__init__()
        self.namespace = namespace
        self.scheduler_job_id = scheduler_job_id
        self.watcher_queue = watcher_queue
        self.resource_version = resource_version
        self.kube_config = kube_config

    def run(self) -> None:
        """Perform watching."""
        if TYPE_CHECKING:
            assert self.scheduler_job_id

        kube_client: client.CoreV1Api = get_kube_client()
        while True:
            try:
                self.resource_version = self._run(
                    kube_client, self.resource_version, self.scheduler_job_id, self.kube_config
                )
            except ReadTimeoutError:
                self.log.info("Kubernetes watch timed out waiting for events. Restarting watch.")
                time.sleep(1)
            except Exception:
                self.log.exception("Unknown error in KubernetesJobWatcher. Failing")
                self.resource_version = "0"
                ResourceVersion().resource_version[self.namespace] = "0"
                raise
            else:
                self.log.warning(
                    "Watch died gracefully, starting back up with: last resource_version: %s",
                    self.resource_version,
                )

    def _pod_events(self, kube_client: client.CoreV1Api, query_kwargs: dict):
        watcher = watch.Watch()
        try:
            if self.namespace == ALL_NAMESPACES:
                return watcher.stream(kube_client.list_pod_for_all_namespaces, **query_kwargs)
            else:
                return watcher.stream(kube_client.list_namespaced_pod, self.namespace, **query_kwargs)
        except ApiException as e:
            if str(e.status) == "410":  # Resource version is too old
                if self.namespace == ALL_NAMESPACES:
                    pods = kube_client.list_pod_for_all_namespaces(watch=False)
                else:
                    pods = kube_client.list_namespaced_pod(namespace=self.namespace, watch=False)
                resource_version = pods.metadata.resource_version
                query_kwargs["resource_version"] = resource_version
                return self._pod_events(kube_client=kube_client, query_kwargs=query_kwargs)
            else:
                raise

    def _run(
        self,
        kube_client: client.CoreV1Api,
        resource_version: str | None,
        scheduler_job_id: str,
        kube_config: Any,
    ) -> str | None:
        self.log.info("Event: and now my watch begins starting at resource_version: %s", resource_version)

        kwargs: dict[str, Any] = {
            "label_selector": f"airflow-worker={scheduler_job_id},{POD_EXECUTOR_DONE_KEY}!=True",
        }
        if resource_version:
            kwargs["resource_version"] = resource_version
        if kube_config.kube_client_request_args:
            for key, value in kube_config.kube_client_request_args.items():
                kwargs[key] = value

        last_resource_version: str | None = None

        # For info about k8s timeout settings see
        # https://github.com/kubernetes-client/python/blob/v29.0.0/examples/watch/timeout-settings.md
        # and https://github.com/kubernetes-client/python/blob/v29.0.0/kubernetes/client/api_client.py#L336-L339
        client_timeout = 30
        server_conn_timeout = 3600
        kwargs["_request_timeout"] = client_timeout
        kwargs["timeout_seconds"] = server_conn_timeout

        for event in self._pod_events(kube_client=kube_client, query_kwargs=kwargs):
            task = event["object"]
            self.log.debug("Event: %s had an event of type %s", task.metadata.name, event["type"])
            if event["type"] == "ERROR":
                return self.process_error(event)
            annotations = task.metadata.annotations
            task_instance_related_annotations = {
                "dag_id": annotations["dag_id"],
                "task_id": annotations["task_id"],
                "execution_date": annotations.get("execution_date"),
                "run_id": annotations.get("run_id"),
                "try_number": annotations["try_number"],
            }
            map_index = annotations.get("map_index")
            if map_index is not None:
                task_instance_related_annotations["map_index"] = map_index

            self.process_status(
                pod_name=task.metadata.name,
                namespace=task.metadata.namespace,
                status=task.status.phase,
                annotations=task_instance_related_annotations,
                resource_version=task.metadata.resource_version,
                event=event,
            )
            last_resource_version = task.metadata.resource_version

        return last_resource_version

    def process_error(self, event: Any) -> str:
        """Process error response."""
        self.log.error("Encountered Error response from k8s list namespaced pod stream => %s", event)
        raw_object = event["raw_object"]
        if raw_object["code"] == 410:
            self.log.info(
                "Kubernetes resource version is too old, must reset to 0 => %s", (raw_object["message"],)
            )
            # Return resource version 0
            return "0"
        raise AirflowException(
            f"Kubernetes failure for {raw_object['reason']} with code {raw_object['code']} and message: "
            f"{raw_object['message']}"
        )

    def process_status(
        self,
        pod_name: str,
        namespace: str,
        status: str,
        annotations: dict[str, str],
        resource_version: str,
        event: Any,
    ) -> None:
        pod = event["object"]
        annotations_string = annotations_for_logging_task_metadata(annotations)
        """Process status response."""
        if event["type"] == "DELETED" and not pod.metadata.deletion_timestamp:
            # This will happen only when the task pods are adopted by another executor.
            # So, there is no change in the pod state.
            # However, need to free the executor slot from the current executor.
            self.log.info("Event: pod %s adopted, annotations: %s", pod_name, annotations_string)
            self.watcher_queue.put((pod_name, namespace, ADOPTED, annotations, resource_version))
        elif hasattr(pod.status, "reason") and pod.status.reason == "ProviderFailed":
            # Most likely this happens due to Kubernetes setup (virtual kubelet, virtual nodes, etc.)
            self.log.error(
                "Event: %s failed to start with reason ProviderFailed, annotations: %s",
                pod_name,
                annotations_string,
            )
            self.watcher_queue.put(
                (pod_name, namespace, TaskInstanceState.FAILED, annotations, resource_version)
            )
        elif status == "Pending":
            # deletion_timestamp is set by kube server when a graceful deletion is requested.
            # since kube server have received request to delete pod set TI state failed
            if event["type"] == "DELETED" and pod.metadata.deletion_timestamp:
                self.log.info("Event: Failed to start pod %s, annotations: %s", pod_name, annotations_string)
                self.watcher_queue.put(
                    (pod_name, namespace, TaskInstanceState.FAILED, annotations, resource_version)
                )
            elif (
                self.kube_config.worker_pod_pending_fatal_container_state_reasons
                and "status" in event["raw_object"]
            ):
                # Init containers and base container statuses to check.
                # Skipping the other containers statuses check.
                container_statuses_to_check = []
                if "initContainerStatuses" in event["raw_object"]["status"]:
                    container_statuses_to_check.extend(event["raw_object"]["status"]["initContainerStatuses"])
                if "containerStatuses" in event["raw_object"]["status"]:
                    container_statuses_to_check.append(event["raw_object"]["status"]["containerStatuses"][0])
                for container_status in container_statuses_to_check:
                    container_status_state = container_status["state"]
                    if "waiting" in container_status_state:
                        if (
                            container_status_state["waiting"]["reason"]
                            in self.kube_config.worker_pod_pending_fatal_container_state_reasons
                        ):
                            if (
                                container_status_state["waiting"]["reason"] == "ErrImagePull"
                                and container_status_state["waiting"]["message"] == "pull QPS exceeded"
                            ):
                                continue
                            self.log.error(
                                "Event: %s has container %s with fatal reason %s",
                                pod_name,
                                container_status["name"],
                                container_status_state["waiting"]["reason"],
                            )
                            self.watcher_queue.put(
                                (pod_name, namespace, TaskInstanceState.FAILED, annotations, resource_version)
                            )
                            break
                else:
                    self.log.info("Event: %s Pending, annotations: %s", pod_name, annotations_string)
            else:
                self.log.debug("Event: %s Pending, annotations: %s", pod_name, annotations_string)
        elif status == "Failed":
            self.log.error("Event: %s Failed, annotations: %s", pod_name, annotations_string)
            self.watcher_queue.put(
                (pod_name, namespace, TaskInstanceState.FAILED, annotations, resource_version)
            )
        elif status == "Succeeded":
            self.log.info("Event: %s Succeeded, annotations: %s", pod_name, annotations_string)
            self.watcher_queue.put((pod_name, namespace, None, annotations, resource_version))
        elif status == "Running":
            # deletion_timestamp is set by kube server when a graceful deletion is requested.
            # since kube server have received request to delete pod set TI state failed
            if event["type"] == "DELETED" and pod.metadata.deletion_timestamp:
                self.log.info(
                    "Event: Pod %s deleted before it could complete, annotations: %s",
                    pod_name,
                    annotations_string,
                )
                self.watcher_queue.put(
                    (pod_name, namespace, TaskInstanceState.FAILED, annotations, resource_version)
                )
            else:
                self.log.info("Event: %s is Running, annotations: %s", pod_name, annotations_string)
        else:
            self.log.warning(
                "Event: Invalid state: %s on pod: %s in namespace %s with annotations: %s with "
                "resource_version: %s",
                status,
                pod_name,
                namespace,
                annotations,
                resource_version,
            )


class AirflowKubernetesScheduler(LoggingMixin):
    """Airflow Scheduler for Kubernetes."""

    def __init__(
        self,
        kube_config: Any,
        result_queue: Queue[KubernetesResultsType],
        kube_client: client.CoreV1Api,
        scheduler_job_id: str,
    ):
        super().__init__()
        self.log.debug("Creating Kubernetes executor")
        self.kube_config = kube_config
        self.result_queue = result_queue
        self.namespace = self.kube_config.kube_namespace
        self.log.debug("Kubernetes using namespace %s", self.namespace)
        self.kube_client = kube_client
        self._manager = multiprocessing.Manager()
        self.watcher_queue = self._manager.Queue()
        self.scheduler_job_id = scheduler_job_id
        self.kube_watchers = self._make_kube_watchers()

    def run_pod_async(self, pod: k8s.V1Pod, **kwargs):
        """Run POD asynchronously."""
        sanitized_pod = self.kube_client.api_client.sanitize_for_serialization(pod)
        json_pod = json.dumps(sanitized_pod, indent=2)

        self.log.debug("Pod Creation Request: \n%s", json_pod)
        try:
            resp = self.kube_client.create_namespaced_pod(
                body=sanitized_pod, namespace=pod.metadata.namespace, **kwargs
            )
            self.log.debug("Pod Creation Response: %s", resp)
        except Exception as e:
            self.log.exception("Exception when attempting to create Namespaced Pod: %s", json_pod)
            raise e
        return resp

    def _make_kube_watcher(self, namespace) -> KubernetesJobWatcher:
        resource_version = ResourceVersion().resource_version.get(namespace, "0")
        watcher = KubernetesJobWatcher(
            watcher_queue=self.watcher_queue,
            namespace=namespace,
            resource_version=resource_version,
            scheduler_job_id=self.scheduler_job_id,
            kube_config=self.kube_config,
        )
        watcher.start()
        return watcher

    def _make_kube_watchers(self) -> dict[str, KubernetesJobWatcher]:
        watchers = {}
        if self.kube_config.multi_namespace_mode:
            namespaces_to_watch = (
                self.kube_config.multi_namespace_mode_namespace_list
                if self.kube_config.multi_namespace_mode_namespace_list
                else [ALL_NAMESPACES]
            )
        else:
            namespaces_to_watch = [self.kube_config.kube_namespace]

        for namespace in namespaces_to_watch:
            watchers[namespace] = self._make_kube_watcher(namespace)
        return watchers

    def _health_check_kube_watchers(self):
        for namespace, kube_watcher in self.kube_watchers.items():
            if kube_watcher.is_alive():
                self.log.debug("KubeJobWatcher for namespace %s alive, continuing", namespace)
            else:
                self.log.error(
                    (
                        "Error while health checking kube watcher process for namespace %s. "
                        "Process died for unknown reasons"
                    ),
                    namespace,
                )
                ResourceVersion().resource_version[namespace] = "0"
                self.kube_watchers[namespace] = self._make_kube_watcher(namespace)

    def run_next(self, next_job: KubernetesJobType) -> None:
        """Receives the next job to run, builds the pod, and creates it."""
        key, command, kube_executor_config, pod_template_file = next_job

        dag_id, task_id, run_id, try_number, map_index = key

        if command[0:3] != ["airflow", "tasks", "run"]:
            raise ValueError('The command must start with ["airflow", "tasks", "run"].')

        base_worker_pod = get_base_pod_from_template(pod_template_file, self.kube_config)

        if not base_worker_pod:
            raise AirflowException(
                f"could not find a valid worker template yaml at {self.kube_config.pod_template_file}"
            )

        pod = PodGenerator.construct_pod(
            namespace=self.namespace,
            scheduler_job_id=self.scheduler_job_id,
            pod_id=create_unique_id(dag_id, task_id),
            dag_id=dag_id,
            task_id=task_id,
            kube_image=self.kube_config.kube_image,
            try_number=try_number,
            map_index=map_index,
            date=None,
            run_id=run_id,
            args=command,
            pod_override_object=kube_executor_config,
            base_worker_pod=base_worker_pod,
            with_mutation_hook=True,
        )
        # Reconcile the pod generated by the Operator and the Pod
        # generated by the .cfg file
        self.log.info(
            "Creating kubernetes pod for job is %s, with pod name %s, annotations: %s",
            key,
            pod.metadata.name,
            annotations_for_logging_task_metadata(pod.metadata.annotations),
        )
        self.log.debug("Kubernetes running for command %s", command)
        self.log.debug("Kubernetes launching image %s", pod.spec.containers[0].image)

        # the watcher will monitor pods, so we do not block.
        self.run_pod_async(pod, **self.kube_config.kube_client_request_args)
        self.log.debug("Kubernetes Job created!")

    def delete_pod(self, pod_name: str, namespace: str) -> None:
        """Delete Pod from a namespace; does not raise if it does not exist."""
        try:
            self.log.debug("Deleting pod %s in namespace %s", pod_name, namespace)
            self.kube_client.delete_namespaced_pod(
                pod_name,
                namespace,
                body=client.V1DeleteOptions(**self.kube_config.delete_option_kwargs),
                **self.kube_config.kube_client_request_args,
            )
        except ApiException as e:
            # If the pod is already deleted
            if str(e.status) != "404":
                raise

    def patch_pod_executor_done(self, *, pod_name: str, namespace: str):
        """Add a "done" annotation to ensure we don't continually adopt pods."""
        self.log.debug("Patching pod %s in namespace %s to mark it as done", pod_name, namespace)
        try:
            self.kube_client.patch_namespaced_pod(
                name=pod_name,
                namespace=namespace,
                body={"metadata": {"labels": {POD_EXECUTOR_DONE_KEY: "True"}}},
            )
        except ApiException as e:
            self.log.info("Failed to patch pod %s with done annotation. Reason: %s", pod_name, e)

    def sync(self) -> None:
        """
        Check the status of all currently running kubernetes jobs.

        If a job is completed, its status is placed in the result queue to be sent back to the scheduler.
        """
        self.log.debug("Syncing KubernetesExecutor")
        self._health_check_kube_watchers()
        with contextlib.suppress(Empty):
            while True:
                task = self.watcher_queue.get_nowait()
                try:
                    self.log.debug("Processing task %s", task)
                    self.process_watcher_task(task)
                finally:
                    self.watcher_queue.task_done()

    def process_watcher_task(self, task: KubernetesWatchType) -> None:
        """Process the task by watcher."""
        pod_name, namespace, state, annotations, resource_version = task
        self.log.debug(
            "Attempting to finish pod; pod_name: %s; state: %s; annotations: %s",
            pod_name,
            state,
            annotations_for_logging_task_metadata(annotations),
        )
        key = annotations_to_key(annotations=annotations)
        if key:
            self.log.debug("finishing job %s - %s (%s)", key, state, pod_name)
            self.result_queue.put((key, state, pod_name, namespace, resource_version))

    def _flush_watcher_queue(self) -> None:
        self.log.debug("Executor shutting down, watcher_queue approx. size=%d", self.watcher_queue.qsize())
        with contextlib.suppress(Empty):
            while True:
                task = self.watcher_queue.get_nowait()
                # Ignoring it since it can only have either FAILED or SUCCEEDED pods
                self.log.warning("Executor shutting down, IGNORING watcher task=%s", task)
                self.watcher_queue.task_done()

    def terminate(self) -> None:
        """Terminates the watcher."""
        self.log.debug("Terminating kube_watchers...")
        for kube_watcher in self.kube_watchers.values():
            kube_watcher.terminate()
            kube_watcher.join()
            self.log.debug("kube_watcher=%s", kube_watcher)
        self.log.debug("Flushing watcher_queue...")
        self._flush_watcher_queue()
        # Queue should be empty...
        self.watcher_queue.join()
        self.log.debug("Shutting down manager...")
        self._manager.shutdown()


def get_base_pod_from_template(pod_template_file: str | None, kube_config: Any) -> k8s.V1Pod:
    """
    Get base pod from template.

    Reads either the pod_template_file set in the executor_config or the base pod_template_file
    set in the airflow.cfg to craft a "base pod" that will be used by the KubernetesExecutor

    :param pod_template_file: absolute path to a pod_template_file.yaml or None
    :param kube_config: The KubeConfig class generated by airflow that contains all kube metadata
    :return: a V1Pod that can be used as the base pod for k8s tasks
    """
    if pod_template_file:
        return PodGenerator.deserialize_model_file(pod_template_file)
    else:
        return PodGenerator.deserialize_model_file(kube_config.pod_template_file)
