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
"""
KubernetesExecutor.

.. seealso::
    For more information on how the KubernetesExecutor works, take a look at the guide:
    :ref:`executor:KubernetesExecutor`
"""
from __future__ import annotations

import json
import logging
import multiprocessing
import time
from collections import defaultdict
from contextlib import suppress
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Tuple

from kubernetes import client, watch
from kubernetes.client import Configuration, models as k8s
from kubernetes.client.rest import ApiException
from sqlalchemy.orm import Session
from urllib3.exceptions import ReadTimeoutError

from airflow.configuration import conf
from airflow.exceptions import AirflowException, PodMutationHookException, PodReconciliationError
from airflow.executors.base_executor import BaseExecutor
from airflow.kubernetes import pod_generator
from airflow.kubernetes.kube_client import get_kube_client
from airflow.kubernetes.kube_config import KubeConfig
from airflow.kubernetes.kubernetes_helper_functions import annotations_to_key, create_pod_id
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.event_scheduler import EventScheduler
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import State, TaskInstanceState

if TYPE_CHECKING:
    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstance import TaskInstanceKey

    # TaskInstance key, command, configuration, pod_template_file
    KubernetesJobType = Tuple[TaskInstanceKey, CommandType, Any, Optional[str]]

    # key, pod state, pod_name, namespace, resource_version
    KubernetesResultsType = Tuple[TaskInstanceKey, Optional[str], str, str, str]

    # pod_name, namespace, pod state, annotations, resource_version
    KubernetesWatchType = Tuple[str, str, Optional[str], Dict[str, str], str]

ALL_NAMESPACES = "ALL_NAMESPACES"
POD_EXECUTOR_DONE_KEY = "airflow_executor_done"


class ResourceVersion:
    """Singleton for tracking resourceVersion from Kubernetes."""

    _instance: ResourceVersion | None = None
    resource_version: dict[str, str] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance


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
        """Performs watching."""
        if TYPE_CHECKING:
            assert self.scheduler_job_id

        kube_client: client.CoreV1Api = get_kube_client()
        while True:
            try:
                self.resource_version = self._run(
                    kube_client, self.resource_version, self.scheduler_job_id, self.kube_config
                )
            except ReadTimeoutError:
                self.log.warning(
                    "There was a timeout error accessing the Kube API. Retrying request.", exc_info=True
                )
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
            if e.status == 410:  # Resource version is too old
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

        kwargs = {"label_selector": f"airflow-worker={scheduler_job_id}"}
        if resource_version:
            kwargs["resource_version"] = resource_version
        if kube_config.kube_client_request_args:
            for key, value in kube_config.kube_client_request_args.items():
                kwargs[key] = value

        last_resource_version: str | None = None

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
        """Process status response."""
        if status == "Pending":
            if event["type"] == "DELETED":
                self.log.info("Event: Failed to start pod %s", pod_name)
                self.watcher_queue.put((pod_name, namespace, State.FAILED, annotations, resource_version))
            else:
                self.log.debug("Event: %s Pending", pod_name)
        elif status == "Failed":
            self.log.error("Event: %s Failed", pod_name)
            self.watcher_queue.put((pod_name, namespace, State.FAILED, annotations, resource_version))
        elif status == "Succeeded":
            # We get multiple events once the pod hits a terminal state, and we only want to
            # send it along to the scheduler once.
            # If our event type is DELETED, we have the POD_EXECUTOR_DONE_KEY, or the pod has
            # a deletion timestamp, we've already seen the initial Succeeded event and sent it
            # along to the scheduler.
            pod = event["object"]
            if (
                event["type"] == "DELETED"
                or POD_EXECUTOR_DONE_KEY in pod.metadata.labels
                or pod.metadata.deletion_timestamp
            ):
                self.log.info(
                    "Skipping event for Succeeded pod %s - event for this pod already sent to executor",
                    pod_name,
                )
                return
            self.log.info("Event: %s Succeeded", pod_name)
            self.watcher_queue.put((pod_name, namespace, None, annotations, resource_version))
        elif status == "Running":
            if event["type"] == "DELETED":
                self.log.info("Event: Pod %s deleted before it could complete", pod_name)
                self.watcher_queue.put((pod_name, namespace, State.FAILED, annotations, resource_version))
            else:
                self.log.info("Event: %s is Running", pod_name)
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
        """Runs POD asynchronously."""
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
            pod_id=create_pod_id(dag_id, task_id),
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
        self.log.info("Creating kubernetes pod for job is %s, with pod name %s", key, pod.metadata.name)
        self.log.debug("Kubernetes running for command %s", command)
        self.log.debug("Kubernetes launching image %s", pod.spec.containers[0].image)

        # the watcher will monitor pods, so we do not block.
        self.run_pod_async(pod, **self.kube_config.kube_client_request_args)
        self.log.debug("Kubernetes Job created!")

    def delete_pod(self, pod_name: str, namespace: str) -> None:
        """Deletes Pod from a namespace. Does not raise if it does not exist."""
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
            if e.status != 404:
                raise

    def patch_pod_executor_done(self, *, pod_name: str, namespace: str):
        """Add a "done" annotation to ensure we don't continually adopt pods"""
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
        The sync function checks the status of all currently running kubernetes jobs.
        If a job is completed, its status is placed in the result queue to
        be sent back to the scheduler.

        :return:

        """
        self.log.debug("Syncing KubernetesExecutor")
        self._health_check_kube_watchers()
        while True:
            try:
                task = self.watcher_queue.get_nowait()
                try:
                    self.log.debug("Processing task %s", task)
                    self.process_watcher_task(task)
                finally:
                    self.watcher_queue.task_done()
            except Empty:
                break

    def process_watcher_task(self, task: KubernetesWatchType) -> None:
        """Process the task by watcher."""
        pod_name, namespace, state, annotations, resource_version = task
        self.log.debug(
            "Attempting to finish pod; pod_name: %s; state: %s; annotations: %s", pod_name, state, annotations
        )
        key = annotations_to_key(annotations=annotations)
        if key:
            self.log.debug("finishing job %s - %s (%s)", key, state, pod_name)
            self.result_queue.put((key, state, pod_name, namespace, resource_version))

    def _flush_watcher_queue(self) -> None:
        self.log.debug("Executor shutting down, watcher_queue approx. size=%d", self.watcher_queue.qsize())
        while True:
            try:
                task = self.watcher_queue.get_nowait()
                # Ignoring it since it can only have either FAILED or SUCCEEDED pods
                self.log.warning("Executor shutting down, IGNORING watcher task=%s", task)
                self.watcher_queue.task_done()
            except Empty:
                break

    def terminate(self) -> None:
        """Terminates the watcher."""
        self.log.debug("Terminating kube_watchers...")
        for namespace, kube_watcher in self.kube_watchers.items():
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


class KubernetesExecutor(BaseExecutor):
    """Executor for Kubernetes."""

    supports_ad_hoc_ti_run: bool = True

    def __init__(self):
        self.kube_config = KubeConfig()
        self._manager = multiprocessing.Manager()
        self.task_queue: Queue[KubernetesJobType] = self._manager.Queue()
        self.result_queue: Queue[KubernetesResultsType] = self._manager.Queue()
        self.kube_scheduler: AirflowKubernetesScheduler | None = None
        self.kube_client: client.CoreV1Api | None = None
        self.scheduler_job_id: str | None = None
        self.event_scheduler: EventScheduler | None = None
        self.last_handled: dict[TaskInstanceKey, float] = {}
        self.kubernetes_queue: str | None = None
        super().__init__(parallelism=self.kube_config.parallelism)

    def _list_pods(self, query_kwargs):
        if self.kube_config.multi_namespace_mode:
            if self.kube_config.multi_namespace_mode_namespace_list:
                pods = []
                for namespace in self.kube_config.multi_namespace_mode_namespace_list:
                    pods.extend(
                        self.kube_client.list_namespaced_pod(namespace=namespace, **query_kwargs).items
                    )
            else:
                pods = self.kube_client.list_pod_for_all_namespaces(**query_kwargs).items
        else:
            pods = self.kube_client.list_namespaced_pod(
                namespace=self.kube_config.kube_namespace, **query_kwargs
            ).items

        return pods

    @provide_session
    def clear_not_launched_queued_tasks(self, session: Session = NEW_SESSION) -> None:
        """
        Clear tasks that were not yet launched, but were previously queued.

        Tasks can end up in a "Queued" state when a rescheduled/deferred operator
        comes back up for execution (with the same try_number) before the
        pod of its previous incarnation has been fully removed (we think).

        It's also possible when an executor abruptly shuts down (leaving a non-empty
        task_queue on that executor), but that scenario is handled via normal adoption.

        This method checks each of our queued tasks to see if the corresponding pod
        is around, and if not, and there's no matching entry in our own
        task_queue, marks it for re-execution.
        """
        if TYPE_CHECKING:
            assert self.kube_client
        from airflow.models.taskinstance import TaskInstance

        self.log.debug("Clearing tasks that have not been launched")
        query = session.query(TaskInstance).filter(
            TaskInstance.state == TaskInstanceState.QUEUED, TaskInstance.queued_by_job_id == self.job_id
        )
        if self.kubernetes_queue:
            query = query.filter(TaskInstance.queue == self.kubernetes_queue)
        queued_tis: list[TaskInstance] = query.all()
        self.log.info("Found %s queued task instances", len(queued_tis))

        # Go through the "last seen" dictionary and clean out old entries
        allowed_age = self.kube_config.worker_pods_queued_check_interval * 3
        for key, timestamp in list(self.last_handled.items()):
            if time.time() - timestamp > allowed_age:
                del self.last_handled[key]

        for ti in queued_tis:
            self.log.debug("Checking task instance %s", ti)

            # Check to see if we've handled it ourselves recently
            if ti.key in self.last_handled:
                continue

            # Build the pod selector
            base_label_selector = (
                f"dag_id={pod_generator.make_safe_label_value(ti.dag_id)},"
                f"task_id={pod_generator.make_safe_label_value(ti.task_id)},"
                f"airflow-worker={pod_generator.make_safe_label_value(str(ti.queued_by_job_id))}"
            )
            if ti.map_index >= 0:
                # Old tasks _couldn't_ be mapped, so we don't have to worry about compat
                base_label_selector += f",map_index={ti.map_index}"
            kwargs = dict(label_selector=base_label_selector)
            if self.kube_config.kube_client_request_args:
                kwargs.update(**self.kube_config.kube_client_request_args)

            # Try run_id first
            kwargs["label_selector"] += ",run_id=" + pod_generator.make_safe_label_value(ti.run_id)
            pod_list = self._list_pods(kwargs)
            if pod_list:
                continue
            # Fallback to old style of using execution_date
            kwargs["label_selector"] = (
                f"{base_label_selector},"
                f"execution_date={pod_generator.datetime_to_label_safe_datestring(ti.execution_date)}"
            )
            pod_list = self._list_pods(kwargs)
            if pod_list:
                continue
            self.log.info("TaskInstance: %s found in queued state but was not launched, rescheduling", ti)
            session.query(TaskInstance).filter(
                TaskInstance.dag_id == ti.dag_id,
                TaskInstance.task_id == ti.task_id,
                TaskInstance.run_id == ti.run_id,
                TaskInstance.map_index == ti.map_index,
            ).update({TaskInstance.state: TaskInstanceState.SCHEDULED})

    def start(self) -> None:
        """Starts the executor."""
        self.log.info("Start Kubernetes executor")
        self.scheduler_job_id = str(self.job_id)
        self.log.debug("Start with scheduler_job_id: %s", self.scheduler_job_id)
        self.kube_client = get_kube_client()
        self.kube_scheduler = AirflowKubernetesScheduler(
            kube_config=self.kube_config,
            result_queue=self.result_queue,
            kube_client=self.kube_client,
            scheduler_job_id=self.scheduler_job_id,
        )
        self.event_scheduler = EventScheduler()

        self.event_scheduler.call_regular_interval(
            self.kube_config.worker_pods_queued_check_interval,
            self.clear_not_launched_queued_tasks,
        )
        # We also call this at startup as that's the most likely time to see
        # stuck queued tasks
        self.clear_not_launched_queued_tasks()

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: str | None = None,
        executor_config: Any | None = None,
    ) -> None:
        """Executes task asynchronously."""
        if TYPE_CHECKING:
            assert self.task_queue

        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug("Add task %s with command %s, executor_config %s", key, command, executor_config)
        else:
            self.log.info("Add task %s with command %s", key, command)

        try:
            kube_executor_config = PodGenerator.from_obj(executor_config)
        except Exception:
            self.log.error("Invalid executor_config for %s. Executor_config: %s", key, executor_config)
            self.fail(key=key, info="Invalid executor_config passed")
            return

        if executor_config:
            pod_template_file = executor_config.get("pod_template_file", None)
        else:
            pod_template_file = None
        self.event_buffer[key] = (TaskInstanceState.QUEUED, self.scheduler_job_id)
        self.task_queue.put((key, command, kube_executor_config, pod_template_file))
        # We keep a temporary local record that we've handled this so we don't
        # try and remove it from the QUEUED state while we process it
        self.last_handled[key] = time.time()

    def sync(self) -> None:
        """Synchronize task state."""
        if TYPE_CHECKING:
            assert self.scheduler_job_id
            assert self.kube_scheduler
            assert self.kube_config
            assert self.result_queue
            assert self.task_queue
            assert self.event_scheduler

        if self.running:
            self.log.debug("self.running: %s", self.running)
        if self.queued_tasks:
            self.log.debug("self.queued: %s", self.queued_tasks)
        self.kube_scheduler.sync()

        last_resource_version: dict[str, str] = defaultdict(lambda: "0")
        while True:
            try:
                results = self.result_queue.get_nowait()
                try:
                    key, state, pod_name, namespace, resource_version = results
                    last_resource_version[namespace] = resource_version
                    self.log.info("Changing state of %s to %s", results, state)
                    try:
                        self._change_state(key, state, pod_name, namespace)
                    except Exception as e:
                        self.log.exception(
                            "Exception: %s when attempting to change state of %s to %s, re-queueing.",
                            e,
                            results,
                            state,
                        )
                        self.result_queue.put(results)
                finally:
                    self.result_queue.task_done()
            except Empty:
                break

        resource_instance = ResourceVersion()
        for ns in resource_instance.resource_version.keys():
            resource_instance.resource_version[ns] = (
                last_resource_version[ns] or resource_instance.resource_version[ns]
            )

        for _ in range(self.kube_config.worker_pods_creation_batch_size):
            try:
                task = self.task_queue.get_nowait()
                try:
                    self.kube_scheduler.run_next(task)
                except PodReconciliationError as e:
                    self.log.error(
                        "Pod reconciliation failed, likely due to kubernetes library upgrade. "
                        "Try clearing the task to re-run.",
                        exc_info=True,
                    )
                    self.fail(task[0], e)
                except ApiException as e:
                    # These codes indicate something is wrong with pod definition; otherwise we assume pod
                    # definition is ok, and that retrying may work
                    if e.status in (400, 422):
                        self.log.error("Pod creation failed with reason %r. Failing task", e.reason)
                        key, _, _, _ = task
                        self.change_state(key, TaskInstanceState.FAILED, e)
                    else:
                        self.log.warning(
                            "ApiException when attempting to run task, re-queueing. Reason: %r. Message: %s",
                            e.reason,
                            json.loads(e.body)["message"],
                        )
                        self.task_queue.put(task)
                except PodMutationHookException as e:
                    key, _, _, _ = task
                    self.log.error(
                        "Pod Mutation Hook failed for the task %s. Failing task. Details: %s",
                        key,
                        e.__cause__,
                    )
                    self.fail(key, e)
                finally:
                    self.task_queue.task_done()
            except Empty:
                break

        # Run any pending timed events
        next_event = self.event_scheduler.run(blocking=False)
        self.log.debug("Next timed event is in %f", next_event)

    @provide_session
    def _change_state(
        self,
        key: TaskInstanceKey,
        state: str | None,
        pod_name: str,
        namespace: str,
        session: Session = NEW_SESSION,
    ) -> None:
        if TYPE_CHECKING:
            assert self.kube_scheduler

        if state == State.RUNNING:
            self.event_buffer[key] = state, None
            return

        if self.kube_config.delete_worker_pods:
            if state != State.FAILED or self.kube_config.delete_worker_pods_on_failure:
                self.kube_scheduler.delete_pod(pod_name=pod_name, namespace=namespace)
                self.log.info("Deleted pod: %s in namespace %s", str(key), str(namespace))
        else:
            self.kube_scheduler.patch_pod_executor_done(pod_name=pod_name, namespace=namespace)
            self.log.info("Patched pod %s in namespace %s to mark it as done", str(key), str(namespace))

        try:
            self.running.remove(key)
        except KeyError:
            self.log.debug("TI key not in running, not adding to event_buffer: %s", key)

        # If we don't have a TI state, look it up from the db. event_buffer expects the TI state
        if state is None:
            state = session.query(TaskInstance.state).filter(TaskInstance.filter_for_tis([key])).scalar()

        self.event_buffer[key] = state, None

    @staticmethod
    def _get_pod_namespace(ti: TaskInstance):
        pod_override = ti.executor_config.get("pod_override")
        namespace = None
        with suppress(Exception):
            namespace = pod_override.metadata.namespace
        return namespace or conf.get("kubernetes_executor", "namespace", fallback="default")

    def get_task_log(self, ti: TaskInstance, try_number: int) -> tuple[list[str], list[str]]:
        messages = []
        log = []
        try:
            from airflow.kubernetes.kube_client import get_kube_client
            from airflow.kubernetes.pod_generator import PodGenerator

            client = get_kube_client()

            messages.append(f"Trying to get logs (last 100 lines) from worker pod {ti.hostname}")
            selector = PodGenerator.build_selector_for_k8s_executor_pod(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                try_number=try_number,
                map_index=ti.map_index,
                run_id=ti.run_id,
                airflow_worker=ti.queued_by_job_id,
            )
            namespace = self._get_pod_namespace(ti)
            pod_list = client.list_namespaced_pod(
                namespace=namespace,
                label_selector=selector,
            ).items
            if not pod_list:
                raise RuntimeError("Cannot find pod for ti %s", ti)
            elif len(pod_list) > 1:
                raise RuntimeError("Found multiple pods for ti %s: %s", ti, pod_list)
            res = client.read_namespaced_pod_log(
                name=pod_list[0].metadata.name,
                namespace=namespace,
                container="base",
                follow=False,
                tail_lines=100,
                _preload_content=False,
            )

            for line in res:
                log.append(line.decode())
        except Exception as e:
            messages.append(f"Reading from k8s pod logs failed: {str(e)}")
        return messages, ["\n".join(log)]

    def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
        # Always flush TIs without queued_by_job_id
        tis_to_flush = [ti for ti in tis if not ti.queued_by_job_id]
        scheduler_job_ids = {ti.queued_by_job_id for ti in tis}
        tis_to_flush_by_key = {ti.key: ti for ti in tis if ti.queued_by_job_id}
        kube_client: client.CoreV1Api = self.kube_client
        for scheduler_job_id in scheduler_job_ids:
            scheduler_job_id = pod_generator.make_safe_label_value(str(scheduler_job_id))
            # We will look for any pods owned by the no-longer-running scheduler,
            # but will exclude only successful pods, as those TIs will have a terminal state
            # and not be up for adoption!
            # Those workers that failed, however, are okay to adopt here as their TI will
            # still be in queued.
            query_kwargs = {
                "field_selector": "status.phase!=Succeeded",
                "label_selector": (
                    "kubernetes_executor=True,"
                    f"airflow-worker={scheduler_job_id},{POD_EXECUTOR_DONE_KEY}!=True"
                ),
            }
            pod_list = self._list_pods(query_kwargs)
            for pod in pod_list:
                self.adopt_launched_task(kube_client, pod, tis_to_flush_by_key)
        self._adopt_completed_pods(kube_client)
        tis_to_flush.extend(tis_to_flush_by_key.values())
        return tis_to_flush

    def cleanup_stuck_queued_tasks(self, tis: list[TaskInstance]) -> list[str]:
        """
        Handle remnants of tasks that were failed because they were stuck in queued.
        Tasks can get stuck in queued. If such a task is detected, it will be marked
        as `UP_FOR_RETRY` if the task instance has remaining retries or marked as `FAILED`
        if it doesn't.

        :param tis: List of Task Instances to clean up
        :return: List of readable task instances for a warning message
        """
        if TYPE_CHECKING:
            assert self.kube_client
            assert self.kube_scheduler
        readable_tis = []
        for ti in tis:
            selector = PodGenerator.build_selector_for_k8s_executor_pod(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                try_number=ti.try_number,
                map_index=ti.map_index,
                run_id=ti.run_id,
                airflow_worker=ti.queued_by_job_id,
            )
            namespace = self._get_pod_namespace(ti)
            pod_list = self.kube_client.list_namespaced_pod(
                namespace=namespace,
                label_selector=selector,
            ).items
            if not pod_list:
                self.log.warning("Cannot find pod for ti %s", ti)
                continue
            elif len(pod_list) > 1:
                self.log.warning("Found multiple pods for ti %s: %s", ti, pod_list)
                continue
            readable_tis.append(repr(ti))
            self.kube_scheduler.delete_pod(pod_name=pod_list[0].metadata.name, namespace=namespace)
        return readable_tis

    def adopt_launched_task(
        self,
        kube_client: client.CoreV1Api,
        pod: k8s.V1Pod,
        tis_to_flush_by_key: dict[TaskInstanceKey, k8s.V1Pod],
    ) -> None:
        """
        Patch existing pod so that the current KubernetesJobWatcher can monitor it via label selectors.

        :param kube_client: kubernetes client for speaking to kube API
        :param pod: V1Pod spec that we will patch with new label
        :param tis_to_flush_by_key: TIs that will be flushed if they aren't adopted
        """
        if TYPE_CHECKING:
            assert self.scheduler_job_id

        self.log.info("attempting to adopt pod %s", pod.metadata.name)
        ti_key = annotations_to_key(pod.metadata.annotations)
        if ti_key not in tis_to_flush_by_key:
            self.log.error("attempting to adopt taskinstance which was not specified by database: %s", ti_key)
            return

        new_worker_id_label = pod_generator.make_safe_label_value(self.scheduler_job_id)
        try:
            kube_client.patch_namespaced_pod(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                body={"metadata": {"labels": {"airflow-worker": new_worker_id_label}}},
            )
        except ApiException as e:
            self.log.info("Failed to adopt pod %s. Reason: %s", pod.metadata.name, e)
            return

        del tis_to_flush_by_key[ti_key]
        self.running.add(ti_key)

    def _adopt_completed_pods(self, kube_client: client.CoreV1Api) -> None:
        """
        Patch completed pods so that the KubernetesJobWatcher can delete them.

        :param kube_client: kubernetes client for speaking to kube API
        """
        if TYPE_CHECKING:
            assert self.scheduler_job_id

        new_worker_id_label = pod_generator.make_safe_label_value(self.scheduler_job_id)
        query_kwargs = {
            "field_selector": "status.phase=Succeeded",
            "label_selector": (
                "kubernetes_executor=True,"
                f"airflow-worker!={new_worker_id_label},{POD_EXECUTOR_DONE_KEY}!=True"
            ),
        }
        pod_list = self._list_pods(query_kwargs)
        for pod in pod_list:
            self.log.info("Attempting to adopt pod %s", pod.metadata.name)
            try:
                kube_client.patch_namespaced_pod(
                    name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
                    body={"metadata": {"labels": {"airflow-worker": new_worker_id_label}}},
                )
            except ApiException as e:
                self.log.info("Failed to adopt pod %s. Reason: %s", pod.metadata.name, e)
            ti_id = annotations_to_key(pod.metadata.annotations)
            self.running.add(ti_id)

    def _flush_task_queue(self) -> None:
        if TYPE_CHECKING:
            assert self.task_queue

        self.log.debug("Executor shutting down, task_queue approximate size=%d", self.task_queue.qsize())
        while True:
            try:
                task = self.task_queue.get_nowait()
                # This is a new task to run thus ok to ignore.
                self.log.warning("Executor shutting down, will NOT run task=%s", task)
                self.task_queue.task_done()
            except Empty:
                break

    def _flush_result_queue(self) -> None:
        if TYPE_CHECKING:
            assert self.result_queue

        self.log.debug("Executor shutting down, result_queue approximate size=%d", self.result_queue.qsize())
        while True:
            try:
                results = self.result_queue.get_nowait()
                self.log.warning("Executor shutting down, flushing results=%s", results)
                try:
                    key, state, pod_name, namespace, resource_version = results
                    self.log.info(
                        "Changing state of %s to %s : resource_version=%d", results, state, resource_version
                    )
                    try:
                        self._change_state(key, state, pod_name, namespace)
                    except Exception as e:
                        self.log.exception(
                            "Ignoring exception: %s when attempting to change state of %s to %s.",
                            e,
                            results,
                            state,
                        )
                finally:
                    self.result_queue.task_done()
            except Empty:
                break

    def end(self) -> None:
        """Called when the executor shuts down"""
        if TYPE_CHECKING:
            assert self.task_queue
            assert self.result_queue
            assert self.kube_scheduler

        self.log.info("Shutting down Kubernetes executor")
        try:
            self.log.debug("Flushing task_queue...")
            self._flush_task_queue()
            self.log.debug("Flushing result_queue...")
            self._flush_result_queue()
            # Both queues should be empty...
            self.task_queue.join()
            self.result_queue.join()
        except ConnectionResetError:
            self.log.exception("Connection Reset error while flushing task_queue and result_queue.")
        if self.kube_scheduler:
            self.kube_scheduler.terminate()
        self._manager.shutdown()

    def terminate(self):
        """Terminate the executor is not doing anything."""
