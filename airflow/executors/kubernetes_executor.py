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

import functools
import json
import logging
import multiprocessing
import time
from datetime import timedelta
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Tuple

from kubernetes import client, watch
from kubernetes.client import Configuration, models as k8s
from kubernetes.client.rest import ApiException
from urllib3.exceptions import ReadTimeoutError

from airflow.exceptions import AirflowException, PodMutationHookException, PodReconciliationError
from airflow.executors.base_executor import BaseExecutor, CommandType
from airflow.kubernetes import pod_generator
from airflow.kubernetes.kube_client import get_kube_client
from airflow.kubernetes.kube_config import KubeConfig
from airflow.kubernetes.kubernetes_helper_functions import annotations_to_key, create_pod_id
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
from airflow.settings import pod_mutation_hook
from airflow.utils import timezone
from airflow.utils.event_scheduler import EventScheduler
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.state import State

# TaskInstance key, command, configuration, pod_template_file
KubernetesJobType = Tuple[TaskInstanceKey, CommandType, Any, Optional[str]]

# key, state, pod_id, namespace, resource_version
KubernetesResultsType = Tuple[TaskInstanceKey, Optional[str], str, str, str]

# pod_id, namespace, state, annotations, resource_version
KubernetesWatchType = Tuple[str, str, Optional[str], Dict[str, str], str]


class ResourceVersion:
    """Singleton for tracking resourceVersion from Kubernetes."""

    _instance = None
    resource_version = "0"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance


class KubernetesJobWatcher(multiprocessing.Process, LoggingMixin):
    """Watches for Kubernetes jobs."""

    def __init__(
        self,
        namespace: str | None,
        multi_namespace_mode: bool,
        watcher_queue: Queue[KubernetesWatchType],
        resource_version: str | None,
        scheduler_job_id: str,
        kube_config: Configuration,
    ):
        super().__init__()
        self.namespace = namespace
        self.multi_namespace_mode = multi_namespace_mode
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
                ResourceVersion().resource_version = "0"
                raise
            else:
                self.log.warning(
                    "Watch died gracefully, starting back up with: last resource_version: %s",
                    self.resource_version,
                )

    def _run(
        self,
        kube_client: client.CoreV1Api,
        resource_version: str | None,
        scheduler_job_id: str,
        kube_config: Any,
    ) -> str | None:
        self.log.info("Event: and now my watch begins starting at resource_version: %s", resource_version)
        watcher = watch.Watch()

        kwargs = {"label_selector": f"airflow-worker={scheduler_job_id}"}
        if resource_version:
            kwargs["resource_version"] = resource_version
        if kube_config.kube_client_request_args:
            for key, value in kube_config.kube_client_request_args.items():
                kwargs[key] = value

        last_resource_version: str | None = None
        if self.multi_namespace_mode:
            list_worker_pods = functools.partial(
                watcher.stream, kube_client.list_pod_for_all_namespaces, **kwargs
            )
        else:
            list_worker_pods = functools.partial(
                watcher.stream, kube_client.list_namespaced_pod, self.namespace, **kwargs
            )
        for event in list_worker_pods():
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
                pod_id=task.metadata.name,
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
        pod_id: str,
        namespace: str,
        status: str,
        annotations: dict[str, str],
        resource_version: str,
        event: Any,
    ) -> None:
        """Process status response."""
        if status == "Pending":
            if event["type"] == "DELETED":
                self.log.info("Event: Failed to start pod %s", pod_id)
                self.watcher_queue.put((pod_id, namespace, State.FAILED, annotations, resource_version))
            else:
                self.log.debug("Event: %s Pending", pod_id)
        elif status == "Failed":
            self.log.error("Event: %s Failed", pod_id)
            self.watcher_queue.put((pod_id, namespace, State.FAILED, annotations, resource_version))
        elif status == "Succeeded":
            self.log.info("Event: %s Succeeded", pod_id)
            self.watcher_queue.put((pod_id, namespace, None, annotations, resource_version))
        elif status == "Running":
            if event["type"] == "DELETED":
                self.log.info("Event: Pod %s deleted before it could complete", pod_id)
                self.watcher_queue.put((pod_id, namespace, State.FAILED, annotations, resource_version))
            else:
                self.log.info("Event: %s is Running", pod_id)
        else:
            self.log.warning(
                "Event: Invalid state: %s on pod: %s in namespace %s with annotations: %s with "
                "resource_version: %s",
                status,
                pod_id,
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
        self.kube_watcher = self._make_kube_watcher()

    def run_pod_async(self, pod: k8s.V1Pod, **kwargs):
        """Runs POD asynchronously."""
        try:
            pod_mutation_hook(pod)
        except Exception as e:
            raise PodMutationHookException(e)

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

    def _make_kube_watcher(self) -> KubernetesJobWatcher:
        resource_version = ResourceVersion().resource_version
        watcher = KubernetesJobWatcher(
            watcher_queue=self.watcher_queue,
            namespace=self.kube_config.kube_namespace,
            multi_namespace_mode=self.kube_config.multi_namespace_mode,
            resource_version=resource_version,
            scheduler_job_id=self.scheduler_job_id,
            kube_config=self.kube_config,
        )
        watcher.start()
        return watcher

    def _health_check_kube_watcher(self):
        if self.kube_watcher.is_alive():
            self.log.debug("KubeJobWatcher alive, continuing")
        else:
            self.log.error(
                "Error while health checking kube watcher process. Process died for unknown reasons"
            )
            ResourceVersion().resource_version = "0"
            self.kube_watcher = self._make_kube_watcher()

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
        )
        # Reconcile the pod generated by the Operator and the Pod
        # generated by the .cfg file
        self.log.info("Creating kubernetes pod for job is %s, with pod name %s", key, pod.metadata.name)
        self.log.debug("Kubernetes running for command %s", command)
        self.log.debug("Kubernetes launching image %s", pod.spec.containers[0].image)

        # the watcher will monitor pods, so we do not block.
        self.run_pod_async(pod, **self.kube_config.kube_client_request_args)
        self.log.debug("Kubernetes Job created!")

    def delete_pod(self, pod_id: str, namespace: str) -> None:
        """Deletes POD."""
        try:
            self.log.debug("Deleting pod %s in namespace %s", pod_id, namespace)
            self.kube_client.delete_namespaced_pod(
                pod_id,
                namespace,
                body=client.V1DeleteOptions(**self.kube_config.delete_option_kwargs),
                **self.kube_config.kube_client_request_args,
            )
        except ApiException as e:
            # If the pod is already deleted
            if e.status != 404:
                raise

    def sync(self) -> None:
        """
        The sync function checks the status of all currently running kubernetes jobs.
        If a job is completed, its status is placed in the result queue to
        be sent back to the scheduler.

        :return:

        """
        self.log.debug("Syncing KubernetesExecutor")
        self._health_check_kube_watcher()
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
        pod_id, namespace, state, annotations, resource_version = task
        self.log.debug(
            "Attempting to finish pod; pod_id: %s; state: %s; annotations: %s", pod_id, state, annotations
        )
        key = annotations_to_key(annotations=annotations)
        if key:
            self.log.debug("finishing job %s - %s (%s)", key, state, pod_id)
            self.result_queue.put((key, state, pod_id, namespace, resource_version))

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
        self.log.debug("Terminating kube_watcher...")
        self.kube_watcher.terminate()
        self.kube_watcher.join()
        self.log.debug("kube_watcher=%s", self.kube_watcher)
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

    @provide_session
    def clear_not_launched_queued_tasks(self, session=None) -> None:
        """
        Clear tasks that were not yet launched, but were previously queued.

        Tasks can end up in a "Queued" state through either the executor being
        abruptly shut down (leaving a non-empty task_queue on this executor)
        or when a rescheduled/deferred operator comes back up for execution
        (with the same try_number) before the pod of its previous incarnation
        has been fully removed (we think).

        This method checks each of those tasks to see if the corresponding pod
        is around, and if not, and there's no matching entry in our own
        task_queue, marks it for re-execution.
        """
        if TYPE_CHECKING:
            assert self.kube_client

        self.log.debug("Clearing tasks that have not been launched")
        query = session.query(TaskInstance).filter(
            TaskInstance.state == State.QUEUED, TaskInstance.queued_by_job_id == self.job_id
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
            pod_list = self.kube_client.list_namespaced_pod(self.kube_config.kube_namespace, **kwargs)
            if pod_list.items:
                continue
            # Fallback to old style of using execution_date
            kwargs["label_selector"] = (
                f"{base_label_selector},"
                f"execution_date={pod_generator.datetime_to_label_safe_datestring(ti.execution_date)}"
            )
            pod_list = self.kube_client.list_namespaced_pod(self.kube_config.kube_namespace, **kwargs)
            if pod_list.items:
                continue
            self.log.info("TaskInstance: %s found in queued state but was not launched, rescheduling", ti)
            session.query(TaskInstance).filter(
                TaskInstance.dag_id == ti.dag_id,
                TaskInstance.task_id == ti.task_id,
                TaskInstance.run_id == ti.run_id,
                TaskInstance.map_index == ti.map_index,
            ).update({TaskInstance.state: State.SCHEDULED})

    def start(self) -> None:
        """Starts the executor."""
        self.log.info("Start Kubernetes executor")
        if not self.job_id:
            raise AirflowException("Could not get scheduler_job_id")
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
            self.kube_config.worker_pods_pending_timeout_check_interval,
            self._check_worker_pods_pending_timeout,
        )

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
        self.event_buffer[key] = (State.QUEUED, self.scheduler_job_id)
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

        last_resource_version = None
        while True:
            try:
                results = self.result_queue.get_nowait()
                try:
                    key, state, pod_id, namespace, resource_version = results
                    last_resource_version = resource_version
                    self.log.info("Changing state of %s to %s", results, state)
                    try:
                        self._change_state(key, state, pod_id, namespace)
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
        resource_instance.resource_version = last_resource_version or resource_instance.resource_version

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
                        self.change_state(key, State.FAILED, e)
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
                        e,
                    )
                    self.fail(key, e)
                finally:
                    self.task_queue.task_done()
            except Empty:
                break

        # Run any pending timed events
        next_event = self.event_scheduler.run(blocking=False)
        self.log.debug("Next timed event is in %f", next_event)

    def _check_worker_pods_pending_timeout(self):
        """Check if any pending worker pods have timed out."""
        if TYPE_CHECKING:
            assert self.scheduler_job_id

        timeout = self.kube_config.worker_pods_pending_timeout
        self.log.debug("Looking for pending worker pods older than %d seconds", timeout)

        kwargs = {
            "limit": self.kube_config.worker_pods_pending_timeout_batch_size,
            "field_selector": "status.phase=Pending",
            "label_selector": f"airflow-worker={self.scheduler_job_id}",
            **self.kube_config.kube_client_request_args,
        }
        if self.kube_config.multi_namespace_mode:
            pending_pods = functools.partial(self.kube_client.list_pod_for_all_namespaces, **kwargs)
        else:
            pending_pods = functools.partial(
                self.kube_client.list_namespaced_pod, self.kube_config.kube_namespace, **kwargs
            )

        cutoff = timezone.utcnow() - timedelta(seconds=timeout)
        for pod in pending_pods().items:
            self.log.debug(
                'Found a pending pod "%s", created "%s"', pod.metadata.name, pod.metadata.creation_timestamp
            )
            if pod.metadata.creation_timestamp < cutoff:
                self.log.error(
                    (
                        'Pod "%s" has been pending for longer than %d seconds.'
                        "It will be deleted and set to failed."
                    ),
                    pod.metadata.name,
                    timeout,
                )
                self.kube_scheduler.delete_pod(pod.metadata.name, pod.metadata.namespace)

    def _change_state(self, key: TaskInstanceKey, state: str | None, pod_id: str, namespace: str) -> None:
        if TYPE_CHECKING:
            assert self.kube_scheduler

        if state != State.RUNNING:
            if self.kube_config.delete_worker_pods:
                if state != State.FAILED or self.kube_config.delete_worker_pods_on_failure:
                    self.kube_scheduler.delete_pod(pod_id, namespace)
                    self.log.info("Deleted pod: %s in namespace %s", str(key), str(namespace))
            try:
                self.running.remove(key)
            except KeyError:
                self.log.debug("Could not find key: %s", str(key))
        self.event_buffer[key] = state, None

    def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
        tis_to_flush = [ti for ti in tis if not ti.queued_by_job_id]
        scheduler_job_ids = {ti.queued_by_job_id for ti in tis}
        pod_ids = {ti.key: ti for ti in tis if ti.queued_by_job_id}
        kube_client: client.CoreV1Api = self.kube_client
        for scheduler_job_id in scheduler_job_ids:
            scheduler_job_id = pod_generator.make_safe_label_value(str(scheduler_job_id))
            kwargs = {"label_selector": f"airflow-worker={scheduler_job_id}"}
            pod_list = kube_client.list_namespaced_pod(namespace=self.kube_config.kube_namespace, **kwargs)
            for pod in pod_list.items:
                self.adopt_launched_task(kube_client, pod, pod_ids)
        self._adopt_completed_pods(kube_client)
        tis_to_flush.extend(pod_ids.values())
        return tis_to_flush

    def adopt_launched_task(
        self, kube_client: client.CoreV1Api, pod: k8s.V1Pod, pod_ids: dict[TaskInstanceKey, k8s.V1Pod]
    ) -> None:
        """
        Patch existing pod so that the current KubernetesJobWatcher can monitor it via label selectors.

        :param kube_client: kubernetes client for speaking to kube API
        :param pod: V1Pod spec that we will patch with new label
        :param pod_ids: pod_ids we expect to patch.
        """
        if TYPE_CHECKING:
            assert self.scheduler_job_id

        self.log.info("attempting to adopt pod %s", pod.metadata.name)
        pod.metadata.labels["airflow-worker"] = pod_generator.make_safe_label_value(self.scheduler_job_id)
        pod_id = annotations_to_key(pod.metadata.annotations)
        if pod_id not in pod_ids:
            self.log.error("attempting to adopt taskinstance which was not specified by database: %s", pod_id)
            return

        try:
            kube_client.patch_namespaced_pod(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                body=PodGenerator.serialize_pod(pod),
            )
            pod_ids.pop(pod_id)
            self.running.add(pod_id)
        except ApiException as e:
            self.log.info("Failed to adopt pod %s. Reason: %s", pod.metadata.name, e)

    def _adopt_completed_pods(self, kube_client: client.CoreV1Api) -> None:
        """
        Patch completed pod so that the KubernetesJobWatcher can delete it.

        :param kube_client: kubernetes client for speaking to kube API
        """
        if TYPE_CHECKING:
            assert self.scheduler_job_id

        new_worker_id_label = pod_generator.make_safe_label_value(self.scheduler_job_id)
        kwargs = {
            "field_selector": "status.phase=Succeeded",
            "label_selector": f"kubernetes_executor=True,airflow-worker!={new_worker_id_label}",
        }
        pod_list = kube_client.list_namespaced_pod(namespace=self.kube_config.kube_namespace, **kwargs)
        for pod in pod_list.items:
            self.log.info("Attempting to adopt pod %s", pod.metadata.name)
            pod.metadata.labels["airflow-worker"] = new_worker_id_label
            try:
                kube_client.patch_namespaced_pod(
                    name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
                    body=PodGenerator.serialize_pod(pod),
                )
            except ApiException as e:
                self.log.info("Failed to adopt pod %s. Reason: %s", pod.metadata.name, e)

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
                    key, state, pod_id, namespace, resource_version = results
                    self.log.info(
                        "Changing state of %s to %s : resource_version=%d", results, state, resource_version
                    )
                    try:
                        self._change_state(key, state, pod_id, namespace)
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
        self.log.debug("Flushing task_queue...")
        self._flush_task_queue()
        self.log.debug("Flushing result_queue...")
        self._flush_result_queue()
        # Both queues should be empty...
        self.task_queue.join()
        self.result_queue.join()
        if self.kube_scheduler:
            self.kube_scheduler.terminate()
        self._manager.shutdown()

    def terminate(self):
        """Terminate the executor is not doing anything."""
