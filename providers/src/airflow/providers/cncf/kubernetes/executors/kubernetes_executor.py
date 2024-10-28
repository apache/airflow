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
    :doc:`/kubernetes_executor`
"""

from __future__ import annotations

import contextlib
import json
import logging
import multiprocessing
import time
from collections import Counter, defaultdict
from contextlib import suppress
from datetime import datetime
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any, Sequence

from kubernetes.dynamic import DynamicClient
from sqlalchemy import or_, select, update

from airflow.cli.cli_config import (
    ARG_DAG_ID,
    ARG_EXECUTION_DATE,
    ARG_OUTPUT_PATH,
    ARG_SUBDIR,
    ARG_VERBOSE,
    ActionCommand,
    Arg,
    GroupCommand,
    lazy_load_command,
    positive_int,
)
from airflow.configuration import conf
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.executor_constants import KUBERNETES_EXECUTOR
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types import (
    ADOPTED,
    POD_EXECUTOR_DONE_KEY,
)
from airflow.providers.cncf.kubernetes.kube_config import KubeConfig
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import (
    annotations_to_key,
)
from airflow.providers.cncf.kubernetes.pod_generator import (
    PodMutationHookException,
    PodReconciliationError,
)
from airflow.stats import Stats
from airflow.utils.event_scheduler import EventScheduler
from airflow.utils.log.logging_mixin import remove_escape_codes
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    import argparse

    from kubernetes import client
    from kubernetes.client import models as k8s
    from sqlalchemy.orm import Session

    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types import (
        KubernetesJobType,
        KubernetesResultsType,
    )
    from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils import (
        AirflowKubernetesScheduler,
    )

# CLI Args
ARG_NAMESPACE = Arg(
    ("--namespace",),
    default=conf.get("kubernetes_executor", "namespace"),
    help="Kubernetes Namespace. Default value is `[kubernetes] namespace` in configuration.",
)

ARG_MIN_PENDING_MINUTES = Arg(
    ("--min-pending-minutes",),
    default=30,
    type=positive_int(allow_zero=False),
    help=(
        "Pending pods created before the time interval are to be cleaned up, "
        "measured in minutes. Default value is 30(m). The minimum value is 5(m)."
    ),
)

# CLI Commands
KUBERNETES_COMMANDS = (
    ActionCommand(
        name="cleanup-pods",
        help=(
            "Clean up Kubernetes pods "
            "(created by KubernetesExecutor/KubernetesPodOperator) "
            "in evicted/failed/succeeded/pending states"
        ),
        func=lazy_load_command(
            "airflow.providers.cncf.kubernetes.cli.kubernetes_command.cleanup_pods"
        ),
        args=(ARG_NAMESPACE, ARG_MIN_PENDING_MINUTES, ARG_VERBOSE),
    ),
    ActionCommand(
        name="generate-dag-yaml",
        help="Generate YAML files for all tasks in DAG. Useful for debugging tasks without "
        "launching into a cluster",
        func=lazy_load_command(
            "airflow.providers.cncf.kubernetes.cli.kubernetes_command.generate_pod_yaml"
        ),
        args=(ARG_DAG_ID, ARG_EXECUTION_DATE, ARG_SUBDIR, ARG_OUTPUT_PATH, ARG_VERBOSE),
    ),
)


class KubernetesExecutor(BaseExecutor):
    """Executor for Kubernetes."""

    RUNNING_POD_LOG_LINES = 100
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
        self.task_publish_retries: Counter[TaskInstanceKey] = Counter()
        self.task_publish_max_retries = conf.getint(
            "kubernetes_executor", "task_publish_max_retries", fallback=0
        )
        super().__init__(parallelism=self.kube_config.parallelism)

    def _list_pods(self, query_kwargs):
        query_kwargs["header_params"] = {
            "Accept": "application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io"
        }
        dynamic_client = DynamicClient(self.kube_client.api_client)
        pod_resource = dynamic_client.resources.get(api_version="v1", kind="Pod")
        if self.kube_config.multi_namespace_mode:
            if self.kube_config.multi_namespace_mode_namespace_list:
                namespaces = self.kube_config.multi_namespace_mode_namespace_list
            else:
                namespaces = [None]
        else:
            namespaces = [self.kube_config.kube_namespace]

        pods = []
        for namespace in namespaces:
            pods.extend(
                dynamic_client.get(
                    resource=pod_resource, namespace=namespace, **query_kwargs
                ).items
            )

        return pods

    def _make_safe_label_value(self, input_value: str | datetime) -> str:
        """
        Normalize a provided label to be of valid length and characters.

        See airflow.providers.cncf.kubernetes.pod_generator.make_safe_label_value for more details.
        """
        # airflow.providers.cncf.kubernetes is an expensive import, locally import it here to
        # speed up load times of the kubernetes_executor module.
        from airflow.providers.cncf.kubernetes import pod_generator

        if isinstance(input_value, datetime):
            return pod_generator.datetime_to_label_safe_datestring(input_value)
        return pod_generator.make_safe_label_value(input_value)

    def get_pod_combined_search_str_to_pod_map(self) -> dict[str, k8s.V1Pod]:
        """
        List the worker pods owned by this scheduler and create a map containing pod combined search str -> pod.

        For every pod, it creates two below entries in the map
        dag_id={dag_id},task_id={task_id},airflow-worker={airflow_worker},<map_index={map_index}>,run_id={run_id}
        """
        # airflow worker label selector batch call
        kwargs = {
            "label_selector": f"airflow-worker={self._make_safe_label_value(str(self.job_id))}"
        }
        if self.kube_config.kube_client_request_args:
            kwargs.update(self.kube_config.kube_client_request_args)
        pod_list = self._list_pods(kwargs)

        # create a set against pod query label fields
        pod_combined_search_str_to_pod_map = {}
        for pod in pod_list:
            dag_id = pod.metadata.annotations.get("dag_id", None)
            task_id = pod.metadata.annotations.get("task_id", None)
            map_index = pod.metadata.annotations.get("map_index", None)
            run_id = pod.metadata.annotations.get("run_id", None)
            if dag_id is None or task_id is None:
                continue
            search_base_str = f"dag_id={dag_id},task_id={task_id}"
            if map_index is not None:
                search_base_str += f",map_index={map_index}"
            if run_id is not None:
                search_str = f"{search_base_str},run_id={run_id}"
                pod_combined_search_str_to_pod_map[search_str] = pod
        return pod_combined_search_str_to_pod_map

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

        hybrid_executor_enabled = hasattr(TaskInstance, "executor")
        default_executor = None
        if hybrid_executor_enabled:
            from airflow.executors.executor_loader import ExecutorLoader

            default_executor = str(ExecutorLoader.get_default_executor_name())

        with Stats.timer("kubernetes_executor.clear_not_launched_queued_tasks.duration"):
            self.log.debug("Clearing tasks that have not been launched")
            query = select(TaskInstance).where(
                TaskInstance.state == TaskInstanceState.QUEUED,
                TaskInstance.queued_by_job_id == self.job_id,
            )
            if self.kubernetes_queue:
                query = query.where(TaskInstance.queue == self.kubernetes_queue)
            elif hybrid_executor_enabled and KUBERNETES_EXECUTOR == default_executor:
                query = query.where(
                    or_(
                        TaskInstance.executor == KUBERNETES_EXECUTOR,
                        TaskInstance.executor.is_(None),
                    ),
                )
            elif hybrid_executor_enabled:
                query = query.where(TaskInstance.executor == KUBERNETES_EXECUTOR)
            queued_tis: list[TaskInstance] = session.scalars(query).all()
            self.log.info("Found %s queued task instances", len(queued_tis))

            # Go through the "last seen" dictionary and clean out old entries
            allowed_age = self.kube_config.worker_pods_queued_check_interval * 3
            for key, timestamp in list(self.last_handled.items()):
                if time.time() - timestamp > allowed_age:
                    del self.last_handled[key]

            if not queued_tis:
                return

            pod_combined_search_str_to_pod_map = (
                self.get_pod_combined_search_str_to_pod_map()
            )

            for ti in queued_tis:
                self.log.debug("Checking task instance %s", ti)

                # Check to see if we've handled it ourselves recently
                if ti.key in self.last_handled:
                    continue

                # Build the pod selector
                base_selector = f"dag_id={ti.dag_id},task_id={ti.task_id}"
                if ti.map_index >= 0:
                    # Old tasks _couldn't_ be mapped, so we don't have to worry about compat
                    base_selector += f",map_index={ti.map_index}"

                search_str = f"{base_selector},run_id={ti.run_id}"
                if search_str in pod_combined_search_str_to_pod_map:
                    continue
                self.log.info(
                    "TaskInstance: %s found in queued state but was not launched, rescheduling",
                    ti,
                )
                session.execute(
                    update(TaskInstance)
                    .where(
                        TaskInstance.dag_id == ti.dag_id,
                        TaskInstance.task_id == ti.task_id,
                        TaskInstance.run_id == ti.run_id,
                        TaskInstance.map_index == ti.map_index,
                    )
                    .values(state=TaskInstanceState.SCHEDULED)
                )

    def start(self) -> None:
        """Start the executor."""
        self.log.info("Start Kubernetes executor")
        self.scheduler_job_id = str(self.job_id)
        self.log.debug("Start with scheduler_job_id: %s", self.scheduler_job_id)
        from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils import (
            AirflowKubernetesScheduler,
        )
        from airflow.providers.cncf.kubernetes.kube_client import get_kube_client

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
        """Execute task asynchronously."""
        if TYPE_CHECKING:
            assert self.task_queue

        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug(
                "Add task %s with command %s, executor_config %s",
                key,
                command,
                executor_config,
            )
        else:
            self.log.info("Add task %s with command %s", key, command)

        from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator

        try:
            kube_executor_config = PodGenerator.from_obj(executor_config)
        except Exception:
            self.log.error(
                "Invalid executor_config for %s. Executor_config: %s",
                key,
                executor_config,
            )
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
        with contextlib.suppress(Empty):
            while True:
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

        from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils import (
            ResourceVersion,
        )

        resource_instance = ResourceVersion()
        for ns in resource_instance.resource_version:
            resource_instance.resource_version[ns] = (
                last_resource_version[ns] or resource_instance.resource_version[ns]
            )

        from kubernetes.client.rest import ApiException

        with contextlib.suppress(Empty):
            for _ in range(self.kube_config.worker_pods_creation_batch_size):
                task = self.task_queue.get_nowait()

                try:
                    key, command, kube_executor_config, pod_template_file = task
                    self.kube_scheduler.run_next(task)
                    self.task_publish_retries.pop(key, None)
                except PodReconciliationError as e:
                    self.log.exception(
                        "Pod reconciliation failed, likely due to kubernetes library upgrade. "
                        "Try clearing the task to re-run.",
                    )
                    self.fail(task[0], e)
                except ApiException as e:
                    body = json.loads(e.body)
                    retries = self.task_publish_retries[key]
                    # In case of exceeded quota errors, requeue the task as per the task_publish_max_retries
                    if (
                        str(e.status) == "403"
                        and "exceeded quota" in body["message"]
                        and (
                            self.task_publish_max_retries == -1
                            or retries < self.task_publish_max_retries
                        )
                    ):
                        self.log.warning(
                            "[Try %s of %s] Kube ApiException for Task: (%s). Reason: %r. Message: %s",
                            self.task_publish_retries[key] + 1,
                            self.task_publish_max_retries,
                            key,
                            e.reason,
                            body["message"],
                        )
                        self.task_queue.put(task)
                        self.task_publish_retries[key] = retries + 1
                    else:
                        self.log.error(
                            "Pod creation failed with reason %r. Failing task", e.reason
                        )
                        key, _, _, _ = task
                        self.fail(key, e)
                        self.task_publish_retries.pop(key, None)
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

        # Run any pending timed events
        next_event = self.event_scheduler.run(blocking=False)
        self.log.debug("Next timed event is in %f", next_event)

    @provide_session
    def _change_state(
        self,
        key: TaskInstanceKey,
        state: TaskInstanceState | str | None,
        pod_name: str,
        namespace: str,
        session: Session = NEW_SESSION,
    ) -> None:
        if TYPE_CHECKING:
            assert self.kube_scheduler

        if state == ADOPTED:
            # When the task pod is adopted by another executor,
            # then remove the task from the current executor running queue.
            try:
                self.running.remove(key)
            except KeyError:
                self.log.debug("TI key not in running: %s", key)
            return

        if state == TaskInstanceState.RUNNING:
            self.event_buffer[key] = state, None
            return

        if self.kube_config.delete_worker_pods:
            if (
                state != TaskInstanceState.FAILED
                or self.kube_config.delete_worker_pods_on_failure
            ):
                self.kube_scheduler.delete_pod(pod_name=pod_name, namespace=namespace)
                self.log.info(
                    "Deleted pod associated with the TI %s. Pod name: %s. Namespace: %s",
                    key,
                    pod_name,
                    namespace,
                )
        else:
            self.kube_scheduler.patch_pod_executor_done(
                pod_name=pod_name, namespace=namespace
            )
            self.log.info(
                "Patched pod %s in namespace %s to mark it as done", key, namespace
            )

        try:
            self.running.remove(key)
        except KeyError:
            self.log.debug("TI key not in running, not adding to event_buffer: %s", key)
            return

        # If we don't have a TI state, look it up from the db. event_buffer expects the TI state
        if state is None:
            from airflow.models.taskinstance import TaskInstance

            state = session.scalar(
                select(TaskInstance.state).where(TaskInstance.filter_for_tis([key]))
            )
            state = TaskInstanceState(state) if state else None

        self.event_buffer[key] = state, None

    @staticmethod
    def _get_pod_namespace(ti: TaskInstance):
        pod_override = ti.executor_config.get("pod_override")
        namespace = None
        with suppress(Exception):
            namespace = pod_override.metadata.namespace
        return namespace or conf.get("kubernetes_executor", "namespace")

    def get_task_log(
        self, ti: TaskInstance, try_number: int
    ) -> tuple[list[str], list[str]]:
        messages = []
        log = []
        try:
            from airflow.providers.cncf.kubernetes.kube_client import get_kube_client
            from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator

            client = get_kube_client()

            messages.append(
                f"Attempting to fetch logs from pod {ti.hostname} through kube API"
            )
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
                tail_lines=self.RUNNING_POD_LOG_LINES,
                _preload_content=False,
            )
            for line in res:
                log.append(remove_escape_codes(line.decode()))
            if log:
                messages.append("Found logs through kube API")
        except Exception as e:
            messages.append(f"Reading from k8s pod logs failed: {e}")
        return messages, ["\n".join(log)]

    def try_adopt_task_instances(
        self, tis: Sequence[TaskInstance]
    ) -> Sequence[TaskInstance]:
        with Stats.timer("kubernetes_executor.adopt_task_instances.duration"):
            # Always flush TIs without queued_by_job_id
            tis_to_flush = [ti for ti in tis if not ti.queued_by_job_id]
            scheduler_job_ids = {ti.queued_by_job_id for ti in tis}
            tis_to_flush_by_key = {ti.key: ti for ti in tis if ti.queued_by_job_id}
            kube_client: client.CoreV1Api = self.kube_client
            for scheduler_job_id in scheduler_job_ids:
                scheduler_job_id = self._make_safe_label_value(str(scheduler_job_id))
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

            # as this method can be retried within a short time frame
            # (wrapped in a run_with_db_retries of scheduler_job_runner,
            # and get retried due to an OperationalError, for example),
            # there is a chance that in second attempt, adopt_launched_task will not be called even once
            # as all pods are already adopted in the first attempt.
            # and tis_to_flush_by_key will contain TIs that are already adopted.
            # therefore, we need to check if the TIs are already adopted by the first attempt and remove them.
            def _iter_tis_to_flush():
                for key, ti in tis_to_flush_by_key.items():
                    if key in self.running:
                        self.log.info("%s is already adopted, no need to flush.", ti)
                    else:
                        yield ti

            tis_to_flush.extend(_iter_tis_to_flush())
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
        readable_tis: list[str] = []
        if not tis:
            return readable_tis
        pod_combined_search_str_to_pod_map = self.get_pod_combined_search_str_to_pod_map()
        for ti in tis:
            # Build the pod selector
            base_label_selector = f"dag_id={ti.dag_id},task_id={ti.task_id}"
            if ti.map_index >= 0:
                # Old tasks _couldn't_ be mapped, so we don't have to worry about compat
                base_label_selector += f",map_index={ti.map_index}"

            search_str = f"{base_label_selector},run_id={ti.run_id}"
            pod = pod_combined_search_str_to_pod_map.get(search_str, None)
            if not pod:
                self.log.warning("Cannot find pod for ti %s", ti)
                continue
            readable_tis.append(repr(ti))
            self.kube_scheduler.delete_pod(
                pod_name=pod.metadata.name, namespace=pod.metadata.namespace
            )
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
            self.log.error(
                "attempting to adopt taskinstance which was not specified by database: %s",
                ti_key,
            )
            return

        new_worker_id_label = self._make_safe_label_value(self.scheduler_job_id)
        from kubernetes.client.rest import ApiException

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

        new_worker_id_label = self._make_safe_label_value(self.scheduler_job_id)
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
            from kubernetes.client.rest import ApiException

            try:
                kube_client.patch_namespaced_pod(
                    name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
                    body={
                        "metadata": {"labels": {"airflow-worker": new_worker_id_label}}
                    },
                )
            except ApiException as e:
                self.log.info("Failed to adopt pod %s. Reason: %s", pod.metadata.name, e)
                continue

            ti_id = annotations_to_key(pod.metadata.annotations)
            self.running.add(ti_id)

    def _flush_task_queue(self) -> None:
        if TYPE_CHECKING:
            assert self.task_queue

        self.log.debug(
            "Executor shutting down, task_queue approximate size=%d",
            self.task_queue.qsize(),
        )
        with contextlib.suppress(Empty):
            while True:
                task = self.task_queue.get_nowait()
                # This is a new task to run thus ok to ignore.
                self.log.warning("Executor shutting down, will NOT run task=%s", task)
                self.task_queue.task_done()

    def _flush_result_queue(self) -> None:
        if TYPE_CHECKING:
            assert self.result_queue

        self.log.debug(
            "Executor shutting down, result_queue approximate size=%d",
            self.result_queue.qsize(),
        )
        with contextlib.suppress(Empty):
            while True:
                results = self.result_queue.get_nowait()
                self.log.warning("Executor shutting down, flushing results=%s", results)
                try:
                    key, state, pod_name, namespace, resource_version = results
                    self.log.info(
                        "Changing state of %s to %s : resource_version=%d",
                        results,
                        state,
                        resource_version,
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

    def end(self) -> None:
        """Shut down the executor."""
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
            self.log.exception(
                "Connection Reset error while flushing task_queue and result_queue."
            )
        if self.kube_scheduler:
            self.kube_scheduler.terminate()
        self._manager.shutdown()

    def terminate(self):
        """Terminate the executor is not doing anything."""

    @staticmethod
    def get_cli_commands() -> list[GroupCommand]:
        return [
            GroupCommand(
                name="kubernetes",
                help="Tools to help run the KubernetesExecutor",
                subcommands=KUBERNETES_COMMANDS,
            )
        ]


def _get_parser() -> argparse.ArgumentParser:
    """
    Generate documentation; used by Sphinx.

    :meta private:
    """
    return KubernetesExecutor._get_parser()
