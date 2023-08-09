#
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

import itertools
import logging
import multiprocessing
import os
import signal
import sys
import time
import warnings
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache, partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Collection, Iterable, Iterator

from sqlalchemy import and_, delete, func, not_, or_, select, text, update
from sqlalchemy.engine import Result
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Query, Session, joinedload, load_only, make_transient, selectinload
from sqlalchemy.sql import expression

from airflow import settings
from airflow.callbacks.callback_requests import DagCallbackRequest, SlaCallbackRequest, TaskCallbackRequest
from airflow.callbacks.pipe_callback_sink import PipeCallbackSink
from airflow.configuration import conf
from airflow.exceptions import RemovedInAirflow3Warning
from airflow.executors.executor_loader import ExecutorLoader
from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.jobs.job import Job, perform_heartbeat
from airflow.models.dag import DAG, DagModel
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.dataset import (
    DagScheduleDatasetReference,
    DatasetDagRunQueue,
    DatasetEvent,
    DatasetModel,
    TaskOutletDatasetReference,
)
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstance, TaskInstanceKey
from airflow.stats import Stats
from airflow.ti_deps.dependencies_states import EXECUTION_STATES
from airflow.timetables.simple import DatasetTriggeredTimetable
from airflow.utils import timezone
from airflow.utils.event_scheduler import EventScheduler
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.retries import MAX_DB_RETRIES, retry_db_transaction, run_with_db_retries
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.sqlalchemy import (
    CommitProhibitorGuard,
    is_lock_not_available_error,
    prohibit_commit,
    skip_locked,
    tuple_in_condition,
    with_row_locks,
)
from airflow.utils.state import DagRunState, JobState, State, TaskInstanceState
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from types import FrameType

    from airflow.dag_processing.manager import DagFileProcessorAgent

TI = TaskInstance
DR = DagRun
DM = DagModel


@dataclass
class ConcurrencyMap:
    """
    Dataclass to represent concurrency maps.

    It contains a map from (dag_id, task_id) to # of task instances, a map from (dag_id, task_id)
    to # of task instances in the given state list and a map from (dag_id, run_id, task_id)
    to # of task instances in the given state list in each DAG run.
    """

    dag_active_tasks_map: dict[str, int]
    task_concurrency_map: dict[tuple[str, str], int]
    task_dagrun_concurrency_map: dict[tuple[str, str, str], int]

    @classmethod
    def from_concurrency_map(cls, mapping: dict[tuple[str, str, str], int]) -> ConcurrencyMap:
        instance = cls(Counter(), Counter(), Counter(mapping))
        for (d, r, t), c in mapping.items():
            instance.dag_active_tasks_map[d] += c
            instance.task_concurrency_map[(d, t)] += c
        return instance


def _is_parent_process() -> bool:
    """
    Whether this is a parent process.

    Return True if the current process is the parent process.
    False if the current process is a child process started by multiprocessing.
    """
    return multiprocessing.current_process().name == "MainProcess"


class SchedulerJobRunner(BaseJobRunner[Job], LoggingMixin):
    """
    SchedulerJobRunner runs for a specific time interval and schedules jobs that are ready to run.

    It figures out the latest runs for each task and sees if the dependencies
    for the next schedules are met.
    If so, it creates appropriate TaskInstances and sends run commands to the
    executor. It does this for each task in each DAG and repeats.

    :param subdir: directory containing Python files with Airflow DAG
        definitions, or a specific path to a file
    :param num_runs: The number of times to run the scheduling loop. If you
        have a large number of DAG files this could complete before each file
        has been parsed. -1 for unlimited times.
    :param num_times_parse_dags: The number of times to try to parse each DAG file.
        -1 for unlimited times.
    :param scheduler_idle_sleep_time: The number of seconds to wait between
        polls of running processors
    :param do_pickle: once a DAG object is obtained by executing the Python
        file, whether to serialize the DAG object to the DB
    :param log: override the default Logger
    """

    job_type = "SchedulerJob"
    heartrate: int = conf.getint("scheduler", "SCHEDULER_HEARTBEAT_SEC")

    def __init__(
        self,
        job: Job,
        subdir: str = settings.DAGS_FOLDER,
        num_runs: int = conf.getint("scheduler", "num_runs"),
        num_times_parse_dags: int = -1,
        scheduler_idle_sleep_time: float = conf.getfloat("scheduler", "scheduler_idle_sleep_time"),
        do_pickle: bool = False,
        log: logging.Logger | None = None,
        processor_poll_interval: float | None = None,
    ):
        super().__init__(job)
        self.subdir = subdir
        self.num_runs = num_runs
        # In specific tests, we want to stop the parse loop after the _files_ have been parsed a certain
        # number of times. This is only to support testing, and isn't something a user is likely to want to
        # configure -- they'll want num_runs
        self.num_times_parse_dags = num_times_parse_dags
        if processor_poll_interval:
            # TODO: Remove in Airflow 3.0
            warnings.warn(
                "The 'processor_poll_interval' parameter is deprecated. "
                "Please use 'scheduler_idle_sleep_time'.",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )
            scheduler_idle_sleep_time = processor_poll_interval
        self._scheduler_idle_sleep_time = scheduler_idle_sleep_time
        # How many seconds do we wait for tasks to heartbeat before mark them as zombies.
        self._zombie_threshold_secs = conf.getint("scheduler", "scheduler_zombie_task_threshold")
        self._standalone_dag_processor = conf.getboolean("scheduler", "standalone_dag_processor")
        self._dag_stale_not_seen_duration = conf.getint("scheduler", "dag_stale_not_seen_duration")

        # Since the functionality for stalled_task_timeout, task_adoption_timeout, and
        # worker_pods_pending_timeout are now handled by a single config (task_queued_timeout),
        # we can't deprecate them as we normally would. So, we'll read each config and take
        # the max value in order to ensure we're not undercutting a legitimate
        # use of any of these configs.
        stalled_task_timeout = conf.getfloat("celery", "stalled_task_timeout", fallback=0)
        if stalled_task_timeout:
            # TODO: Remove in Airflow 3.0
            warnings.warn(
                "The '[celery] stalled_task_timeout' config option is deprecated. "
                "Please update your config to use '[scheduler] task_queued_timeout' instead.",
                DeprecationWarning,
            )
        task_adoption_timeout = conf.getfloat("celery", "task_adoption_timeout", fallback=0)
        if task_adoption_timeout:
            # TODO: Remove in Airflow 3.0
            warnings.warn(
                "The '[celery] task_adoption_timeout' config option is deprecated. "
                "Please update your config to use '[scheduler] task_queued_timeout' instead.",
                DeprecationWarning,
            )
        worker_pods_pending_timeout = conf.getfloat(
            "kubernetes_executor", "worker_pods_pending_timeout", fallback=0
        )
        if worker_pods_pending_timeout:
            # TODO: Remove in Airflow 3.0
            warnings.warn(
                "The '[kubernetes_executor] worker_pods_pending_timeout' config option is deprecated. "
                "Please update your config to use '[scheduler] task_queued_timeout' instead.",
                DeprecationWarning,
            )
        task_queued_timeout = conf.getfloat("scheduler", "task_queued_timeout")
        self._task_queued_timeout = max(
            stalled_task_timeout, task_adoption_timeout, worker_pods_pending_timeout, task_queued_timeout
        )

        self.do_pickle = do_pickle

        if log:
            self._log = log

        # Check what SQL backend we use
        sql_conn: str = conf.get_mandatory_value("database", "sql_alchemy_conn").lower()
        self.using_sqlite = sql_conn.startswith("sqlite")
        # Dag Processor agent - not used in Dag Processor standalone mode.
        self.processor_agent: DagFileProcessorAgent | None = None

        self.dagbag = DagBag(dag_folder=self.subdir, read_dags_from_db=True, load_op_links=False)
        self._paused_dag_without_running_dagruns: set = set()

    @provide_session
    def heartbeat_callback(self, session: Session = NEW_SESSION) -> None:
        Stats.incr("scheduler_heartbeat", 1, 1)

    def register_signals(self) -> None:
        """Register signals that stop child processes."""
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)
        signal.signal(signal.SIGUSR2, self._debug_dump)

    def _exit_gracefully(self, signum: int, frame: FrameType | None) -> None:
        """Helper method to clean up processor_agent to avoid leaving orphan processes."""
        if not _is_parent_process():
            # Only the parent process should perform the cleanup.
            return

        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        if self.processor_agent:
            self.processor_agent.end()
        sys.exit(os.EX_OK)

    def _debug_dump(self, signum: int, frame: FrameType | None) -> None:
        if not _is_parent_process():
            # Only the parent process should perform the debug dump.
            return

        try:
            sig_name = signal.Signals(signum).name
        except Exception:
            sig_name = str(signum)

        self.log.info("%s\n%s received, printing debug\n%s", "-" * 80, sig_name, "-" * 80)

        self.job.executor.debug_dump()
        self.log.info("-" * 80)

    def __get_concurrency_maps(self, states: Iterable[TaskInstanceState], session: Session) -> ConcurrencyMap:
        """
        Get the concurrency maps.

        :param states: List of states to query for
        :return: Concurrency map
        """
        ti_concurrency_query: Result = session.execute(
            select(TI.task_id, TI.run_id, TI.dag_id, func.count("*"))
            .where(TI.state.in_(states))
            .group_by(TI.task_id, TI.run_id, TI.dag_id)
        )
        return ConcurrencyMap.from_concurrency_map(
            {(dag_id, run_id, task_id): count for task_id, run_id, dag_id, count in ti_concurrency_query}
        )

    def _executable_task_instances_to_queued(self, max_tis: int, session: Session) -> list[TI]:
        """
        Find TIs that are ready for execution based on conditions.

        Conditions include:
        - pool limits
        - DAG max_active_tasks
        - executor state
        - priority
        - max active tis per DAG
        - max active tis per DAG run

        :param max_tis: Maximum number of TIs to queue in this loop.
        :return: list[airflow.models.TaskInstance]
        """
        from airflow.models.pool import Pool
        from airflow.utils.db import DBLocks

        executable_tis: list[TI] = []

        if session.get_bind().dialect.name == "postgresql":
            # Optimization: to avoid littering the DB errors of "ERROR: canceling statement due to lock
            # timeout", try to take out a transactional advisory lock (unlocks automatically on
            # COMMIT/ROLLBACK)
            lock_acquired = session.execute(
                text("SELECT pg_try_advisory_xact_lock(:id)").bindparams(
                    id=DBLocks.SCHEDULER_CRITICAL_SECTION.value
                )
            ).scalar()
            if not lock_acquired:
                # Throw an error like the one that would happen with NOWAIT
                raise OperationalError(
                    "Failed to acquire advisory lock", params=None, orig=RuntimeError("55P03")
                )

        # Get the pool settings. We get a lock on the pool rows, treating this as a "critical section"
        # Throws an exception if lock cannot be obtained, rather than blocking
        pools = Pool.slots_stats(lock_rows=True, session=session)

        # If the pools are full, there is no point doing anything!
        # If _somehow_ the pool is overfull, don't let the limit go negative - it breaks SQL
        pool_slots_free = sum(max(0, pool["open"]) for pool in pools.values())

        if pool_slots_free == 0:
            self.log.debug("All pools are full!")
            return []

        max_tis = min(max_tis, pool_slots_free)

        starved_pools = {pool_name for pool_name, stats in pools.items() if stats["open"] <= 0}

        # dag_id to # of running tasks and (dag_id, task_id) to # of running tasks.
        concurrency_map = self.__get_concurrency_maps(states=EXECUTION_STATES, session=session)

        # Number of tasks that cannot be scheduled because of no open slot in pool
        num_starving_tasks_total = 0

        # dag and task ids that can't be queued because of concurrency limits
        starved_dags: set[str] = set()
        starved_tasks: set[tuple[str, str]] = set()
        starved_tasks_task_dagrun_concurrency: set[tuple[str, str, str]] = set()

        pool_num_starving_tasks: dict[str, int] = Counter()

        for loop_count in itertools.count(start=1):
            num_starved_pools = len(starved_pools)
            num_starved_dags = len(starved_dags)
            num_starved_tasks = len(starved_tasks)
            num_starved_tasks_task_dagrun_concurrency = len(starved_tasks_task_dagrun_concurrency)

            # Get task instances associated with scheduled
            # DagRuns which are not backfilled, in the given states,
            # and the dag is not paused
            query = (
                select(TI)
                .with_hint(TI, "USE INDEX (ti_state)", dialect_name="mysql")
                .join(TI.dag_run)
                .where(DR.run_type != DagRunType.BACKFILL_JOB, DR.state == DagRunState.RUNNING)
                .join(TI.dag_model)
                .where(not_(DM.is_paused))
                .where(TI.state == TaskInstanceState.SCHEDULED)
                .options(selectinload(TI.dag_model))
                .order_by(-TI.priority_weight, DR.execution_date, TI.map_index)
            )

            if starved_pools:
                query = query.where(not_(TI.pool.in_(starved_pools)))

            if starved_dags:
                query = query.where(not_(TI.dag_id.in_(starved_dags)))

            if starved_tasks:
                task_filter = tuple_in_condition((TI.dag_id, TI.task_id), starved_tasks)
                query = query.where(not_(task_filter))

            if starved_tasks_task_dagrun_concurrency:
                task_filter = tuple_in_condition(
                    (TI.dag_id, TI.run_id, TI.task_id),
                    starved_tasks_task_dagrun_concurrency,
                )
                query = query.where(not_(task_filter))

            query = query.limit(max_tis)

            timer = Stats.timer("scheduler.critical_section_query_duration")
            timer.start()

            try:
                query = with_row_locks(
                    query,
                    of=TI,
                    session=session,
                    **skip_locked(session=session),
                )
                task_instances_to_examine: list[TI] = session.scalars(query).all()

                timer.stop(send=True)
            except OperationalError as e:
                timer.stop(send=False)
                raise e

            # TODO[HA]: This was wrong before anyway, as it only looked at a sub-set of dags, not everything.
            # Stats.gauge('scheduler.tasks.pending', len(task_instances_to_examine))

            if len(task_instances_to_examine) == 0:
                self.log.debug("No tasks to consider for execution.")
                break

            # Put one task instance on each line
            task_instance_str = "\n\t".join(repr(x) for x in task_instances_to_examine)
            self.log.info(
                "%s tasks up for execution:\n\t%s", len(task_instances_to_examine), task_instance_str
            )

            for task_instance in task_instances_to_examine:
                pool_name = task_instance.pool

                pool_stats = pools.get(pool_name)
                if not pool_stats:
                    self.log.warning("Tasks using non-existent pool '%s' will not be scheduled", pool_name)
                    starved_pools.add(pool_name)
                    continue

                # Make sure to emit metrics if pool has no starving tasks
                pool_num_starving_tasks.setdefault(pool_name, 0)

                pool_total = pool_stats["total"]
                open_slots = pool_stats["open"]

                if open_slots <= 0:
                    self.log.info(
                        "Not scheduling since there are %s open slots in pool %s", open_slots, pool_name
                    )
                    # Can't schedule any more since there are no more open slots.
                    pool_num_starving_tasks[pool_name] += 1
                    num_starving_tasks_total += 1
                    starved_pools.add(pool_name)
                    continue

                if task_instance.pool_slots > pool_total:
                    self.log.warning(
                        "Not executing %s. Requested pool slots (%s) are greater than "
                        "total pool slots: '%s' for pool: %s.",
                        task_instance,
                        task_instance.pool_slots,
                        pool_total,
                        pool_name,
                    )

                    pool_num_starving_tasks[pool_name] += 1
                    num_starving_tasks_total += 1
                    starved_tasks.add((task_instance.dag_id, task_instance.task_id))
                    continue

                if task_instance.pool_slots > open_slots:
                    self.log.info(
                        "Not executing %s since it requires %s slots "
                        "but there are %s open slots in the pool %s.",
                        task_instance,
                        task_instance.pool_slots,
                        open_slots,
                        pool_name,
                    )
                    pool_num_starving_tasks[pool_name] += 1
                    num_starving_tasks_total += 1
                    starved_tasks.add((task_instance.dag_id, task_instance.task_id))
                    # Though we can execute tasks with lower priority if there's enough room
                    continue

                # Check to make sure that the task max_active_tasks of the DAG hasn't been
                # reached.
                dag_id = task_instance.dag_id

                current_active_tasks_per_dag = concurrency_map.dag_active_tasks_map[dag_id]
                max_active_tasks_per_dag_limit = task_instance.dag_model.max_active_tasks
                self.log.info(
                    "DAG %s has %s/%s running and queued tasks",
                    dag_id,
                    current_active_tasks_per_dag,
                    max_active_tasks_per_dag_limit,
                )
                if current_active_tasks_per_dag >= max_active_tasks_per_dag_limit:
                    self.log.info(
                        "Not executing %s since the number of tasks running or queued "
                        "from DAG %s is >= to the DAG's max_active_tasks limit of %s",
                        task_instance,
                        dag_id,
                        max_active_tasks_per_dag_limit,
                    )
                    starved_dags.add(dag_id)
                    continue

                if task_instance.dag_model.has_task_concurrency_limits:
                    # Many dags don't have a task_concurrency, so where we can avoid loading the full
                    # serialized DAG the better.
                    serialized_dag = self.dagbag.get_dag(dag_id, session=session)
                    # If the dag is missing, fail the task and continue to the next task.
                    if not serialized_dag:
                        self.log.error(
                            "DAG '%s' for task instance %s not found in serialized_dag table",
                            dag_id,
                            task_instance,
                        )
                        session.execute(
                            update(TI)
                            .where(TI.dag_id == dag_id, TI.state == TaskInstanceState.SCHEDULED)
                            .values(state=TaskInstanceState.FAILED)
                            .execution_options(synchronize_session="fetch")
                        )
                        continue

                    task_concurrency_limit: int | None = None
                    if serialized_dag.has_task(task_instance.task_id):
                        task_concurrency_limit = serialized_dag.get_task(
                            task_instance.task_id
                        ).max_active_tis_per_dag

                    if task_concurrency_limit is not None:
                        current_task_concurrency = concurrency_map.task_concurrency_map[
                            (task_instance.dag_id, task_instance.task_id)
                        ]

                        if current_task_concurrency >= task_concurrency_limit:
                            self.log.info(
                                "Not executing %s since the task concurrency for"
                                " this task has been reached.",
                                task_instance,
                            )
                            starved_tasks.add((task_instance.dag_id, task_instance.task_id))
                            continue

                    task_dagrun_concurrency_limit: int | None = None
                    if serialized_dag.has_task(task_instance.task_id):
                        task_dagrun_concurrency_limit = serialized_dag.get_task(
                            task_instance.task_id
                        ).max_active_tis_per_dagrun

                    if task_dagrun_concurrency_limit is not None:
                        current_task_dagrun_concurrency = concurrency_map.task_dagrun_concurrency_map[
                            (task_instance.dag_id, task_instance.run_id, task_instance.task_id)
                        ]

                        if current_task_dagrun_concurrency >= task_dagrun_concurrency_limit:
                            self.log.info(
                                "Not executing %s since the task concurrency per DAG run for"
                                " this task has been reached.",
                                task_instance,
                            )
                            starved_tasks_task_dagrun_concurrency.add(
                                (task_instance.dag_id, task_instance.run_id, task_instance.task_id)
                            )
                            continue

                executable_tis.append(task_instance)
                open_slots -= task_instance.pool_slots
                concurrency_map.dag_active_tasks_map[dag_id] += 1
                concurrency_map.task_concurrency_map[(task_instance.dag_id, task_instance.task_id)] += 1
                concurrency_map.task_dagrun_concurrency_map[
                    (task_instance.dag_id, task_instance.run_id, task_instance.task_id)
                ] += 1

                pool_stats["open"] = open_slots

            is_done = executable_tis or len(task_instances_to_examine) < max_tis
            # Check this to avoid accidental infinite loops
            found_new_filters = (
                len(starved_pools) > num_starved_pools
                or len(starved_dags) > num_starved_dags
                or len(starved_tasks) > num_starved_tasks
                or len(starved_tasks_task_dagrun_concurrency) > num_starved_tasks_task_dagrun_concurrency
            )

            if is_done or not found_new_filters:
                break

            self.log.info(
                "Found no task instances to queue on query iteration %s "
                "but there could be more candidate task instances to check.",
                loop_count,
            )

        for pool_name, num_starving_tasks in pool_num_starving_tasks.items():
            Stats.gauge(f"pool.starving_tasks.{pool_name}", num_starving_tasks)
            # Same metric with tagging
            Stats.gauge("pool.starving_tasks", num_starving_tasks, tags={"pool_name": pool_name})

        Stats.gauge("scheduler.tasks.starving", num_starving_tasks_total)
        Stats.gauge("scheduler.tasks.executable", len(executable_tis))

        if len(executable_tis) > 0:
            task_instance_str = "\n\t".join(repr(x) for x in executable_tis)
            self.log.info("Setting the following tasks to queued state:\n\t%s", task_instance_str)

            # set TIs to queued state
            filter_for_tis = TI.filter_for_tis(executable_tis)
            session.execute(
                update(TI)
                .where(filter_for_tis)
                .values(
                    # TODO[ha]: should we use func.now()? How does that work with DB timezone
                    # on mysql when it's not UTC?
                    state=TaskInstanceState.QUEUED,
                    queued_dttm=timezone.utcnow(),
                    queued_by_job_id=self.job.id,
                )
                .execution_options(synchronize_session=False)
            )

            for ti in executable_tis:
                ti.emit_state_change_metric(TaskInstanceState.QUEUED)

        for ti in executable_tis:
            make_transient(ti)
        return executable_tis

    def _enqueue_task_instances_with_queued_state(self, task_instances: list[TI], session: Session) -> None:
        """
        Enqueue task_instances which should have been set to queued with the executor.

        :param task_instances: TaskInstances to enqueue
        :param session: The session object
        """
        # actually enqueue them
        for ti in task_instances:
            if ti.dag_run.state in State.finished_dr_states:
                ti.set_state(None, session=session)
                continue
            command = ti.command_as_list(
                local=True,
                pickle_id=ti.dag_model.pickle_id,
            )

            priority = ti.priority_weight
            queue = ti.queue
            self.log.info("Sending %s to executor with priority %s and queue %s", ti.key, priority, queue)

            self.job.executor.queue_command(
                ti,
                command,
                priority=priority,
                queue=queue,
            )

    def _critical_section_enqueue_task_instances(self, session: Session) -> int:
        """
        Enqueues TaskInstances for execution.

        There are three steps:
        1. Pick TIs by priority with the constraint that they are in the expected states
        and that we do not exceed max_active_runs or pool limits.
        2. Change the state for the TIs above atomically.
        3. Enqueue the TIs in the executor.

        HA note: This function is a "critical section" meaning that only a single executor process can execute
        this function at the same time. This is achieved by doing ``SELECT ... from pool FOR UPDATE``. For DBs
        that support NOWAIT, a "blocked" scheduler will skip this and continue on with other tasks (creating
        new DAG runs, progressing TIs from None to SCHEDULED etc.); DBs that don't support this (such as
        MariaDB or MySQL 5.x) the other schedulers will wait for the lock before continuing.

        :param session:
        :return: Number of task instance with state changed.
        """
        if self.job.max_tis_per_query == 0:
            max_tis = self.job.executor.slots_available
        else:
            max_tis = min(self.job.max_tis_per_query, self.job.executor.slots_available)
        queued_tis = self._executable_task_instances_to_queued(max_tis, session=session)

        self._enqueue_task_instances_with_queued_state(queued_tis, session=session)
        return len(queued_tis)

    def _process_executor_events(self, session: Session) -> int:
        """Respond to executor events."""
        if not self._standalone_dag_processor and not self.processor_agent:
            raise ValueError("Processor agent is not started.")
        ti_primary_key_to_try_number_map: dict[tuple[str, str, str, int], int] = {}
        event_buffer = self.job.executor.get_event_buffer()
        tis_with_right_state: list[TaskInstanceKey] = []

        # Report execution
        for ti_key, (state, _) in event_buffer.items():
            # We create map (dag_id, task_id, execution_date) -> in-memory try_number
            ti_primary_key_to_try_number_map[ti_key.primary] = ti_key.try_number

            self.log.info("Received executor event with state %s for task instance %s", state, ti_key)
            if state in (TaskInstanceState.FAILED, TaskInstanceState.SUCCESS, TaskInstanceState.QUEUED):
                tis_with_right_state.append(ti_key)

        # Return if no finished tasks
        if not tis_with_right_state:
            return len(event_buffer)

        # Check state of finished tasks
        filter_for_tis = TI.filter_for_tis(tis_with_right_state)
        query = select(TI).where(filter_for_tis).options(selectinload(TI.dag_model))
        # row lock this entire set of taskinstances to make sure the scheduler doesn't fail when we have
        # multi-schedulers
        tis: Iterator[TI] = with_row_locks(
            query,
            of=TI,
            session=session,
            **skip_locked(session=session),
        )
        tis = session.scalars(tis)
        for ti in tis:
            try_number = ti_primary_key_to_try_number_map[ti.key.primary]
            buffer_key = ti.key.with_try_number(try_number)
            state, info = event_buffer.pop(buffer_key)

            if state == TaskInstanceState.QUEUED:
                ti.external_executor_id = info
                self.log.info("Setting external_id for %s to %s", ti, info)
                continue

            msg = (
                "TaskInstance Finished: dag_id=%s, task_id=%s, run_id=%s, map_index=%s, "
                "run_start_date=%s, run_end_date=%s, "
                "run_duration=%s, state=%s, executor_state=%s, try_number=%s, max_tries=%s, job_id=%s, "
                "pool=%s, queue=%s, priority_weight=%d, operator=%s, queued_dttm=%s, "
                "queued_by_job_id=%s, pid=%s"
            )
            self.log.info(
                msg,
                ti.dag_id,
                ti.task_id,
                ti.run_id,
                ti.map_index,
                ti.start_date,
                ti.end_date,
                ti.duration,
                ti.state,
                state,
                try_number,
                ti.max_tries,
                ti.job_id,
                ti.pool,
                ti.queue,
                ti.priority_weight,
                ti.operator,
                ti.queued_dttm,
                ti.queued_by_job_id,
                ti.pid,
            )

            # There are two scenarios why the same TI with the same try_number is queued
            # after executor is finished with it:
            # 1) the TI was killed externally and it had no time to mark itself failed
            # - in this case we should mark it as failed here.
            # 2) the TI has been requeued after getting deferred - in this case either our executor has it
            # or the TI is queued by another job. Either ways we should not fail it.

            # All of this could also happen if the state is "running",
            # but that is handled by the zombie detection.

            ti_queued = ti.try_number == buffer_key.try_number and ti.state == TaskInstanceState.QUEUED
            ti_requeued = (
                ti.queued_by_job_id != self.job.id  # Another scheduler has queued this task again
                or self.job.executor.has_task(ti)  # This scheduler has this task already
            )

            if ti_queued and not ti_requeued:
                Stats.incr(
                    "scheduler.tasks.killed_externally",
                    tags={"dag_id": ti.dag_id, "task_id": ti.task_id},
                )
                msg = (
                    "Executor reports task instance %s finished (%s) although the "
                    "task says it's %s. (Info: %s) Was the task killed externally?"
                )
                self.log.error(msg, ti, state, ti.state, info)

                # Get task from the Serialized DAG
                try:
                    dag = self.dagbag.get_dag(ti.dag_id)
                    task = dag.get_task(ti.task_id)
                except Exception:
                    self.log.exception("Marking task instance %s as %s", ti, state)
                    ti.set_state(state)
                    continue
                ti.task = task
                if task.on_retry_callback or task.on_failure_callback:
                    request = TaskCallbackRequest(
                        full_filepath=ti.dag_model.fileloc,
                        simple_task_instance=SimpleTaskInstance.from_ti(ti),
                        msg=msg % (ti, state, ti.state, info),
                        processor_subdir=ti.dag_model.processor_subdir,
                    )
                    self.job.executor.send_callback(request)
                else:
                    ti.handle_failure(error=msg % (ti, state, ti.state, info), session=session)

        return len(event_buffer)

    def _execute(self) -> int | None:
        from airflow.dag_processing.manager import DagFileProcessorAgent

        self.log.info("Starting the scheduler")

        executor_class, _ = ExecutorLoader.import_default_executor_cls()

        # DAGs can be pickled for easier remote execution by some executors
        pickle_dags = self.do_pickle and executor_class.supports_pickling

        self.log.info("Processing each file at most %s times", self.num_times_parse_dags)

        # When using sqlite, we do not use async_mode
        # so the scheduler job and DAG parser don't access the DB at the same time.
        async_mode = not self.using_sqlite

        processor_timeout_seconds: int = conf.getint("core", "dag_file_processor_timeout")
        processor_timeout = timedelta(seconds=processor_timeout_seconds)
        if not self._standalone_dag_processor:
            self.processor_agent = DagFileProcessorAgent(
                dag_directory=Path(self.subdir),
                max_runs=self.num_times_parse_dags,
                processor_timeout=processor_timeout,
                dag_ids=[],
                pickle_dags=pickle_dags,
                async_mode=async_mode,
            )

        try:
            self.job.executor.job_id = self.job.id
            if self.processor_agent:
                self.log.debug("Using PipeCallbackSink as callback sink.")
                self.job.executor.callback_sink = PipeCallbackSink(
                    get_sink_pipe=self.processor_agent.get_callbacks_pipe
                )
            else:
                from airflow.callbacks.database_callback_sink import DatabaseCallbackSink

                self.log.debug("Using DatabaseCallbackSink as callback sink.")
                self.job.executor.callback_sink = DatabaseCallbackSink()

            self.job.executor.start()

            self.register_signals()

            if self.processor_agent:
                self.processor_agent.start()

            execute_start_time = timezone.utcnow()

            self._run_scheduler_loop()

            if self.processor_agent:
                # Stop any processors
                self.processor_agent.terminate()

                # Verify that all files were processed, and if so, deactivate DAGs that
                # haven't been touched by the scheduler as they likely have been
                # deleted.
                if self.processor_agent.all_files_processed:
                    self.log.info(
                        "Deactivating DAGs that haven't been touched since %s", execute_start_time.isoformat()
                    )
                    DAG.deactivate_stale_dags(execute_start_time)

            settings.Session.remove()  # type: ignore
        except Exception:
            self.log.exception("Exception when executing SchedulerJob._run_scheduler_loop")
            raise
        finally:
            try:
                self.job.executor.end()
            except Exception:
                self.log.exception("Exception when executing Executor.end")
            if self.processor_agent:
                try:
                    self.processor_agent.end()
                except Exception:
                    self.log.exception("Exception when executing DagFileProcessorAgent.end")
            self.log.info("Exited execute loop")
        return None

    @provide_session
    def _update_dag_run_state_for_paused_dags(self, session: Session = NEW_SESSION) -> None:
        try:
            paused_runs = session.scalars(
                select(DagRun)
                .join(DagRun.dag_model)
                .join(TI)
                .where(
                    DagModel.is_paused == expression.true(),
                    DagRun.state == DagRunState.RUNNING,
                    DagRun.run_type != DagRunType.BACKFILL_JOB,
                )
                .having(DagRun.last_scheduling_decision <= func.max(TI.updated_at))
                .group_by(DagRun)
            )
            for dag_run in paused_runs:
                dag = self.dagbag.get_dag(dag_run.dag_id, session=session)
                if dag is None:
                    continue

                dag_run.dag = dag
                _, callback_to_run = dag_run.update_state(execute_callbacks=False, session=session)
                if callback_to_run:
                    self._send_dag_callbacks_to_processor(dag, callback_to_run)
        except Exception as e:  # should not fail the scheduler
            self.log.exception("Failed to update dag run state for paused dags due to %s", str(e))

    def _run_scheduler_loop(self) -> None:
        """
        The actual scheduler loop.

        The main steps in the loop are:
            #. Harvest DAG parsing results through DagFileProcessorAgent
            #. Find and queue executable tasks
                #. Change task instance state in DB
                #. Queue tasks in executor
            #. Heartbeat executor
                #. Execute queued tasks in executor asynchronously
                #. Sync on the states of running tasks

        Following is a graphic representation of these steps.

        .. image:: ../docs/apache-airflow/img/scheduler_loop.jpg

        """
        if not self.processor_agent and not self._standalone_dag_processor:
            raise ValueError("Processor agent is not started.")
        is_unit_test: bool = conf.getboolean("core", "unit_test_mode")

        timers = EventScheduler()

        # Check on start up, then every configured interval
        self.adopt_or_reset_orphaned_tasks()

        timers.call_regular_interval(
            conf.getfloat("scheduler", "orphaned_tasks_check_interval", fallback=300.0),
            self.adopt_or_reset_orphaned_tasks,
        )

        timers.call_regular_interval(
            conf.getfloat("scheduler", "trigger_timeout_check_interval", fallback=15.0),
            self.check_trigger_timeouts,
        )

        timers.call_regular_interval(
            conf.getfloat("scheduler", "pool_metrics_interval", fallback=5.0),
            self._emit_pool_metrics,
        )

        timers.call_regular_interval(
            conf.getfloat("scheduler", "zombie_detection_interval", fallback=10.0),
            self._find_zombies,
        )

        timers.call_regular_interval(60.0, self._update_dag_run_state_for_paused_dags)

        timers.call_regular_interval(
            conf.getfloat("scheduler", "task_queued_timeout_check_interval"),
            self._fail_tasks_stuck_in_queued,
        )

        timers.call_regular_interval(
            conf.getfloat("scheduler", "parsing_cleanup_interval"),
            self._orphan_unreferenced_datasets,
        )

        if self._standalone_dag_processor:
            timers.call_regular_interval(
                conf.getfloat("scheduler", "parsing_cleanup_interval"),
                self._cleanup_stale_dags,
            )

        for loop_count in itertools.count(start=1):
            with Stats.timer("scheduler.scheduler_loop_duration") as timer:
                if self.using_sqlite and self.processor_agent:
                    self.processor_agent.run_single_parsing_loop()
                    # For the sqlite case w/ 1 thread, wait until the processor
                    # is finished to avoid concurrent access to the DB.
                    self.log.debug("Waiting for processors to finish since we're using sqlite")
                    self.processor_agent.wait_until_finished()

                with create_session() as session:
                    num_queued_tis = self._do_scheduling(session)

                    self.job.executor.heartbeat()
                    session.expunge_all()
                    num_finished_events = self._process_executor_events(session=session)
                if self.processor_agent:
                    self.processor_agent.heartbeat()

                # Heartbeat the scheduler periodically
                perform_heartbeat(
                    job=self.job, heartbeat_callback=self.heartbeat_callback, only_if_necessary=True
                )

                # Run any pending timed events
                next_event = timers.run(blocking=False)
                self.log.debug("Next timed event is in %f", next_event)

            self.log.debug("Ran scheduling loop in %.2f seconds", timer.duration)

            if not is_unit_test and not num_queued_tis and not num_finished_events:
                # If the scheduler is doing things, don't sleep. This means when there is work to do, the
                # scheduler will run "as quick as possible", but when it's stopped, it can sleep, dropping CPU
                # usage when "idle"
                time.sleep(min(self._scheduler_idle_sleep_time, next_event if next_event else 0))

            if loop_count >= self.num_runs > 0:
                self.log.info(
                    "Exiting scheduler loop as requested number of runs (%d - got to %d) has been reached",
                    self.num_runs,
                    loop_count,
                )
                break
            if self.processor_agent and self.processor_agent.done:
                self.log.info(
                    "Exiting scheduler loop as requested DAG parse count (%d) has been reached after %d"
                    " scheduler loops",
                    self.num_times_parse_dags,
                    loop_count,
                )
                break

    def _do_scheduling(self, session: Session) -> int:
        """
        This function is where the main scheduling decisions take places.

        It:
        - Creates any necessary DAG runs by examining the next_dagrun_create_after column of DagModel

          Since creating Dag Runs is a relatively time consuming process, we select only 10 dags by default
          (configurable via ``scheduler.max_dagruns_to_create_per_loop`` setting) - putting this higher will
          mean one scheduler could spend a chunk of time creating dag runs, and not ever get around to
          scheduling tasks.

        - Finds the "next n oldest" running DAG Runs to examine for scheduling (n=20 by default, configurable
          via ``scheduler.max_dagruns_per_loop_to_schedule`` config setting) and tries to progress state (TIs
          to SCHEDULED, or DagRuns to SUCCESS/FAILURE etc)

          By "next oldest", we mean hasn't been examined/scheduled in the most time.

          We don't select all dagruns at once, because the rows are selected with row locks, meaning
          that only one scheduler can "process them", even it is waiting behind other dags. Increasing this
          limit will allow more throughput for smaller DAGs but will likely slow down throughput for larger
          (>500 tasks.) DAGs

        - Then, via a Critical Section (locking the rows of the Pool model) we queue tasks, and then send them
          to the executor.

          See docs of _critical_section_enqueue_task_instances for more.

        :return: Number of TIs enqueued in this iteration
        """
        # Put a check in place to make sure we don't commit unexpectedly
        with prohibit_commit(session) as guard:
            if settings.USE_JOB_SCHEDULE:
                self._create_dagruns_for_dags(guard, session)

            self._start_queued_dagruns(session)
            guard.commit()
            dag_runs = self._get_next_dagruns_to_examine(DagRunState.RUNNING, session)
            # Bulk fetch the currently active dag runs for the dags we are
            # examining, rather than making one query per DagRun

            callback_tuples = self._schedule_all_dag_runs(guard, dag_runs, session)

        # Send the callbacks after we commit to ensure the context is up to date when it gets run
        # cache saves time during scheduling of many dag_runs for same dag
        cached_get_dag: Callable[[str], DAG | None] = lru_cache()(
            partial(self.dagbag.get_dag, session=session)
        )
        for dag_run, callback_to_run in callback_tuples:
            dag = cached_get_dag(dag_run.dag_id)

            if not dag:
                self.log.error("DAG '%s' not found in serialized_dag table", dag_run.dag_id)
                continue
            # Sending callbacks there as in standalone_dag_processor they are adding to the database,
            # so it must be done outside of prohibit_commit.
            self._send_dag_callbacks_to_processor(dag, callback_to_run)

        with prohibit_commit(session) as guard:
            # Without this, the session has an invalid view of the DB
            session.expunge_all()
            # END: schedule TIs

            if self.job.executor.slots_available <= 0:
                # We know we can't do anything here, so don't even try!
                self.log.debug("Executor full, skipping critical section")
                num_queued_tis = 0
            else:
                try:
                    timer = Stats.timer("scheduler.critical_section_duration")
                    timer.start()

                    # Find anything TIs in state SCHEDULED, try to QUEUE it (send it to the executor)
                    num_queued_tis = self._critical_section_enqueue_task_instances(session=session)

                    # Make sure we only sent this metric if we obtained the lock, otherwise we'll skew the
                    # metric, way down
                    timer.stop(send=True)
                except OperationalError as e:
                    timer.stop(send=False)

                    if is_lock_not_available_error(error=e):
                        self.log.debug("Critical section lock held by another Scheduler")
                        Stats.incr("scheduler.critical_section_busy")
                        session.rollback()
                        return 0
                    raise

            guard.commit()

        return num_queued_tis

    @retry_db_transaction
    def _get_next_dagruns_to_examine(self, state: DagRunState, session: Session) -> Query:
        """Get Next DagRuns to Examine with retries."""
        return DagRun.next_dagruns_to_examine(state, session)

    @retry_db_transaction
    def _create_dagruns_for_dags(self, guard: CommitProhibitorGuard, session: Session) -> None:
        """Find Dag Models needing DagRuns and Create Dag Runs with retries in case of OperationalError."""
        query, dataset_triggered_dag_info = DagModel.dags_needing_dagruns(session)
        all_dags_needing_dag_runs = set(query.all())
        dataset_triggered_dags = [
            dag for dag in all_dags_needing_dag_runs if dag.dag_id in dataset_triggered_dag_info
        ]
        non_dataset_dags = all_dags_needing_dag_runs.difference(dataset_triggered_dags)
        self._create_dag_runs(non_dataset_dags, session)
        if dataset_triggered_dags:
            self._create_dag_runs_dataset_triggered(
                dataset_triggered_dags, dataset_triggered_dag_info, session
            )

        # commit the session - Release the write lock on DagModel table.
        guard.commit()
        # END: create dagruns

    def _create_dag_runs(self, dag_models: Collection[DagModel], session: Session) -> None:
        """Create a DAG run and update the dag_model to control if/when the next DAGRun should be created."""
        # Bulk Fetch DagRuns with dag_id and execution_date same
        # as DagModel.dag_id and DagModel.next_dagrun
        # This list is used to verify if the DagRun already exist so that we don't attempt to create
        # duplicate dag runs
        existing_dagruns = (
            session.execute(
                select(DagRun.dag_id, DagRun.execution_date).where(
                    tuple_in_condition(
                        (DagRun.dag_id, DagRun.execution_date),
                        ((dm.dag_id, dm.next_dagrun) for dm in dag_models),
                    ),
                )
            )
            .unique()
            .all()
        )

        active_runs_of_dags = Counter(
            DagRun.active_runs_of_dags(dag_ids=(dm.dag_id for dm in dag_models), session=session),
        )

        for dag_model in dag_models:
            dag = self.dagbag.get_dag(dag_model.dag_id, session=session)
            if not dag:
                self.log.error("DAG '%s' not found in serialized_dag table", dag_model.dag_id)
                continue

            dag_hash = self.dagbag.dags_hash.get(dag.dag_id)

            data_interval = dag.get_next_data_interval(dag_model)
            # Explicitly check if the DagRun already exists. This is an edge case
            # where a Dag Run is created but `DagModel.next_dagrun` and `DagModel.next_dagrun_create_after`
            # are not updated.
            # We opted to check DagRun existence instead
            # of catching an Integrity error and rolling back the session i.e
            # we need to set dag.next_dagrun_info if the Dag Run already exists or if we
            # create a new one. This is so that in the next Scheduling loop we try to create new runs
            # instead of falling in a loop of Integrity Error.
            if (dag.dag_id, dag_model.next_dagrun) not in existing_dagruns:
                dag.create_dagrun(
                    run_type=DagRunType.SCHEDULED,
                    execution_date=dag_model.next_dagrun,
                    state=DagRunState.QUEUED,
                    data_interval=data_interval,
                    external_trigger=False,
                    session=session,
                    dag_hash=dag_hash,
                    creating_job_id=self.job.id,
                )
                active_runs_of_dags[dag.dag_id] += 1
            if self._should_update_dag_next_dagruns(
                dag, dag_model, active_runs_of_dags[dag.dag_id], session=session
            ):
                dag_model.calculate_dagrun_date_fields(dag, data_interval)
        # TODO[HA]: Should we do a session.flush() so we don't have to keep lots of state/object in
        # memory for larger dags? or expunge_all()

    def _create_dag_runs_dataset_triggered(
        self,
        dag_models: Collection[DagModel],
        dataset_triggered_dag_info: dict[str, tuple[datetime, datetime]],
        session: Session,
    ) -> None:
        """For DAGs that are triggered by datasets, create dag runs."""
        # Bulk Fetch DagRuns with dag_id and execution_date same
        # as DagModel.dag_id and DagModel.next_dagrun
        # This list is used to verify if the DagRun already exist so that we don't attempt to create
        # duplicate dag runs
        exec_dates = {
            dag_id: timezone.coerce_datetime(last_time)
            for dag_id, (_, last_time) in dataset_triggered_dag_info.items()
        }
        existing_dagruns: set[tuple[str, timezone.DateTime]] = set(
            session.execute(
                select(DagRun.dag_id, DagRun.execution_date).where(
                    tuple_in_condition((DagRun.dag_id, DagRun.execution_date), exec_dates.items())
                )
            )
        )

        for dag_model in dag_models:
            dag = self.dagbag.get_dag(dag_model.dag_id, session=session)
            if not dag:
                self.log.error("DAG '%s' not found in serialized_dag table", dag_model.dag_id)
                continue

            if not isinstance(dag.timetable, DatasetTriggeredTimetable):
                self.log.error(
                    "DAG '%s' was dataset-scheduled, but didn't have a DatasetTriggeredTimetable!",
                    dag_model.dag_id,
                )
                continue

            dag_hash = self.dagbag.dags_hash.get(dag.dag_id)

            # Explicitly check if the DagRun already exists. This is an edge case
            # where a Dag Run is created but `DagModel.next_dagrun` and `DagModel.next_dagrun_create_after`
            # are not updated.
            # We opted to check DagRun existence instead
            # of catching an Integrity error and rolling back the session i.e
            # we need to set dag.next_dagrun_info if the Dag Run already exists or if we
            # create a new one. This is so that in the next Scheduling loop we try to create new runs
            # instead of falling in a loop of Integrity Error.
            exec_date = exec_dates[dag.dag_id]
            if (dag.dag_id, exec_date) not in existing_dagruns:
                previous_dag_run = session.scalar(
                    select(DagRun)
                    .where(
                        DagRun.dag_id == dag.dag_id,
                        DagRun.execution_date < exec_date,
                        DagRun.run_type == DagRunType.DATASET_TRIGGERED,
                    )
                    .order_by(DagRun.execution_date.desc())
                    .limit(1)
                )
                dataset_event_filters = [
                    DagScheduleDatasetReference.dag_id == dag.dag_id,
                    DatasetEvent.timestamp <= exec_date,
                ]
                if previous_dag_run:
                    dataset_event_filters.append(DatasetEvent.timestamp > previous_dag_run.execution_date)

                dataset_events = session.scalars(
                    select(DatasetEvent)
                    .join(
                        DagScheduleDatasetReference,
                        DatasetEvent.dataset_id == DagScheduleDatasetReference.dataset_id,
                    )
                    .join(DatasetEvent.source_dag_run)
                    .where(*dataset_event_filters)
                ).all()

                data_interval = dag.timetable.data_interval_for_events(exec_date, dataset_events)
                run_id = dag.timetable.generate_run_id(
                    run_type=DagRunType.DATASET_TRIGGERED,
                    logical_date=exec_date,
                    data_interval=data_interval,
                    session=session,
                    events=dataset_events,
                )

                dag_run = dag.create_dagrun(
                    run_id=run_id,
                    run_type=DagRunType.DATASET_TRIGGERED,
                    execution_date=exec_date,
                    data_interval=data_interval,
                    state=DagRunState.QUEUED,
                    external_trigger=False,
                    session=session,
                    dag_hash=dag_hash,
                    creating_job_id=self.job.id,
                )
                Stats.incr("dataset.triggered_dagruns")
                dag_run.consumed_dataset_events.extend(dataset_events)
                session.execute(
                    delete(DatasetDagRunQueue).where(DatasetDagRunQueue.target_dag_id == dag_run.dag_id)
                )

    def _should_update_dag_next_dagruns(
        self, dag: DAG, dag_model: DagModel, total_active_runs: int | None = None, *, session: Session
    ) -> bool:
        """Check if the dag's next_dagruns_create_after should be updated."""
        # If the DAG never schedules skip save runtime
        if not dag.timetable.can_be_scheduled:
            return False

        # get active dag runs from DB if not available
        if not total_active_runs:
            total_active_runs = dag.get_num_active_runs(only_running=False, session=session)

        if total_active_runs and total_active_runs >= dag.max_active_runs:
            self.log.info(
                "DAG %s is at (or above) max_active_runs (%d of %d), not creating any more runs",
                dag_model.dag_id,
                total_active_runs,
                dag.max_active_runs,
            )
            dag_model.next_dagrun_create_after = None
            return False
        return True

    def _start_queued_dagruns(self, session: Session) -> None:
        """Find DagRuns in queued state and decide moving them to running state."""
        # added all() to save runtime, otherwise query is executed more than once
        dag_runs: Collection[DagRun] = self._get_next_dagruns_to_examine(DagRunState.QUEUED, session).all()

        active_runs_of_dags = Counter(
            DagRun.active_runs_of_dags((dr.dag_id for dr in dag_runs), only_running=True, session=session),
        )

        def _update_state(dag: DAG, dag_run: DagRun):
            dag_run.state = DagRunState.RUNNING
            dag_run.start_date = timezone.utcnow()
            if dag.timetable.periodic:
                # TODO: Logically, this should be DagRunInfo.run_after, but the
                # information is not stored on a DagRun, only before the actual
                # execution on DagModel.next_dagrun_create_after. We should add
                # a field on DagRun for this instead of relying on the run
                # always happening immediately after the data interval.
                expected_start_date = dag.get_run_data_interval(dag_run).end
                schedule_delay = dag_run.start_date - expected_start_date
                # Publish metrics twice with backward compatible name, and then with tags
                Stats.timing(f"dagrun.schedule_delay.{dag.dag_id}", schedule_delay)
                Stats.timing(
                    "dagrun.schedule_delay",
                    schedule_delay,
                    tags={"dag_id": dag.dag_id},
                )

        # cache saves time during scheduling of many dag_runs for same dag
        cached_get_dag: Callable[[str], DAG | None] = lru_cache()(
            partial(self.dagbag.get_dag, session=session)
        )

        for dag_run in dag_runs:
            dag = dag_run.dag = cached_get_dag(dag_run.dag_id)

            if not dag:
                self.log.error("DAG '%s' not found in serialized_dag table", dag_run.dag_id)
                continue
            active_runs = active_runs_of_dags[dag_run.dag_id]

            if dag.max_active_runs and active_runs >= dag.max_active_runs:
                self.log.debug(
                    "DAG %s already has %d active runs, not moving any more runs to RUNNING state %s",
                    dag.dag_id,
                    active_runs,
                    dag_run.execution_date,
                )
            else:
                active_runs_of_dags[dag_run.dag_id] += 1
                _update_state(dag, dag_run)
                dag_run.notify_dagrun_state_changed()

    @retry_db_transaction
    def _schedule_all_dag_runs(
        self,
        guard: CommitProhibitorGuard,
        dag_runs: Iterable[DagRun],
        session: Session,
    ) -> list[tuple[DagRun, DagCallbackRequest | None]]:
        """Makes scheduling decisions for all `dag_runs`."""
        callback_tuples = [(run, self._schedule_dag_run(run, session=session)) for run in dag_runs]
        guard.commit()
        return callback_tuples

    def _schedule_dag_run(
        self,
        dag_run: DagRun,
        session: Session,
    ) -> DagCallbackRequest | None:
        """
        Make scheduling decisions about an individual dag run.

        :param dag_run: The DagRun to schedule
        :return: Callback that needs to be executed
        """
        callback: DagCallbackRequest | None = None

        dag = dag_run.dag = self.dagbag.get_dag(dag_run.dag_id, session=session)
        # Adopt row locking to account for inconsistencies when next_dagrun_create_after = None
        query = (
            session.query(DagModel)
            .filter(DagModel.dag_id == dag_run.dag_id)
            .options(joinedload(DagModel.parent_dag))
        )
        dag_model = with_row_locks(
            query, of=DagModel, session=session, **skip_locked(session=session)
        ).one_or_none()

        if not dag:
            self.log.error("Couldn't find DAG %s in DAG bag or database!", dag_run.dag_id)
            return callback
        if not dag_model:
            self.log.info(
                "DAG %s scheduling was skipped, probably because the DAG record was locked", dag_run.dag_id
            )
            return callback

        if (
            dag_run.start_date
            and dag.dagrun_timeout
            and dag_run.start_date < timezone.utcnow() - dag.dagrun_timeout
        ):
            dag_run.set_state(DagRunState.FAILED)
            unfinished_task_instances = session.scalars(
                select(TI)
                .where(TI.dag_id == dag_run.dag_id)
                .where(TI.run_id == dag_run.run_id)
                .where(TI.state.in_(State.unfinished))
            )
            for task_instance in unfinished_task_instances:
                task_instance.state = TaskInstanceState.SKIPPED
                session.merge(task_instance)
            session.flush()
            self.log.info("Run %s of %s has timed-out", dag_run.run_id, dag_run.dag_id)
            # Work out if we should allow creating a new DagRun now?
            if self._should_update_dag_next_dagruns(dag, dag_model, session=session):
                dag_model.calculate_dagrun_date_fields(dag, dag.get_run_data_interval(dag_run))

            callback_to_execute = DagCallbackRequest(
                full_filepath=dag.fileloc,
                dag_id=dag.dag_id,
                run_id=dag_run.run_id,
                is_failure_callback=True,
                processor_subdir=dag_model.processor_subdir,
                msg="timed_out",
            )

            dag_run.notify_dagrun_state_changed()
            duration = dag_run.end_date - dag_run.start_date
            Stats.timing(f"dagrun.duration.failed.{dag_run.dag_id}", duration)
            Stats.timing("dagrun.duration.failed", duration, tags={"dag_id": dag_run.dag_id})
            return callback_to_execute

        if dag_run.execution_date > timezone.utcnow() and not dag.allow_future_exec_dates:
            self.log.error("Execution date is in future: %s", dag_run.execution_date)
            return callback

        if not self._verify_integrity_if_dag_changed(dag_run=dag_run, session=session):
            self.log.warning("The DAG disappeared before verifying integrity: %s. Skipping.", dag_run.dag_id)
            return callback
        # TODO[HA]: Rename update_state -> schedule_dag_run, ?? something else?
        schedulable_tis, callback_to_run = dag_run.update_state(session=session, execute_callbacks=False)
        # Check if DAG not scheduled then skip interval calculation to same scheduler runtime
        if dag_run.state in State.finished_dr_states:
            # Work out if we should allow creating a new DagRun now?
            if self._should_update_dag_next_dagruns(dag, dag_model, session=session):
                dag_model.calculate_dagrun_date_fields(dag, dag.get_run_data_interval(dag_run))
        # This will do one query per dag run. We "could" build up a complex
        # query to update all the TIs across all the execution dates and dag
        # IDs in a single query, but it turns out that can be _very very slow_
        # see #11147/commit ee90807ac for more details
        dag_run.schedule_tis(schedulable_tis, session, max_tis_per_query=self.job.max_tis_per_query)

        return callback_to_run

    def _verify_integrity_if_dag_changed(self, dag_run: DagRun, session: Session) -> bool:
        """
        Only run DagRun.verify integrity if Serialized DAG has changed since it is slow.

        Return True if we determine that DAG still exists.
        """
        latest_version = SerializedDagModel.get_latest_version_hash(dag_run.dag_id, session=session)
        if dag_run.dag_hash == latest_version:
            self.log.debug("DAG %s not changed structure, skipping dagrun.verify_integrity", dag_run.dag_id)
            return True

        dag_run.dag_hash = latest_version

        # Refresh the DAG
        dag_run.dag = self.dagbag.get_dag(dag_id=dag_run.dag_id, session=session)
        if not dag_run.dag:
            return False

        # Verify integrity also takes care of session.flush
        dag_run.verify_integrity(session=session)
        return True

    def _send_dag_callbacks_to_processor(self, dag: DAG, callback: DagCallbackRequest | None = None) -> None:
        self._send_sla_callbacks_to_processor(dag)
        if callback:
            self.job.executor.send_callback(callback)
        else:
            self.log.debug("callback is empty")

    def _send_sla_callbacks_to_processor(self, dag: DAG) -> None:
        """Sends SLA Callbacks to DagFileProcessor if tasks have SLAs set and check_slas=True."""
        if not settings.CHECK_SLAS:
            return

        if not any(isinstance(task.sla, timedelta) for task in dag.tasks):
            self.log.debug("Skipping SLA check for %s because no tasks in DAG have SLAs", dag)
            return

        if not dag.timetable.periodic:
            self.log.debug("Skipping SLA check for %s because DAG is not scheduled", dag)
            return

        dag_model = DagModel.get_dagmodel(dag.dag_id)
        if not dag_model:
            self.log.error("Couldn't find DAG %s in database!", dag.dag_id)
            return

        request = SlaCallbackRequest(
            full_filepath=dag.fileloc,
            dag_id=dag.dag_id,
            processor_subdir=dag_model.processor_subdir,
        )
        self.job.executor.send_callback(request)

    @provide_session
    def _fail_tasks_stuck_in_queued(self, session: Session = NEW_SESSION) -> None:
        """
        Mark tasks stuck in queued for longer than `task_queued_timeout` as failed.

        Tasks can get stuck in queued for a wide variety of reasons (e.g. celery loses
        track of a task, a cluster can't further scale up its workers, etc.), but tasks
        should not be stuck in queued for a long time. This will mark tasks stuck in
        queued for longer than `self._task_queued_timeout` as failed. If the task has
        available retries, it will be retried.
        """
        self.log.debug("Calling SchedulerJob._fail_tasks_stuck_in_queued method")

        tasks_stuck_in_queued = session.scalars(
            select(TI).where(
                TI.state == TaskInstanceState.QUEUED,
                TI.queued_dttm < (timezone.utcnow() - timedelta(seconds=self._task_queued_timeout)),
                TI.queued_by_job_id == self.job.id,
            )
        ).all()
        try:
            tis_for_warning_message = self.job.executor.cleanup_stuck_queued_tasks(tis=tasks_stuck_in_queued)
            if tis_for_warning_message:
                task_instance_str = "\n\t".join(tis_for_warning_message)
                self.log.warning(
                    "Marked the following %s task instances stuck in queued as failed. "
                    "If the task instance has available retries, it will be retried.\n\t%s",
                    len(tasks_stuck_in_queued),
                    task_instance_str,
                )
        except NotImplementedError:
            self.log.debug("Executor doesn't support cleanup of stuck queued tasks. Skipping.")
            ...

    @provide_session
    def _emit_pool_metrics(self, session: Session = NEW_SESSION) -> None:
        from airflow.models.pool import Pool

        pools = Pool.slots_stats(session=session)
        for pool_name, slot_stats in pools.items():
            Stats.gauge(f"pool.open_slots.{pool_name}", slot_stats["open"])
            Stats.gauge(f"pool.queued_slots.{pool_name}", slot_stats["queued"])
            Stats.gauge(f"pool.running_slots.{pool_name}", slot_stats["running"])
            Stats.gauge(f"pool.deferred_slots.{pool_name}", slot_stats["deferred"])
            # Same metrics with tagging
            Stats.gauge("pool.open_slots", slot_stats["open"], tags={"pool_name": pool_name})
            Stats.gauge("pool.queued_slots", slot_stats["queued"], tags={"pool_name": pool_name})
            Stats.gauge("pool.running_slots", slot_stats["running"], tags={"pool_name": pool_name})
            Stats.gauge("pool.deferred_slots", slot_stats["deferred"], tags={"pool_name": pool_name})

    @provide_session
    def adopt_or_reset_orphaned_tasks(self, session: Session = NEW_SESSION) -> int:
        """
        Reset any TaskInstance in QUEUED or SCHEDULED state if its SchedulerJob is no longer running.

        :return: the number of TIs reset
        """
        self.log.info("Resetting orphaned tasks for active dag runs")
        timeout = conf.getint("scheduler", "scheduler_health_check_threshold")

        for attempt in run_with_db_retries(logger=self.log):
            with attempt:
                self.log.debug(
                    "Running SchedulerJob.adopt_or_reset_orphaned_tasks with retries. Try %d of %d",
                    attempt.retry_state.attempt_number,
                    MAX_DB_RETRIES,
                )
                self.log.debug("Calling SchedulerJob.adopt_or_reset_orphaned_tasks method")
                try:
                    num_failed = session.execute(
                        update(Job)
                        .where(
                            Job.job_type == "SchedulerJob",
                            Job.state == JobState.RUNNING,
                            Job.latest_heartbeat < (timezone.utcnow() - timedelta(seconds=timeout)),
                        )
                        .values(state=JobState.FAILED)
                    ).rowcount

                    if num_failed:
                        self.log.info("Marked %d SchedulerJob instances as failed", num_failed)
                        Stats.incr(self.__class__.__name__.lower() + "_end", num_failed)

                    resettable_states = [TaskInstanceState.QUEUED, TaskInstanceState.RUNNING]
                    query = (
                        select(TI)
                        .where(TI.state.in_(resettable_states))
                        # outerjoin is because we didn't use to have queued_by_job
                        # set, so we need to pick up anything pre upgrade. This (and the
                        # "or queued_by_job_id IS NONE") can go as soon as scheduler HA is
                        # released.
                        .outerjoin(TI.queued_by_job)
                        .where(or_(TI.queued_by_job_id.is_(None), Job.state != JobState.RUNNING))
                        .join(TI.dag_run)
                        .where(
                            DagRun.run_type != DagRunType.BACKFILL_JOB,
                            DagRun.state == DagRunState.RUNNING,
                        )
                        .options(load_only(TI.dag_id, TI.task_id, TI.run_id))
                    )

                    # Lock these rows, so that another scheduler can't try and adopt these too
                    tis_to_reset_or_adopt = with_row_locks(
                        query, of=TI, session=session, **skip_locked(session=session)
                    )
                    tis_to_reset_or_adopt = session.scalars(tis_to_reset_or_adopt).all()
                    to_reset = self.job.executor.try_adopt_task_instances(tis_to_reset_or_adopt)

                    reset_tis_message = []
                    for ti in to_reset:
                        reset_tis_message.append(repr(ti))
                        ti.state = None
                        ti.queued_by_job_id = None

                    for ti in set(tis_to_reset_or_adopt) - set(to_reset):
                        ti.queued_by_job_id = self.job.id

                    Stats.incr("scheduler.orphaned_tasks.cleared", len(to_reset))
                    Stats.incr("scheduler.orphaned_tasks.adopted", len(tis_to_reset_or_adopt) - len(to_reset))

                    if to_reset:
                        task_instance_str = "\n\t".join(reset_tis_message)
                        self.log.info(
                            "Reset the following %s orphaned TaskInstances:\n\t%s",
                            len(to_reset),
                            task_instance_str,
                        )

                    # Issue SQL/finish "Unit of Work", but let @provide_session
                    # commit (or if passed a session, let caller decide when to commit
                    session.flush()
                except OperationalError:
                    session.rollback()
                    raise

        return len(to_reset)

    @provide_session
    def check_trigger_timeouts(self, session: Session = NEW_SESSION) -> None:
        """Mark any "deferred" task as failed if the trigger or execution timeout has passed."""
        num_timed_out_tasks = session.execute(
            update(TI)
            .where(
                TI.state == TaskInstanceState.DEFERRED,
                TI.trigger_timeout < timezone.utcnow(),
            )
            .values(
                state=TaskInstanceState.SCHEDULED,
                next_method="__fail__",
                next_kwargs={"error": "Trigger/execution timeout"},
                trigger_id=None,
            )
        ).rowcount
        if num_timed_out_tasks:
            self.log.info("Timed out %i deferred tasks without fired triggers", num_timed_out_tasks)

    def _find_zombies(self) -> None:
        """
        Find zombie task instances and create a TaskCallbackRequest to be handled by the DAG processor.

        Zombie instances are tasks haven't heartbeated for too long or have a no-longer-running LocalTaskJob.
        """
        from airflow.jobs.job import Job

        self.log.debug("Finding 'running' jobs without a recent heartbeat")
        limit_dttm = timezone.utcnow() - timedelta(seconds=self._zombie_threshold_secs)

        with create_session() as session:
            zombies: list[tuple[TI, str, str]] = (
                session.execute(
                    select(TI, DM.fileloc, DM.processor_subdir)
                    .with_hint(TI, "USE INDEX (ti_state)", dialect_name="mysql")
                    .join(Job, TI.job_id == Job.id)
                    .join(DM, TI.dag_id == DM.dag_id)
                    .where(TI.state == TaskInstanceState.RUNNING)
                    .where(
                        or_(
                            Job.state != JobState.RUNNING,
                            Job.latest_heartbeat < limit_dttm,
                        )
                    )
                    .where(Job.job_type == "LocalTaskJob")
                    .where(TI.queued_by_job_id == self.job.id)
                )
                .unique()
                .all()
            )

        if zombies:
            self.log.warning("Failing (%s) jobs without heartbeat after %s", len(zombies), limit_dttm)

        for ti, file_loc, processor_subdir in zombies:
            zombie_message_details = self._generate_zombie_message_details(ti)
            request = TaskCallbackRequest(
                full_filepath=file_loc,
                processor_subdir=processor_subdir,
                simple_task_instance=SimpleTaskInstance.from_ti(ti),
                msg=str(zombie_message_details),
            )
            self.log.error("Detected zombie job: %s", request)
            self.job.executor.send_callback(request)
            Stats.incr("zombies_killed", tags={"dag_id": ti.dag_id, "task_id": ti.task_id})

    @staticmethod
    def _generate_zombie_message_details(ti: TI) -> dict[str, Any]:
        zombie_message_details = {
            "DAG Id": ti.dag_id,
            "Task Id": ti.task_id,
            "Run Id": ti.run_id,
        }

        if ti.map_index != -1:
            zombie_message_details["Map Index"] = ti.map_index
        if ti.hostname:
            zombie_message_details["Hostname"] = ti.hostname
        if ti.external_executor_id:
            zombie_message_details["External Executor Id"] = ti.external_executor_id

        return zombie_message_details

    @provide_session
    def _cleanup_stale_dags(self, session: Session = NEW_SESSION) -> None:
        """
        Find all dags that were not updated by Dag Processor recently and mark them as inactive.

        In case one of DagProcessors is stopped (in case there are multiple of them
        for different dag folders), it's dags are never marked as inactive.
        Also remove dags from SerializedDag table.
        Executed on schedule only if [scheduler]standalone_dag_processor is True.
        """
        self.log.debug("Checking dags not parsed within last %s seconds.", self._dag_stale_not_seen_duration)
        limit_lpt = timezone.utcnow() - timedelta(seconds=self._dag_stale_not_seen_duration)
        stale_dags = session.scalars(
            select(DagModel).where(DagModel.is_active, DagModel.last_parsed_time < limit_lpt)
        ).all()
        if not stale_dags:
            self.log.debug("Not stale dags found.")
            return

        self.log.info("Found (%d) stales dags not parsed after %s.", len(stale_dags), limit_lpt)
        for dag in stale_dags:
            dag.is_active = False
            SerializedDagModel.remove_dag(dag_id=dag.dag_id, session=session)
        session.flush()

    def _set_orphaned(self, dataset: DatasetModel) -> int:
        self.log.info("Orphaning unreferenced dataset '%s'", dataset.uri)
        dataset.is_orphaned = expression.true()
        return 1

    @provide_session
    def _orphan_unreferenced_datasets(self, session: Session = NEW_SESSION) -> None:
        """
        Detect orphaned datasets and set is_orphaned flag to True.

        An orphaned dataset is no longer referenced in any DAG schedule parameters or task outlets.
        """
        orphaned_dataset_query = session.scalars(
            select(DatasetModel)
            .join(
                DagScheduleDatasetReference,
                isouter=True,
            )
            .join(
                TaskOutletDatasetReference,
                isouter=True,
            )
            # MSSQL doesn't like it when we select a column that we haven't grouped by. All other DBs let us
            # group by id and select all columns.
            .group_by(DatasetModel if session.get_bind().dialect.name == "mssql" else DatasetModel.id)
            .having(
                and_(
                    func.count(DagScheduleDatasetReference.dag_id) == 0,
                    func.count(TaskOutletDatasetReference.dag_id) == 0,
                )
            )
        )

        updated_count = sum(self._set_orphaned(dataset) for dataset in orphaned_dataset_query)
        Stats.gauge("dataset.orphaned", updated_count)
