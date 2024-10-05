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
import multiprocessing
import operator
import os
import signal
import sys
import time
from collections import Counter, defaultdict, deque
from datetime import timedelta
from functools import lru_cache, partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Collection, Iterable, Iterator

from sqlalchemy import and_, delete, exists, func, not_, or_, select, text, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import lazyload, load_only, make_transient, selectinload
from sqlalchemy.sql import expression

from airflow import settings
from airflow.callbacks.callback_requests import DagCallbackRequest, TaskCallbackRequest
from airflow.callbacks.pipe_callback_sink import PipeCallbackSink
from airflow.configuration import conf
from airflow.exceptions import UnknownExecutorException
from airflow.executors.executor_loader import ExecutorLoader
from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.jobs.job import Job, perform_heartbeat
from airflow.models import Log
from airflow.models.asset import (
    AssetActive,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    DagScheduleAssetReference,
    TaskOutletAssetReference,
)
from airflow.models.backfill import Backfill
from airflow.models.dag import DAG, DagModel
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.dagwarning import DagWarning, DagWarningType
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstance
from airflow.stats import Stats
from airflow.ti_deps.dependencies_states import EXECUTION_STATES
from airflow.timetables.simple import AssetTriggeredTimetable
from airflow.traces import utils as trace_utils
from airflow.traces.tracer import Trace, add_span
from airflow.utils import timezone
from airflow.utils.dates import datetime_to_nano
from airflow.utils.event_scheduler import EventScheduler
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.retries import MAX_DB_RETRIES, retry_db_transaction, run_with_db_retries
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.sqlalchemy import (
    is_lock_not_available_error,
    prohibit_commit,
    tuple_in_condition,
    with_row_locks,
)
from airflow.utils.state import DagRunState, JobState, State, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    import logging
    from datetime import datetime
    from types import FrameType

    from sqlalchemy.orm import Query, Session

    from airflow.dag_processing.manager import DagFileProcessorAgent
    from airflow.executors.base_executor import BaseExecutor
    from airflow.executors.executor_utils import ExecutorName
    from airflow.models.taskinstance import TaskInstanceKey
    from airflow.utils.sqlalchemy import (
        CommitProhibitorGuard,
    )

TI = TaskInstance
DR = DagRun
DM = DagModel


class ConcurrencyMap:
    """
    Dataclass to represent concurrency maps.

    It contains a map from (dag_id, task_id) to # of task instances, a map from (dag_id, task_id)
    to # of task instances in the given state list and a map from (dag_id, run_id, task_id)
    to # of task instances in the given state list in each DAG run.
    """

    def __init__(self):
        self.dag_run_active_tasks_map: Counter[tuple[str, str]] = Counter()
        self.task_concurrency_map: Counter[tuple[str, str]] = Counter()
        self.task_dagrun_concurrency_map: Counter[tuple[str, str, str]] = Counter()

    def load(self, session: Session) -> None:
        self.dag_run_active_tasks_map.clear()
        self.task_concurrency_map.clear()
        self.task_dagrun_concurrency_map.clear()
        query = session.execute(
            select(TI.dag_id, TI.task_id, TI.run_id, func.count("*"))
            .where(TI.state.in_(EXECUTION_STATES))
            .group_by(TI.task_id, TI.run_id, TI.dag_id)
        )
        for dag_id, task_id, run_id, c in query:
            self.dag_run_active_tasks_map[dag_id, run_id] += c
            self.task_concurrency_map[(dag_id, task_id)] += c
            self.task_dagrun_concurrency_map[(dag_id, run_id, task_id)] += c


def _is_parent_process() -> bool:
    """
    Whether this is a parent process.

    Return True if the current process is the parent process.
    False if the current process is a child process started by multiprocessing.
    """
    return multiprocessing.current_process().name == "MainProcess"


class SchedulerJobRunner(BaseJobRunner, LoggingMixin):
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

    def __init__(
        self,
        job: Job,
        subdir: str = settings.DAGS_FOLDER,
        num_runs: int = conf.getint("scheduler", "num_runs"),
        num_times_parse_dags: int = -1,
        scheduler_idle_sleep_time: float = conf.getfloat("scheduler", "scheduler_idle_sleep_time"),
        do_pickle: bool = False,
        log: logging.Logger | None = None,
    ):
        super().__init__(job)
        self.subdir = subdir
        self.num_runs = num_runs
        # In specific tests, we want to stop the parse loop after the _files_ have been parsed a certain
        # number of times. This is only to support testing, and isn't something a user is likely to want to
        # configure -- they'll want num_runs
        self.num_times_parse_dags = num_times_parse_dags
        self._scheduler_idle_sleep_time = scheduler_idle_sleep_time
        # How many seconds do we wait for tasks to heartbeat before mark them as zombies.
        self._zombie_threshold_secs = conf.getint("scheduler", "scheduler_zombie_task_threshold")
        self._standalone_dag_processor = conf.getboolean("scheduler", "standalone_dag_processor")
        self._dag_stale_not_seen_duration = conf.getint("scheduler", "dag_stale_not_seen_duration")
        self._task_queued_timeout = conf.getfloat("scheduler", "task_queued_timeout")

        self.do_pickle = do_pickle

        self._enable_tracemalloc = conf.getboolean("scheduler", "enable_tracemalloc")
        if self._enable_tracemalloc:
            import tracemalloc

            tracemalloc.start()

        if log:
            self._log = log

        # Check what SQL backend we use
        sql_conn: str = conf.get_mandatory_value("database", "sql_alchemy_conn").lower()
        self.using_sqlite = sql_conn.startswith("sqlite")
        # Dag Processor agent - not used in Dag Processor standalone mode.
        self.processor_agent: DagFileProcessorAgent | None = None

        self.dagbag = DagBag(dag_folder=self.subdir, read_dags_from_db=True, load_op_links=False)

    @provide_session
    def heartbeat_callback(self, session: Session = NEW_SESSION) -> None:
        Stats.incr("scheduler_heartbeat", 1, 1)

    def register_signals(self) -> None:
        """Register signals that stop child processes."""
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)
        signal.signal(signal.SIGUSR2, self._debug_dump)

        if self._enable_tracemalloc:
            signal.signal(signal.SIGUSR1, self._log_memory_usage)

    def _exit_gracefully(self, signum: int, frame: FrameType | None) -> None:
        """Clean up processor_agent to avoid leaving orphan processes."""
        if not _is_parent_process():
            # Only the parent process should perform the cleanup.
            return

        if self._enable_tracemalloc:
            import tracemalloc

            tracemalloc.stop()

        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        if self.processor_agent:
            self.processor_agent.end()
        sys.exit(os.EX_OK)

    def _log_memory_usage(self, signum: int, frame: FrameType | None) -> None:
        import tracemalloc

        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")
        n = 10
        self.log.error(
            "scheduler memory usgae:\n Top %d\n %s",
            n,
            "\n\t".join(map(str, top_stats[:n])),
        )

    def _debug_dump(self, signum: int, frame: FrameType | None) -> None:
        if not _is_parent_process():
            # Only the parent process should perform the debug dump.
            return

        try:
            sig_name = signal.Signals(signum).name
        except Exception:
            sig_name = str(signum)

        self.log.info("%s\n%s received, printing debug\n%s", "-" * 80, sig_name, "-" * 80)

        for executor in self.job.executors:
            self.log.info("Debug dump for the executor %s", executor)
            executor.debug_dump()
            self.log.info("-" * 80)

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
        concurrency_map = ConcurrencyMap()
        concurrency_map.load(session=session)

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

            query = (
                select(TI)
                .with_hint(TI, "USE INDEX (ti_state)", dialect_name="mysql")
                .join(TI.dag_run)
                .where(DR.state == DagRunState.RUNNING)
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
                query = with_row_locks(query, of=TI, session=session, skip_locked=True)
                task_instances_to_examine: list[TI] = session.scalars(query).all()

                timer.stop(send=True)
            except OperationalError as e:
                timer.stop(send=False)
                raise e

            # TODO[HA]: This was wrong before anyway, as it only looked at a sub-set of dags, not everything.
            # Stats.gauge('scheduler.tasks.pending', len(task_instances_to_examine))

            if not task_instances_to_examine:
                self.log.debug("No tasks to consider for execution.")
                break

            # Put one task instance on each line
            task_instance_str = "\n".join(f"\t{x!r}" for x in task_instances_to_examine)
            self.log.info("%s tasks up for execution:\n%s", len(task_instances_to_examine), task_instance_str)

            executor_slots_available: dict[ExecutorName, int] = {}
            # First get a mapping of executor names to slots they have available
            for executor in self.job.executors:
                if TYPE_CHECKING:
                    # All executors should have a name if they are initted from the executor_loader.
                    # But we need to check for None to make mypy happy.
                    assert executor.name
                executor_slots_available[executor.name] = executor.slots_available

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
                dag_run_key = (dag_id, task_instance.run_id)
                current_active_tasks_per_dag_run = concurrency_map.dag_run_active_tasks_map[dag_run_key]
                dag_max_active_tasks = task_instance.dag_model.max_active_tasks
                self.log.info(
                    "DAG %s has %s/%s running and queued tasks",
                    dag_id,
                    current_active_tasks_per_dag_run,
                    dag_max_active_tasks,
                )
                if current_active_tasks_per_dag_run >= dag_max_active_tasks:
                    self.log.info(
                        "Not executing %s since the number of tasks running or queued "
                        "from DAG %s is >= to the DAG's max_active_tasks limit of %s",
                        task_instance,
                        dag_id,
                        dag_max_active_tasks,
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

                if executor_obj := self._try_to_load_executor(task_instance.executor):
                    if TYPE_CHECKING:
                        # All executors should have a name if they are initted from the executor_loader.
                        # But we need to check for None to make mypy happy.
                        assert executor_obj.name
                    if executor_slots_available[executor_obj.name] <= 0:
                        self.log.debug(
                            "Not scheduling %s since its executor %s does not currently have any more "
                            "available slots"
                        )
                        starved_tasks.add((task_instance.dag_id, task_instance.task_id))
                        continue
                    else:
                        executor_slots_available[executor_obj.name] -= 1
                else:
                    # This is a defensive guard for if we happen to have a task who's executor cannot be
                    # found. The check in the dag parser should make this not realistically possible but the
                    # loader can fail if some direct DB modification has happened or another as yet unknown
                    # edge case. _try_to_load_executor will log an error message explaining the executor
                    # cannot be found.
                    starved_tasks.add((task_instance.dag_id, task_instance.task_id))
                    continue

                executable_tis.append(task_instance)
                open_slots -= task_instance.pool_slots
                concurrency_map.dag_run_active_tasks_map[dag_run_key] += 1
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

        if executable_tis:
            task_instance_str = "\n".join(f"\t{x!r}" for x in executable_tis)
            self.log.info("Setting the following tasks to queued state:\n%s", task_instance_str)

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

    def _enqueue_task_instances_with_queued_state(
        self, task_instances: list[TI], executor: BaseExecutor, session: Session
    ) -> None:
        """
        Enqueue task_instances which should have been set to queued with the executor.

        :param task_instances: TaskInstances to enqueue
        :param executor: The executor to enqueue tasks for
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
            self.log.info(
                "Sending %s to %s with priority %s and queue %s", ti.key, executor.name, priority, queue
            )

            executor.queue_command(
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

        HA note: This function is a "critical section" meaning that only a single scheduler process can
        execute this function at the same time. This is achieved by doing
        ``SELECT ... from pool FOR UPDATE``. For DBs that support NOWAIT, a "blocked" scheduler will skip
        this and continue on with other tasks (creating new DAG runs, progressing TIs from None to SCHEDULED
        etc.); DBs that don't support this (such as MariaDB or MySQL 5.x) the other schedulers will wait for
        the lock before continuing.

        :param session:
        :return: Number of task instance with state changed.
        """
        # The user can either request a certain number of tis to schedule per main scheduler loop (default
        # is non-zero). If that value has been set to zero, that means use the value of core.parallelism (or
        # however many free slots are left). core.parallelism represents the max number of running TIs per
        # scheduler. Historically this value was stored in the executor, who's job it was to control/enforce
        # it. However, with multiple executors, any of which can run up to core.parallelism TIs individually,
        # we need to make sure in the scheduler now that we don't schedule more than core.parallelism totally
        # across all executors.
        num_occupied_slots = sum([executor.slots_occupied for executor in self.job.executors])
        parallelism = conf.getint("core", "parallelism")
        # Parallelism configured to 0 means infinite currently running tasks
        if parallelism == 0:
            parallelism = sys.maxsize
        if self.job.max_tis_per_query == 0:
            max_tis = parallelism - num_occupied_slots
        else:
            max_tis = min(self.job.max_tis_per_query, parallelism - num_occupied_slots)
        if max_tis <= 0:
            self.log.debug("max_tis query size is less than or equal to zero. No query will be performed!")
            return 0

        queued_tis = self._executable_task_instances_to_queued(max_tis, session=session)

        # Sort queued TIs to there respective executor
        executor_to_queued_tis = self._executor_to_tis(queued_tis)
        for executor, queued_tis_per_executor in executor_to_queued_tis.items():
            self.log.info(
                "Trying to enqueue tasks: %s for executor: %s",
                queued_tis_per_executor,
                executor,
            )

            self._enqueue_task_instances_with_queued_state(queued_tis_per_executor, executor, session=session)

        return len(queued_tis)

    @staticmethod
    def _process_task_event_logs(log_records: deque[Log], session: Session):
        objects = (log_records.popleft() for _ in range(len(log_records)))
        session.bulk_save_objects(objects=objects, preserve_order=False)

    def _process_executor_events(self, executor: BaseExecutor, session: Session) -> int:
        if not self._standalone_dag_processor and not self.processor_agent:
            raise ValueError("Processor agent is not started.")

        return SchedulerJobRunner.process_executor_events(
            executor=executor, dag_bag=self.dagbag, job_id=self.job.id, session=session
        )

    @classmethod
    def process_executor_events(
        cls, executor: BaseExecutor, dag_bag: DagBag, job_id: str | None, session: Session
    ) -> int:
        """
        Respond to executor events.

        This is a classmethod because this is also used in `dag.test()`.
        `dag.test` execute DAGs with no scheduler, therefore it needs to handle the events pushed by the
        executors as well.
        """
        ti_primary_key_to_try_number_map: dict[tuple[str, str, str, int], int] = {}
        event_buffer = executor.get_event_buffer()
        tis_with_right_state: list[TaskInstanceKey] = []

        # Report execution
        for ti_key, (state, _) in event_buffer.items():
            # We create map (dag_id, task_id, execution_date) -> in-memory try_number
            ti_primary_key_to_try_number_map[ti_key.primary] = ti_key.try_number

            cls.logger().info("Received executor event with state %s for task instance %s", state, ti_key)
            if state in (
                TaskInstanceState.FAILED,
                TaskInstanceState.SUCCESS,
                TaskInstanceState.QUEUED,
                TaskInstanceState.RUNNING,
            ):
                tis_with_right_state.append(ti_key)

        # Return if no finished tasks
        if not tis_with_right_state:
            return len(event_buffer)

        # Check state of finished tasks
        filter_for_tis = TI.filter_for_tis(tis_with_right_state)
        query = select(TI).where(filter_for_tis).options(selectinload(TI.dag_model))
        # row lock this entire set of taskinstances to make sure the scheduler doesn't fail when we have
        # multi-schedulers
        tis_query: Query = with_row_locks(query, of=TI, session=session, skip_locked=True)
        tis: Iterator[TI] = session.scalars(tis_query)
        for ti in tis:
            try_number = ti_primary_key_to_try_number_map[ti.key.primary]
            buffer_key = ti.key.with_try_number(try_number)
            state, info = event_buffer.pop(buffer_key)

            if state in (TaskInstanceState.QUEUED, TaskInstanceState.RUNNING):
                ti.external_executor_id = info
                cls.logger().info("Setting external_id for %s to %s", ti, info)
                continue

            msg = (
                "TaskInstance Finished: dag_id=%s, task_id=%s, run_id=%s, map_index=%s, "
                "run_start_date=%s, run_end_date=%s, "
                "run_duration=%s, state=%s, executor=%s, executor_state=%s, try_number=%s, max_tries=%s, "
                "job_id=%s, pool=%s, queue=%s, priority_weight=%d, operator=%s, queued_dttm=%s, "
                "queued_by_job_id=%s, pid=%s"
            )
            cls.logger().info(
                msg,
                ti.dag_id,
                ti.task_id,
                ti.run_id,
                ti.map_index,
                ti.start_date,
                ti.end_date,
                ti.duration,
                ti.state,
                executor,
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

            with Trace.start_span_from_taskinstance(ti=ti) as span:
                span.set_attribute("category", "scheduler")
                span.set_attribute("task_id", ti.task_id)
                span.set_attribute("dag_id", ti.dag_id)
                span.set_attribute("state", ti.state)
                if ti.state == TaskInstanceState.FAILED:
                    span.set_attribute("error", True)
                span.set_attribute("start_date", str(ti.start_date))
                span.set_attribute("end_date", str(ti.end_date))
                span.set_attribute("duration", ti.duration)
                span.set_attribute("executor_config", str(ti.executor_config))
                span.set_attribute("execution_date", str(ti.execution_date))
                span.set_attribute("hostname", ti.hostname)
                span.set_attribute("log_url", ti.log_url)
                span.set_attribute("operator", str(ti.operator))
                span.set_attribute("try_number", ti.try_number)
                span.set_attribute("executor_state", state)
                span.set_attribute("job_id", ti.job_id)
                span.set_attribute("pool", ti.pool)
                span.set_attribute("queue", ti.queue)
                span.set_attribute("priority_weight", ti.priority_weight)
                span.set_attribute("queued_dttm", str(ti.queued_dttm))
                span.set_attribute("queued_by_job_id", ti.queued_by_job_id)
                span.set_attribute("pid", ti.pid)
                if span.is_recording():
                    span.add_event(name="queued", timestamp=datetime_to_nano(ti.queued_dttm))
                    span.add_event(name="started", timestamp=datetime_to_nano(ti.start_date))
                    span.add_event(name="ended", timestamp=datetime_to_nano(ti.end_date))
                if conf.has_option("traces", "otel_task_log_event") and conf.getboolean(
                    "traces", "otel_task_log_event"
                ):
                    from airflow.utils.log.log_reader import TaskLogReader

                    task_log_reader = TaskLogReader()
                    if task_log_reader.supports_read:
                        metadata: dict[str, Any] = {}
                        logs, metadata = task_log_reader.read_log_chunks(ti, ti.try_number, metadata)
                        if ti.hostname in dict(logs[0]):
                            message = str(dict(logs[0])[ti.hostname]).replace("\\n", "\n")
                            while metadata["end_of_log"] is False:
                                logs, metadata = task_log_reader.read_log_chunks(
                                    ti, ti.try_number - 1, metadata
                                )
                                if ti.hostname in dict(logs[0]):
                                    message = message + str(dict(logs[0])[ti.hostname]).replace("\\n", "\n")
                            if span.is_recording():
                                span.add_event(
                                    name="task_log",
                                    attributes={
                                        "message": message,
                                        "metadata": str(metadata),
                                    },
                                )

            # There are two scenarios why the same TI with the same try_number is queued
            # after executor is finished with it:
            # 1) the TI was killed externally and it had no time to mark itself failed
            # - in this case we should mark it as failed here.
            # 2) the TI has been requeued after getting deferred - in this case either our executor has it
            # or the TI is queued by another job. Either ways we should not fail it.

            # All of this could also happen if the state is "running",
            # but that is handled by the zombie detection.

            ti_queued = ti.try_number == buffer_key.try_number and ti.state in (
                TaskInstanceState.SCHEDULED,
                TaskInstanceState.QUEUED,
                TaskInstanceState.RUNNING,
            )
            ti_requeued = (
                ti.queued_by_job_id != job_id  # Another scheduler has queued this task again
                or executor.has_task(ti)  # This scheduler has this task already
            )

            if ti_queued and not ti_requeued:
                Stats.incr(
                    "scheduler.tasks.killed_externally",
                    tags={"dag_id": ti.dag_id, "task_id": ti.task_id},
                )
                msg = (
                    "Executor %s reported that the task instance %s finished with state %s, but the task instance's state attribute is %s. "  # noqa: RUF100, UP031, flynt
                    "Learn more: https://airflow.apache.org/docs/apache-airflow/stable/troubleshooting.html#task-state-changed-externally"
                    % (executor, ti, state, ti.state)
                )
                if info is not None:
                    msg += " Extra info: %s" % info  # noqa: RUF100, UP031, flynt
                cls.logger().error(msg)
                session.add(Log(event="state mismatch", extra=msg, task_instance=ti.key))

                # Get task from the Serialized DAG
                try:
                    dag = dag_bag.get_dag(ti.dag_id)
                    task = dag.get_task(ti.task_id)
                except Exception:
                    cls.logger().exception("Marking task instance %s as %s", ti, state)
                    ti.set_state(state)
                    continue
                ti.task = task
                if task.on_retry_callback or task.on_failure_callback:
                    request = TaskCallbackRequest(
                        full_filepath=ti.dag_model.fileloc,
                        simple_task_instance=SimpleTaskInstance.from_ti(ti),
                        msg=msg,
                        processor_subdir=ti.dag_model.processor_subdir,
                    )
                    executor.send_callback(request)
                else:
                    ti.handle_failure(error=msg, session=session)

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
        if not self._standalone_dag_processor and not self.processor_agent:
            self.processor_agent = DagFileProcessorAgent(
                dag_directory=Path(self.subdir),
                max_runs=self.num_times_parse_dags,
                processor_timeout=processor_timeout,
                dag_ids=[],
                pickle_dags=pickle_dags,
                async_mode=async_mode,
            )

        try:
            callback_sink: PipeCallbackSink | DatabaseCallbackSink

            if self.processor_agent:
                self.log.debug("Using PipeCallbackSink as callback sink.")
                callback_sink = PipeCallbackSink(get_sink_pipe=self.processor_agent.get_callbacks_pipe)
            else:
                from airflow.callbacks.database_callback_sink import DatabaseCallbackSink

                self.log.debug("Using DatabaseCallbackSink as callback sink.")
                callback_sink = DatabaseCallbackSink()

            for executor in self.job.executors:
                executor.job_id = self.job.id
                executor.callback_sink = callback_sink
                executor.start()

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
            for executor in self.job.executors:
                try:
                    executor.end()
                except Exception:
                    self.log.exception("Exception when executing Executor.end on %s", executor)
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
                )
                .having(DagRun.last_scheduling_decision <= func.max(TI.updated_at))
                .group_by(DagRun)
            )
            for dag_run in paused_runs:
                dag = self.dagbag.get_dag(dag_run.dag_id, session=session)
                if dag is not None:
                    dag_run.dag = dag
                    _, callback_to_run = dag_run.update_state(execute_callbacks=False, session=session)
                    if callback_to_run:
                        self._send_dag_callbacks_to_processor(dag, callback_to_run)
        except Exception as e:  # should not fail the scheduler
            self.log.exception("Failed to update dag run state for paused dags due to %s", e)

    def _run_scheduler_loop(self) -> None:
        """
        Harvest DAG parsing results, queue tasks, and perform executor heartbeat; the actual scheduler loop.

        The main steps in the loop are:
            #. Harvest DAG parsing results through DagFileProcessorAgent
            #. Find and queue executable tasks
                #. Change task instance state in DB
                #. Queue tasks in executor
            #. Heartbeat executor
                #. Execute queued tasks in executor asynchronously
                #. Sync on the states of running tasks
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
            30,
            self._mark_backfills_complete,
        )

        timers.call_regular_interval(
            conf.getfloat("scheduler", "pool_metrics_interval", fallback=5.0),
            self._emit_pool_metrics,
        )

        timers.call_regular_interval(
            conf.getfloat("scheduler", "zombie_detection_interval", fallback=10.0),
            self._find_and_purge_zombies,
        )

        timers.call_regular_interval(60.0, self._update_dag_run_state_for_paused_dags)

        timers.call_regular_interval(
            conf.getfloat("scheduler", "task_queued_timeout_check_interval"),
            self._fail_tasks_stuck_in_queued,
        )

        timers.call_regular_interval(
            conf.getfloat("scheduler", "parsing_cleanup_interval"),
            self._update_asset_orphanage,
        )

        if self._standalone_dag_processor:
            timers.call_regular_interval(
                conf.getfloat("scheduler", "parsing_cleanup_interval"),
                self._cleanup_stale_dags,
            )

        for loop_count in itertools.count(start=1):
            with Trace.start_span(
                span_name="scheduler_job_loop", component="SchedulerJobRunner"
            ) as span, Stats.timer("scheduler.scheduler_loop_duration") as timer:
                span.set_attribute("category", "scheduler")
                span.set_attribute("loop_count", loop_count)

                if self.using_sqlite and self.processor_agent:
                    self.processor_agent.run_single_parsing_loop()
                    # For the sqlite case w/ 1 thread, wait until the processor
                    # is finished to avoid concurrent access to the DB.
                    self.log.debug("Waiting for processors to finish since we're using sqlite")
                    self.processor_agent.wait_until_finished()

                with create_session() as session:
                    # This will schedule for as many executors as possible.
                    num_queued_tis = self._do_scheduling(session)

                    # Heartbeat all executors, even if they're not receiving new tasks this loop. It will be
                    # either a no-op, or they will check-in on currently running tasks and send out new
                    # events to be processed below.
                    for executor in self.job.executors:
                        executor.heartbeat()

                    session.expunge_all()
                    num_finished_events = 0
                    for executor in self.job.executors:
                        num_finished_events += self._process_executor_events(
                            executor=executor, session=session
                        )

                for executor in self.job.executors:
                    try:
                        # this is backcompat check if executor does not inherit from BaseExecutor
                        if not hasattr(executor, "_task_event_logs"):
                            continue
                        with create_session() as session:
                            self._process_task_event_logs(executor._task_event_logs, session)
                    except Exception:
                        self.log.exception("Something went wrong when trying to save task event logs.")

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
            if span.is_recording():
                span.add_event(
                    name="Ran scheduling loop",
                    attributes={
                        "duration in seconds": timer.duration,
                    },
                )

            if not is_unit_test and not num_queued_tis and not num_finished_events:
                # If the scheduler is doing things, don't sleep. This means when there is work to do, the
                # scheduler will run "as quick as possible", but when it's stopped, it can sleep, dropping CPU
                # usage when "idle"
                time.sleep(min(self._scheduler_idle_sleep_time, next_event or 0))

            if loop_count >= self.num_runs > 0:
                self.log.info(
                    "Exiting scheduler loop as requested number of runs (%d - got to %d) has been reached",
                    self.num_runs,
                    loop_count,
                )
                if span.is_recording():
                    span.add_event("Exiting scheduler loop as requested number of runs has been reached")
                break
            if self.processor_agent and self.processor_agent.done:
                self.log.info(
                    "Exiting scheduler loop as requested DAG parse count (%d) has been reached after %d"
                    " scheduler loops",
                    self.num_times_parse_dags,
                    loop_count,
                )
                if span.is_recording():
                    span.add_event("Exiting scheduler loop as requested DAG parse count has been reached")
                break

    def _do_scheduling(self, session: Session) -> int:
        """
        Make the main scheduling decisions.

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

            # Bulk fetch the currently active dag runs for the dags we are
            # examining, rather than making one query per DagRun
            dag_runs = DagRun.get_running_dag_runs_to_examine(session=session)

            callback_tuples = self._schedule_all_dag_runs(guard, dag_runs, session)

        # Send the callbacks after we commit to ensure the context is up to date when it gets run
        # cache saves time during scheduling of many dag_runs for same dag
        cached_get_dag: Callable[[str], DAG | None] = lru_cache()(
            partial(self.dagbag.get_dag, session=session)
        )
        for dag_run, callback_to_run in callback_tuples:
            dag = cached_get_dag(dag_run.dag_id)
            if dag:
                # Sending callbacks there as in standalone_dag_processor they are adding to the database,
                # so it must be done outside of prohibit_commit.
                self._send_dag_callbacks_to_processor(dag, callback_to_run)
            else:
                self.log.error("DAG '%s' not found in serialized_dag table", dag_run.dag_id)

        with prohibit_commit(session) as guard:
            # Without this, the session has an invalid view of the DB
            session.expunge_all()
            # END: schedule TIs

            # Attempt to schedule even if some executors are full but not all.
            total_free_executor_slots = sum([executor.slots_available for executor in self.job.executors])
            if total_free_executor_slots <= 0:
                # We know we can't do anything here, so don't even try!
                self.log.debug("All executors are full, skipping critical section")
                num_queued_tis = 0
            else:
                try:
                    timer = Stats.timer("scheduler.critical_section_duration")
                    timer.start()

                    # Find any TIs in state SCHEDULED, try to QUEUE them (send it to the executors)
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
    def _create_dagruns_for_dags(self, guard: CommitProhibitorGuard, session: Session) -> None:
        """Find Dag Models needing DagRuns and Create Dag Runs with retries in case of OperationalError."""
        query, asset_triggered_dag_info = DagModel.dags_needing_dagruns(session)
        all_dags_needing_dag_runs = set(query.all())
        asset_triggered_dags = [
            dag for dag in all_dags_needing_dag_runs if dag.dag_id in asset_triggered_dag_info
        ]
        non_asset_dags = all_dags_needing_dag_runs.difference(asset_triggered_dags)
        self._create_dag_runs(non_asset_dags, session)
        if asset_triggered_dags:
            self._create_dag_runs_asset_triggered(asset_triggered_dags, asset_triggered_dag_info, session)

        # commit the session - Release the write lock on DagModel table.
        guard.commit()
        # END: create dagruns

    @provide_session
    def _mark_backfills_complete(self, session: Session = NEW_SESSION) -> None:
        """Mark completed backfills as completed."""
        self.log.debug("checking for completed backfills.")
        unfinished_states = (DagRunState.RUNNING, DagRunState.QUEUED)
        now = timezone.utcnow()
        # todo: AIP-78 simplify this function to an update statement
        query = select(Backfill).where(
            Backfill.completed_at.is_(None),
            ~exists(
                select(DagRun.id).where(
                    and_(DagRun.backfill_id == Backfill.id, DagRun.state.in_(unfinished_states))
                )
            ),
        )
        backfills = session.scalars(query).all()
        if not backfills:
            return
        self.log.info("marking %s backfills as complete", len(backfills))
        for b in backfills:
            b.completed_at = now

    @add_span
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

        # backfill runs are not created by scheduler and their concurrency is separate
        # so we exclude them here
        dag_ids = (dm.dag_id for dm in dag_models)
        active_runs_of_dags = Counter(
            DagRun.active_runs_of_dags(
                dag_ids=dag_ids,
                exclude_backfill=True,
                session=session,
            )
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
                try:
                    dag.create_dagrun(
                        run_type=DagRunType.SCHEDULED,
                        execution_date=dag_model.next_dagrun,
                        state=DagRunState.QUEUED,
                        data_interval=data_interval,
                        external_trigger=False,
                        session=session,
                        dag_hash=dag_hash,
                        creating_job_id=self.job.id,
                        triggered_by=DagRunTriggeredByType.TIMETABLE,
                    )
                    active_runs_of_dags[dag.dag_id] += 1
                # Exceptions like ValueError, ParamValidationError, etc. are raised by
                # dag.create_dagrun() when dag is misconfigured. The scheduler should not
                # crash due to misconfigured dags. We should log any exception encountered
                # and continue to the next dag.
                except Exception:
                    self.log.exception("Failed creating DagRun for %s", dag.dag_id)
                    continue
            if self._should_update_dag_next_dagruns(
                dag,
                dag_model,
                last_dag_run=None,
                active_non_backfill_runs=active_runs_of_dags[dag.dag_id],
                session=session,
            ):
                dag_model.calculate_dagrun_date_fields(dag, data_interval)
        # TODO[HA]: Should we do a session.flush() so we don't have to keep lots of state/object in
        # memory for larger dags? or expunge_all()

    def _create_dag_runs_asset_triggered(
        self,
        dag_models: Collection[DagModel],
        asset_triggered_dag_info: dict[str, tuple[datetime, datetime]],
        session: Session,
    ) -> None:
        """For DAGs that are triggered by assets, create dag runs."""
        # Bulk Fetch DagRuns with dag_id and execution_date same
        # as DagModel.dag_id and DagModel.next_dagrun
        # This list is used to verify if the DagRun already exist so that we don't attempt to create
        # duplicate dag runs
        exec_dates = {
            dag_id: timezone.coerce_datetime(last_time)
            for dag_id, (_, last_time) in asset_triggered_dag_info.items()
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

            if not isinstance(dag.timetable, AssetTriggeredTimetable):
                self.log.error(
                    "DAG '%s' was asset-scheduled, but didn't have an AssetTriggeredTimetable!",
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
                        DagRun.run_type == DagRunType.ASSET_TRIGGERED,
                    )
                    .order_by(DagRun.execution_date.desc())
                    .limit(1)
                )
                asset_event_filters = [
                    DagScheduleAssetReference.dag_id == dag.dag_id,
                    AssetEvent.timestamp <= exec_date,
                ]
                if previous_dag_run:
                    asset_event_filters.append(AssetEvent.timestamp > previous_dag_run.execution_date)

                asset_events = session.scalars(
                    select(AssetEvent)
                    .join(
                        DagScheduleAssetReference,
                        AssetEvent.asset_id == DagScheduleAssetReference.asset_id,
                    )
                    .where(*asset_event_filters)
                ).all()

                data_interval = dag.timetable.data_interval_for_events(exec_date, asset_events)
                run_id = dag.timetable.generate_run_id(
                    run_type=DagRunType.ASSET_TRIGGERED,
                    logical_date=exec_date,
                    data_interval=data_interval,
                    session=session,
                    events=asset_events,
                )

                dag_run = dag.create_dagrun(
                    run_id=run_id,
                    run_type=DagRunType.ASSET_TRIGGERED,
                    execution_date=exec_date,
                    data_interval=data_interval,
                    state=DagRunState.QUEUED,
                    external_trigger=False,
                    session=session,
                    dag_hash=dag_hash,
                    creating_job_id=self.job.id,
                    triggered_by=DagRunTriggeredByType.ASSET,
                )
                Stats.incr("asset.triggered_dagruns")
                dag_run.consumed_asset_events.extend(asset_events)
                session.execute(
                    delete(AssetDagRunQueue).where(AssetDagRunQueue.target_dag_id == dag_run.dag_id)
                )

    def _should_update_dag_next_dagruns(
        self,
        dag: DAG,
        dag_model: DagModel,
        *,
        last_dag_run: DagRun | None = None,
        active_non_backfill_runs: int | None = None,
        session: Session,
    ) -> bool:
        """Check if the dag's next_dagruns_create_after should be updated."""
        # If last_dag_run is defined, the update was triggered by a scheduling decision in this DAG run.
        # In such case, schedule next only if last_dag_run is finished and was an automated run.
        if last_dag_run and not (
            last_dag_run.state in State.finished_dr_states
            and last_dag_run.run_type in [DagRunType.SCHEDULED, DagRunType.BACKFILL_JOB]
        ):
            return False
        # If the DAG never schedules skip save runtime
        if not dag.timetable.can_be_scheduled:
            return False

        if active_non_backfill_runs is None:
            runs_dict = DagRun.active_runs_of_dags(
                dag_ids=[dag.dag_id],
                exclude_backfill=True,
                session=session,
            )
            active_non_backfill_runs = runs_dict.get(dag.dag_id, 0)

        if active_non_backfill_runs >= dag.max_active_runs:
            self.log.info(
                "DAG %s is at (or above) max_active_runs (%d of %d), not creating any more runs",
                dag_model.dag_id,
                active_non_backfill_runs,
                dag.max_active_runs,
            )
            dag_model.next_dagrun_create_after = None
            return False
        return True

    @add_span
    def _start_queued_dagruns(self, session: Session) -> None:
        """Find DagRuns in queued state and decide moving them to running state."""
        # added all() to save runtime, otherwise query is executed more than once
        dag_runs: Collection[DagRun] = DagRun.get_queued_dag_runs_to_set_running(session).all()

        query = (
            select(
                DagRun.dag_id,
                DagRun.backfill_id,
                func.count(DagRun.id).label("num_running"),
            )
            .where(DagRun.state == DagRunState.RUNNING)
            .group_by(DagRun.dag_id, DagRun.backfill_id)
        )
        active_runs_of_dags = Counter({(dag_id, br_id): num for dag_id, br_id, num in session.execute(query)})

        @add_span
        def _update_state(dag: DAG, dag_run: DagRun):
            span = Trace.get_current_span()
            span.set_attribute("state", str(DagRunState.RUNNING))
            span.set_attribute("run_id", dag_run.run_id)
            span.set_attribute("type", dag_run.run_type)
            span.set_attribute("dag_id", dag_run.dag_id)

            dag_run.state = DagRunState.RUNNING
            dag_run.start_date = timezone.utcnow()
            if dag.timetable.periodic and not dag_run.external_trigger and dag_run.clear_number < 1:
                # TODO: Logically, this should be DagRunInfo.run_after, but the
                # information is not stored on a DagRun, only before the actual
                # execution on DagModel.next_dagrun_create_after. We should add
                # a field on DagRun for this instead of relying on the run
                # always happening immediately after the data interval.
                # We only publish these metrics for scheduled dag runs and only
                # when ``external_trigger`` is *False* and ``clear_number`` is 0.
                expected_start_date = dag.get_run_data_interval(dag_run).end
                schedule_delay = dag_run.start_date - expected_start_date
                # Publish metrics twice with backward compatible name, and then with tags
                Stats.timing(f"dagrun.schedule_delay.{dag.dag_id}", schedule_delay)
                Stats.timing(
                    "dagrun.schedule_delay",
                    schedule_delay,
                    tags={"dag_id": dag.dag_id},
                )
                if span.is_recording():
                    span.add_event(
                        name="schedule_delay",
                        attributes={"dag_id": dag.dag_id, "schedule_delay": str(schedule_delay)},
                    )

        # cache saves time during scheduling of many dag_runs for same dag
        cached_get_dag: Callable[[str], DAG | None] = lru_cache()(
            partial(self.dagbag.get_dag, session=session)
        )

        span = Trace.get_current_span()
        for dag_run in dag_runs:
            dag_id = dag_run.dag_id
            run_id = dag_run.run_id
            backfill_id = dag_run.backfill_id
            backfill = dag_run.backfill
            dag = dag_run.dag = cached_get_dag(dag_id)
            if not dag:
                self.log.error("DAG '%s' not found in serialized_dag table", dag_run.dag_id)
                continue
            active_runs = active_runs_of_dags[(dag_id, backfill_id)]
            if backfill_id is not None:
                if active_runs >= backfill.max_active_runs:
                    # todo: delete all "candidate dag runs" from list for this dag right now
                    self.log.info(
                        "dag cannot be started due to backfill max_active_runs constraint; "
                        "active_runs=%s max_active_runs=%s dag_id=%s run_id=%s",
                        active_runs,
                        backfill.max_active_runs,
                        dag_id,
                        run_id,
                    )
                    continue
            elif dag.max_active_runs:
                if active_runs >= dag.max_active_runs:
                    # todo: delete all candidate dag runs for this dag from list right now
                    self.log.info(
                        "dag cannot be started due to dag max_active_runs constraint; "
                        "active_runs=%s max_active_runs=%s dag_id=%s run_id=%s",
                        active_runs,
                        dag_run.max_active_runs,
                        dag_run.dag_id,
                        dag_run.run_id,
                    )
                    continue
            if span.is_recording():
                span.add_event(
                    name="dag_run",
                    attributes={
                        "run_id": dag_run.run_id,
                        "dag_id": dag_run.dag_id,
                        "conf": str(dag_run.conf),
                    },
                )
            active_runs_of_dags[(dag_run.dag_id, backfill_id)] += 1
            _update_state(dag, dag_run)
            dag_run.notify_dagrun_state_changed()

    @retry_db_transaction
    def _schedule_all_dag_runs(
        self,
        guard: CommitProhibitorGuard,
        dag_runs: Iterable[DagRun],
        session: Session,
    ) -> list[tuple[DagRun, DagCallbackRequest | None]]:
        """Make scheduling decisions for all `dag_runs`."""
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
        trace_id = int(trace_utils.gen_trace_id(dag_run=dag_run, as_int=True))
        span_id = int(trace_utils.gen_dag_span_id(dag_run=dag_run, as_int=True))
        links = [{"trace_id": trace_id, "span_id": span_id}]

        with Trace.start_span(
            span_name="_schedule_dag_run", component="SchedulerJobRunner", links=links
        ) as span:
            span.set_attribute("dag_id", dag_run.dag_id)
            span.set_attribute("run_id", dag_run.run_id)
            span.set_attribute("run_type", dag_run.run_type)
            callback: DagCallbackRequest | None = None

            dag = dag_run.dag = self.dagbag.get_dag(dag_run.dag_id, session=session)
            dag_model = DM.get_dagmodel(dag_run.dag_id, session)

            if not dag or not dag_model:
                self.log.error("Couldn't find DAG %s in DAG bag or database!", dag_run.dag_id)
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
                    # If the DAG is set to kill the task instances on timeout, we call on_kill
                    # otherwise we just skip the task instance
                    task_instance.state = TaskInstanceState.SKIPPED
                    if dag.call_on_kill_on_dagrun_timeout:
                        task = dag.get_task(task_instance.task_id)
                        if hasattr(task, "on_kill"):
                            try:
                                task.on_kill()
                                task_instance.state = TaskInstanceState.FAILED
                            except Exception as e:
                                self.log.error("Error when calling on_kill for task %s: %s", task_instance, e)
                        else:
                            self.log.warning("Task %s does not have on_kill method, skipping", task_instance)
                    session.merge(task_instance)
                session.flush()
                self.log.info("Run %s of %s has timed-out", dag_run.run_id, dag_run.dag_id)

                if self._should_update_dag_next_dagruns(
                    dag, dag_model, last_dag_run=dag_run, session=session
                ):
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
                span.set_attribute("error", True)
                if span.is_recording():
                    span.add_event(
                        name="error",
                        attributes={
                            "message": f"Run {dag_run.run_id} of {dag_run.dag_id} has timed-out",
                            "duration": str(duration),
                        },
                    )
                return callback_to_execute

            if dag_run.execution_date > timezone.utcnow() and not dag.allow_future_exec_dates:
                self.log.error("Execution date is in future: %s", dag_run.execution_date)
                return callback

            if not self._verify_integrity_if_dag_changed(dag_run=dag_run, session=session):
                self.log.warning(
                    "The DAG disappeared before verifying integrity: %s. Skipping.", dag_run.dag_id
                )
                return callback
            # TODO[HA]: Rename update_state -> schedule_dag_run, ?? something else?
            schedulable_tis, callback_to_run = dag_run.update_state(session=session, execute_callbacks=False)

            if self._should_update_dag_next_dagruns(dag, dag_model, last_dag_run=dag_run, session=session):
                dag_model.calculate_dagrun_date_fields(dag, dag.get_run_data_interval(dag_run))
            # This will do one query per dag run. We "could" build up a complex
            # query to update all the TIs across all the execution dates and dag
            # IDs in a single query, but it turns out that can be _very very slow_
            # see #11147/commit ee90807ac for more details
            if span.is_recording():
                span.add_event(
                    name="schedule_tis",
                    attributes={
                        "message": "dag_run scheduling its tis",
                        "schedulable_tis": [_ti.task_id for _ti in schedulable_tis],
                    },
                )
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
        if callback:
            self.job.executor.send_callback(callback)
        else:
            self.log.debug("callback is empty")

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

        for executor, stuck_tis in self._executor_to_tis(tasks_stuck_in_queued).items():
            try:
                cleaned_up_task_instances = set(executor.cleanup_stuck_queued_tasks(tis=stuck_tis))
                for ti in stuck_tis:
                    if repr(ti) in cleaned_up_task_instances:
                        self.log.warning(
                            "Marking task instance %s stuck in queued as failed. "
                            "If the task instance has available retries, it will be retried.",
                            ti,
                        )
                        session.add(
                            Log(
                                event="stuck in queued",
                                task_instance=ti.key,
                                extra=(
                                    "Task will be marked as failed. If the task instance has "
                                    "available retries, it will be retried."
                                ),
                            )
                        )
            except NotImplementedError:
                self.log.debug("Executor doesn't support cleanup of stuck queued tasks. Skipping.")

    @provide_session
    def _emit_pool_metrics(self, session: Session = NEW_SESSION) -> None:
        from airflow.models.pool import Pool

        with Trace.start_span(span_name="emit_pool_metrics", component="SchedulerJobRunner") as span:
            pools = Pool.slots_stats(session=session)
            for pool_name, slot_stats in pools.items():
                Stats.gauge(f"pool.open_slots.{pool_name}", slot_stats["open"])
                Stats.gauge(f"pool.queued_slots.{pool_name}", slot_stats["queued"])
                Stats.gauge(f"pool.running_slots.{pool_name}", slot_stats["running"])
                Stats.gauge(f"pool.deferred_slots.{pool_name}", slot_stats["deferred"])
                Stats.gauge(f"pool.scheduled_slots.{pool_name}", slot_stats["scheduled"])

                # Same metrics with tagging
                Stats.gauge("pool.open_slots", slot_stats["open"], tags={"pool_name": pool_name})
                Stats.gauge("pool.queued_slots", slot_stats["queued"], tags={"pool_name": pool_name})
                Stats.gauge("pool.running_slots", slot_stats["running"], tags={"pool_name": pool_name})
                Stats.gauge("pool.deferred_slots", slot_stats["deferred"], tags={"pool_name": pool_name})
                Stats.gauge("pool.scheduled_slots", slot_stats["scheduled"], tags={"pool_name": pool_name})

                span.set_attribute("category", "scheduler")
                span.set_attribute(f"pool.open_slots.{pool_name}", slot_stats["open"])
                span.set_attribute(f"pool.queued_slots.{pool_name}", slot_stats["queued"])
                span.set_attribute(f"pool.running_slots.{pool_name}", slot_stats["running"])
                span.set_attribute(f"pool.deferred_slots.{pool_name}", slot_stats["deferred"])
                span.set_attribute(f"pool.scheduled_slots.{pool_name}", slot_stats["scheduled"])

    @provide_session
    def adopt_or_reset_orphaned_tasks(self, session: Session = NEW_SESSION) -> int:
        """
        Adopt or reset any TaskInstance in resettable state if its SchedulerJob is no longer running.

        :return: the number of TIs reset
        """
        self.log.info("Adopting or resetting orphaned tasks for active dag runs")
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

                    query = (
                        select(TI)
                        .options(lazyload(TI.dag_run))  # avoids double join to dag_run
                        .where(TI.state.in_(State.adoptable_states))
                        .join(TI.queued_by_job)
                        .where(Job.state.is_distinct_from(JobState.RUNNING))
                        .join(TI.dag_run)
                        .where(DagRun.state == DagRunState.RUNNING)
                        .options(load_only(TI.dag_id, TI.task_id, TI.run_id))
                    )

                    # Lock these rows, so that another scheduler can't try and adopt these too
                    tis_to_adopt_or_reset = with_row_locks(query, of=TI, session=session, skip_locked=True)
                    tis_to_adopt_or_reset = session.scalars(tis_to_adopt_or_reset).all()

                    to_reset: list[TaskInstance] = []
                    exec_to_tis = self._executor_to_tis(tis_to_adopt_or_reset)
                    for executor, tis in exec_to_tis.items():
                        to_reset.extend(executor.try_adopt_task_instances(tis))

                    reset_tis_message = []
                    for ti in to_reset:
                        reset_tis_message.append(repr(ti))
                        ti.state = None
                        ti.queued_by_job_id = None

                    for ti in set(tis_to_adopt_or_reset) - set(to_reset):
                        ti.queued_by_job_id = self.job.id

                    Stats.incr("scheduler.orphaned_tasks.cleared", len(to_reset))
                    Stats.incr("scheduler.orphaned_tasks.adopted", len(tis_to_adopt_or_reset) - len(to_reset))

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
    def check_trigger_timeouts(
        self, max_retries: int = MAX_DB_RETRIES, session: Session = NEW_SESSION
    ) -> None:
        """Mark any "deferred" task as failed if the trigger or execution timeout has passed."""
        for attempt in run_with_db_retries(max_retries, logger=self.log):
            with attempt:
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

    # [START find_and_purge_zombies]
    def _find_and_purge_zombies(self) -> None:
        """
        Find and purge zombie task instances.

        Zombie instances are tasks that failed to heartbeat for too long, or
        have a no-longer-running LocalTaskJob.

        A TaskCallbackRequest is also created for the killed zombie to be
        handled by the DAG processor, and the executor is informed to no longer
        count the zombie as running when it calculates parallelism.
        """
        with create_session() as session:
            if zombies := self._find_zombies(session=session):
                self._purge_zombies(zombies, session=session)

    def _find_zombies(self, *, session: Session) -> list[tuple[TI, str, str]]:
        from airflow.jobs.job import Job

        self.log.debug("Finding 'running' jobs without a recent heartbeat")
        limit_dttm = timezone.utcnow() - timedelta(seconds=self._zombie_threshold_secs)
        zombies = session.execute(
            select(TI, DM.fileloc, DM.processor_subdir)
            .with_hint(TI, "USE INDEX (ti_state)", dialect_name="mysql")
            .join(Job, TI.job_id == Job.id)
            .join(DM, TI.dag_id == DM.dag_id)
            .where(TI.state == TaskInstanceState.RUNNING)
            .where(or_(Job.state != JobState.RUNNING, Job.latest_heartbeat < limit_dttm))
            .where(Job.job_type == "LocalTaskJob")
            .where(TI.queued_by_job_id == self.job.id)
        ).all()
        if zombies:
            self.log.warning("Failing (%s) jobs without heartbeat after %s", len(zombies), limit_dttm)
        return zombies

    def _purge_zombies(self, zombies: list[tuple[TI, str, str]], *, session: Session) -> None:
        for ti, file_loc, processor_subdir in zombies:
            zombie_message_details = self._generate_zombie_message_details(ti)
            request = TaskCallbackRequest(
                full_filepath=file_loc,
                processor_subdir=processor_subdir,
                simple_task_instance=SimpleTaskInstance.from_ti(ti),
                msg=str(zombie_message_details),
            )
            session.add(
                Log(
                    event="heartbeat timeout",
                    task_instance=ti.key,
                    extra=(
                        f"Task did not emit heartbeat within time limit ({self._zombie_threshold_secs} "
                        "seconds) and will be terminated. "
                        "See https://airflow.apache.org/docs/apache-airflow/"
                        "stable/core-concepts/tasks.html#zombie-undead-tasks"
                    ),
                )
            )
            self.log.error(
                "Detected zombie job: %s "
                "(See https://airflow.apache.org/docs/apache-airflow/"
                "stable/core-concepts/tasks.html#zombie-undead-tasks)",
                request,
            )
            self.job.executor.send_callback(request)
            if (executor := self._try_to_load_executor(ti.executor)) is None:
                self.log.warning("Cannot clean up zombie %r with non-existent executor %s", ti, ti.executor)
                continue
            executor.change_state(ti.key, TaskInstanceState.FAILED, remove_running=True)
            Stats.incr("zombies_killed", tags={"dag_id": ti.dag_id, "task_id": ti.task_id})

    # [END find_and_purge_zombies]

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
        for different dag folders), its dags are never marked as inactive.
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

    @provide_session
    def _update_asset_orphanage(self, session: Session = NEW_SESSION) -> None:
        """
        Check assets orphanization and update their active entry.

        An orphaned asset is no longer referenced in any DAG schedule parameters
        or task outlets. Active assets (non-orphaned) have entries in AssetActive
        and must have unique names and URIs.
        """
        # Group assets into orphaned=True and orphaned=False groups.
        orphaned = (
            (func.count(DagScheduleAssetReference.dag_id) + func.count(TaskOutletAssetReference.dag_id)) == 0
        ).label("orphaned")
        asset_reference_query = session.execute(
            select(orphaned, AssetModel)
            .outerjoin(DagScheduleAssetReference)
            .outerjoin(TaskOutletAssetReference)
            .group_by(AssetModel.id)
            .order_by(orphaned)
        )
        asset_orphanation: dict[bool, Collection[AssetModel]] = {
            orphaned: [asset for _, asset in group]
            for orphaned, group in itertools.groupby(asset_reference_query, key=operator.itemgetter(0))
        }
        self._orphan_unreferenced_assets(asset_orphanation.get(True, ()), session=session)
        self._activate_referenced_assets(asset_orphanation.get(False, ()), session=session)

    @staticmethod
    def _orphan_unreferenced_assets(assets: Collection[AssetModel], *, session: Session) -> None:
        if assets:
            session.execute(
                delete(AssetActive).where(
                    tuple_in_condition((AssetActive.name, AssetActive.uri), ((a.name, a.uri) for a in assets))
                )
            )
        Stats.gauge("asset.orphaned", len(assets))

    @staticmethod
    def _activate_referenced_assets(assets: Collection[AssetModel], *, session: Session) -> None:
        if not assets:
            return

        active_assets = set(
            session.execute(
                select(AssetActive.name, AssetActive.uri).where(
                    tuple_in_condition((AssetActive.name, AssetActive.uri), ((a.name, a.uri) for a in assets))
                )
            )
        )

        active_name_to_uri: dict[str, str] = {name: uri for name, uri in active_assets}
        active_uri_to_name: dict[str, str] = {uri: name for name, uri in active_assets}

        def _generate_dag_warnings(offending: AssetModel, attr: str, value: str) -> Iterator[DagWarning]:
            for ref in itertools.chain(offending.consuming_dags, offending.producing_tasks):
                yield DagWarning(
                    dag_id=ref.dag_id,
                    error_type=DagWarningType.ASSET_CONFLICT,
                    message=f"Cannot activate asset {offending}; {attr} is already associated to {value!r}",
                )

        def _activate_assets_generate_warnings() -> Iterator[DagWarning]:
            incoming_name_to_uri: dict[str, str] = {}
            incoming_uri_to_name: dict[str, str] = {}
            for asset in assets:
                if (asset.name, asset.uri) in active_assets:
                    continue
                existing_uri = active_name_to_uri.get(asset.name) or incoming_name_to_uri.get(asset.name)
                if existing_uri is not None and existing_uri != asset.uri:
                    yield from _generate_dag_warnings(asset, "name", existing_uri)
                    continue
                existing_name = active_uri_to_name.get(asset.uri) or incoming_uri_to_name.get(asset.uri)
                if existing_name is not None and existing_name != asset.name:
                    yield from _generate_dag_warnings(asset, "uri", existing_name)
                    continue
                incoming_name_to_uri[asset.name] = asset.uri
                incoming_uri_to_name[asset.uri] = asset.name
                session.add(AssetActive.for_asset(asset))

        warnings_to_have = {w.dag_id: w for w in _activate_assets_generate_warnings()}
        session.execute(
            delete(DagWarning).where(
                DagWarning.warning_type == DagWarningType.ASSET_CONFLICT,
                DagWarning.dag_id.not_in(warnings_to_have),
            )
        )
        existing_warned_dag_ids: set[str] = set(
            session.scalars(
                select(DagWarning.dag_id).where(
                    DagWarning.warning_type == DagWarningType.ASSET_CONFLICT,
                    DagWarning.dag_id.not_in(warnings_to_have),
                )
            )
        )
        for dag_id, warning in warnings_to_have.items():
            if dag_id in existing_warned_dag_ids:
                session.merge(warning)
                continue
            session.add(warning)
            existing_warned_dag_ids.add(warning.dag_id)

    def _executor_to_tis(self, tis: list[TaskInstance]) -> dict[BaseExecutor, list[TaskInstance]]:
        """Organize TIs into lists per their respective executor."""
        _executor_to_tis: defaultdict[BaseExecutor, list[TaskInstance]] = defaultdict(list)
        for ti in tis:
            if executor_obj := self._try_to_load_executor(ti.executor):
                _executor_to_tis[executor_obj].append(ti)

        return _executor_to_tis

    def _try_to_load_executor(self, executor_name: str | None) -> BaseExecutor | None:
        """
        Try to load the given executor.

        In this context, we don't want to fail if the executor does not exist. Catch the exception and
        log to the user.
        """
        try:
            return ExecutorLoader.load_executor(executor_name)
        except UnknownExecutorException:
            # This case should not happen unless some (as of now unknown) edge case occurs or direct DB
            # modification, since the DAG parser will validate the tasks in the DAG and ensure the executor
            # they request is available and if not, disallow the DAG to be scheduled.
            # Keeping this exception handling because this is a critical issue if we do somehow find
            # ourselves here and the user should get some feedback about that.
            self.log.warning("Executor, %s, was not found but a Task was configured to use it", executor_name)
            return None
