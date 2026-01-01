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
from collections.abc import Callable, Collection, Iterable, Iterator
from contextlib import ExitStack
from datetime import date, datetime, timedelta
from functools import lru_cache, partial
from itertools import groupby
from typing import TYPE_CHECKING, Any

from sqlalchemy import and_, delete, desc, exists, func, inspect, or_, select, text, tuple_, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import joinedload, lazyload, load_only, make_transient, selectinload
from sqlalchemy.sql import expression

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.api_fastapi.execution_api.datamodels.taskinstance import DagRun as DRDataModel, TIRunContext
from airflow.callbacks.callback_requests import (
    DagCallbackRequest,
    DagRunContext,
    EmailRequest,
    TaskCallbackRequest,
)
from airflow.configuration import conf
from airflow.dag_processing.bundles.base import BundleUsageTrackingManager
from airflow.exceptions import DagNotFound
from airflow.executors import workloads
from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.jobs.job import Job, JobState, perform_heartbeat
from airflow.models import Deadline, Log
from airflow.models.asset import (
    AssetActive,
    AssetAliasModel,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    AssetPartitionDagRun,
    AssetWatcherModel,
    DagScheduleAssetAliasReference,
    DagScheduleAssetReference,
    TaskInletAssetReference,
    TaskOutletAssetReference,
)
from airflow.models.backfill import Backfill
from airflow.models.callback import Callback
from airflow.models.dag import DagModel, get_next_data_interval, get_run_data_interval
from airflow.models.dag_version import DagVersion
from airflow.models.dagbag import DBDagBag
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.models.dagwarning import DagWarning, DagWarningType
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.models.team import Team
from airflow.models.trigger import TRIGGER_FAIL_REPR, Trigger, TriggerFailureReason
from airflow.observability.stats import Stats
from airflow.observability.trace import DebugTrace, Trace, add_debug_span
from airflow.serialization.definitions.notset import NOTSET
from airflow.ti_deps.dependencies_states import EXECUTION_STATES
from airflow.timetables.simple import AssetTriggeredTimetable
from airflow.utils.dates import datetime_to_nano
from airflow.utils.event_scheduler import EventScheduler
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.retries import MAX_DB_RETRIES, retry_db_transaction, run_with_db_retries
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.span_status import SpanStatus
from airflow.utils.sqlalchemy import (
    get_dialect_name,
    is_lock_not_available_error,
    prohibit_commit,
    with_row_locks,
)
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.thread_safe_dict import ThreadSafeDict
from airflow.utils.types import DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    from types import FrameType

    from pendulum.datetime import DateTime
    from sqlalchemy.orm import Session
    from sqlalchemy.orm.interfaces import LoaderOption

    from airflow._shared.logging.types import Logger
    from airflow.executors.base_executor import BaseExecutor
    from airflow.executors.executor_utils import ExecutorName
    from airflow.models.taskinstance import TaskInstanceKey
    from airflow.serialization.definitions.dag import SerializedDAG
    from airflow.timetables.base import DataInterval
    from airflow.utils.sqlalchemy import CommitProhibitorGuard

TI = TaskInstance
DR = DagRun
DM = DagModel

TASK_STUCK_IN_QUEUED_RESCHEDULE_EVENT = "stuck in queued reschedule"
""":meta private:"""


def _eager_load_dag_run_for_validation() -> tuple[LoaderOption, LoaderOption]:
    """
    Eager-load DagRun relations required for execution API datamodel validation.

    When building TaskCallbackRequest with DRDataModel.model_validate(ti.dag_run),
    the consumed_asset_events collection and nested asset/source_aliases must be
    preloaded to avoid DetachedInstanceError after the session closes.

    Returns a tuple of two load options:
        - Asset loader: TI.dag_run → consumed_asset_events → asset
        - Alias loader: TI.dag_run → consumed_asset_events → source_aliases

    Example usage::

        asset_loader, alias_loader = _eager_load_dag_run_for_validation()
        query = select(TI).options(asset_loader).options(alias_loader)
    """
    # Traverse TI → dag_run → consumed_asset_events once, then branch to asset/aliases
    base = joinedload(TI.dag_run).selectinload(DagRun.consumed_asset_events)
    return (
        base.selectinload(AssetEvent.asset),
        base.selectinload(AssetEvent.source_aliases),
    )


def _get_current_dag(dag_id: str, session: Session) -> SerializedDAG | None:
    serdag = SerializedDagModel.get(dag_id=dag_id, session=session)  # grabs the latest version
    if not serdag:
        return None
    serdag.load_op_links = False
    return serdag.dag


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

    :param num_runs: The number of times to run the scheduling loop. If you
        have a large number of DAG files this could complete before each file
        has been parsed. -1 for unlimited times.
    :param scheduler_idle_sleep_time: The number of seconds to wait between
        polls of running processors
    :param log: override the default Logger
    """

    job_type = "SchedulerJob"

    # For a dagrun span
    #   - key: dag_run.run_id | value: span
    #   - dagrun keys will be prefixed with 'dr:'.
    # For a ti span
    #   - key: ti.id | value: span
    #   - taskinstance keys will be prefixed with 'ti:'.
    active_spans = ThreadSafeDict()

    def __init__(
        self,
        job: Job,
        num_runs: int = conf.getint("scheduler", "num_runs"),
        scheduler_idle_sleep_time: float = conf.getfloat("scheduler", "scheduler_idle_sleep_time"),
        log: Logger | None = None,
    ):
        super().__init__(job)
        self.num_runs = num_runs
        self._scheduler_idle_sleep_time = scheduler_idle_sleep_time
        # How many seconds do we wait for tasks to heartbeat before timeout.
        self._task_instance_heartbeat_timeout_secs = conf.getint(
            "scheduler", "task_instance_heartbeat_timeout"
        )
        self._task_queued_timeout = conf.getfloat("scheduler", "task_queued_timeout")
        self._enable_tracemalloc = conf.getboolean("scheduler", "enable_tracemalloc")

        # this param is intentionally undocumented
        self._num_stuck_queued_retries = conf.getint(
            section="scheduler",
            key="num_stuck_in_queued_retries",
            fallback=2,
        )

        if self._enable_tracemalloc:
            import tracemalloc

            tracemalloc.start()

        if log:
            self._log = log

        self.scheduler_dag_bag = DBDagBag(load_op_links=False)

    @provide_session
    def heartbeat_callback(self, session: Session = NEW_SESSION) -> None:
        Stats.incr("scheduler_heartbeat", 1, 1)

    def register_signals(self) -> ExitStack:
        """Register signals that stop child processes."""
        resetter = ExitStack()
        prev_int = signal.signal(signal.SIGINT, self._exit_gracefully)
        prev_term = signal.signal(signal.SIGTERM, self._exit_gracefully)
        prev_usr2 = signal.signal(signal.SIGUSR2, self._debug_dump)

        resetter.callback(signal.signal, signal.SIGINT, prev_int)
        resetter.callback(signal.signal, signal.SIGTERM, prev_term)
        resetter.callback(signal.signal, signal.SIGUSR2, prev_usr2)

        if self._enable_tracemalloc:
            prev = signal.signal(signal.SIGUSR1, self._log_memory_usage)
            resetter.callback(signal.signal, signal.SIGUSR1, prev)

        return resetter

    def _get_team_names_for_dag_ids(
        self, dag_ids: Collection[str], session: Session
    ) -> dict[str, str | None]:
        """
        Batch query to resolve team names for multiple DAG IDs using the DAG > Bundle > Team relationship chain.

        DAG IDs > DagModel (via dag_id) > DagBundleModel (via bundle_name) > Team

        :param dag_ids: Collection of DAG IDs to resolve team names for
        :param session: Database session for queries
        :return: Dictionary mapping dag_id -> team_name (None if no team found)
        """
        if not dag_ids:
            return {}

        try:
            # Query all team names for the given DAG IDs in a single query
            query_results = session.execute(
                select(DagModel.dag_id, Team.name)
                .join(DagBundleModel.teams)  # Join Team to DagBundleModel via association table
                .join(
                    DagModel, DagModel.bundle_name == DagBundleModel.name
                )  # Join DagBundleModel to DagModel
                .where(DagModel.dag_id.in_(dag_ids))
            ).all()

            # Create mapping from results
            dag_id_to_team_name = {dag_id: team_name for dag_id, team_name in query_results}

            # Ensure all requested dag_ids are in the result (with None for those not found)
            result = {dag_id: dag_id_to_team_name.get(dag_id) for dag_id in dag_ids}

            self.log.debug(
                "Resolved team names for %d DAGs: %s",
                len([team for team in result.values() if team is not None]),
                {dag_id: team for dag_id, team in result.items()},
            )

            return result

        except Exception:
            # Log the error, explicitly don't fail the scheduling loop
            self.log.exception("Failed to resolve team names for DAG IDs: %s", list(dag_ids))
            # Return dict with all None values to ensure graceful degradation
            return {}

    def _get_task_team_name(self, task_instance: TaskInstance, session: Session) -> str | None:
        """
        Resolve team name for a task instance using the DAG > Bundle > Team relationship chain.

        TaskInstance > DagModel (via dag_id) > DagBundleModel (via bundle_name) > Team

        :param task_instance: The TaskInstance to resolve team name for
        :param session: Database session for queries
        :return: Team name if found or None
        """
        # Use the batch query function with a single DAG ID
        dag_id_to_team_name = self._get_team_names_for_dag_ids([task_instance.dag_id], session)
        team_name = dag_id_to_team_name.get(task_instance.dag_id)

        if team_name:
            self.log.debug(
                "Resolved team name '%s' for task %s (dag_id=%s)",
                team_name,
                task_instance.task_id,
                task_instance.dag_id,
            )
        else:
            self.log.debug(
                "No team found for task %s (dag_id=%s) - DAG may not have bundle or team association",
                task_instance.task_id,
                task_instance.dag_id,
            )

        return team_name

    def _exit_gracefully(self, signum: int, frame: FrameType | None) -> None:
        """Clean up processor_agent to avoid leaving orphan processes."""
        if self._is_tracing_enabled():
            self._end_active_spans()

        if not _is_parent_process():
            # Only the parent process should perform the cleanup.
            return

        if self._enable_tracemalloc:
            import tracemalloc

            tracemalloc.stop()

        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        sys.exit(os.EX_OK)

    def _log_memory_usage(self, signum: int, frame: FrameType | None) -> None:
        import tracemalloc

        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")
        n = 10
        self.log.error(
            "scheduler memory usage:\n Top %d\n %s",
            n,
            "\n\t".join(map(str, top_stats[:n])),
        )

    def _debug_dump(self, signum: int, frame: FrameType | None) -> None:
        import threading
        from traceback import extract_stack

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

        id2name = {th.ident: th.name for th in threading.enumerate()}
        for threadId, stack in sys._current_frames().items():
            self.log.info("Stack Trace for Scheduler Job Runner on thread: %s", id2name[threadId])
            callstack = extract_stack(f=stack, limit=10)
            self.log.info("\n\t".join(map(repr, callstack)))
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

        if get_dialect_name(session) == "postgresql":
            # Optimization: to avoid littering the DB errors of "ERROR: canceling statement due to lock
            # timeout", try to take out a transactional advisory lock (unlocks automatically on
            # COMMIT/ROLLBACK)
            lock_acquired = session.execute(
                text("SELECT pg_try_advisory_xact_lock(:id)").bindparams(
                    id=DBLocks.SCHEDULER_CRITICAL_SECTION.value
                )
            ).scalar()
            if lock_acquired is None:
                lock_acquired = False
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

        max_tis = int(min(max_tis, pool_slots_free))

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
                .where(~DM.is_paused)
                .where(TI.state == TaskInstanceState.SCHEDULED)
                .where(DM.bundle_name.is_not(None))
                .options(selectinload(TI.dag_model))
                .order_by(-TI.priority_weight, DR.logical_date, TI.map_index)
            )

            if starved_pools:
                query = query.where(TI.pool.not_in(starved_pools))

            if starved_dags:
                query = query.where(TI.dag_id.not_in(starved_dags))

            if starved_tasks:
                query = query.where(tuple_(TI.dag_id, TI.task_id).not_in(starved_tasks))

            if starved_tasks_task_dagrun_concurrency:
                query = query.where(
                    tuple_(TI.dag_id, TI.run_id, TI.task_id).not_in(starved_tasks_task_dagrun_concurrency)
                )

            query = query.limit(max_tis)

            timer = Stats.timer("scheduler.critical_section_query_duration")
            timer.start()

            try:
                locked_query = with_row_locks(query, of=TI, session=session, skip_locked=True)
                task_instances_to_examine: list[TI] = list(session.scalars(locked_query).all())

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

            dag_id_to_team_name: dict[str, str | None] = {}
            if conf.getboolean("core", "multi_team"):
                # Batch query to resolve team names for all DAG IDs to optimize performance
                # Instead of individual queries in _try_to_load_executor(), resolve all team names upfront
                unique_dag_ids = {ti.dag_id for ti in task_instances_to_examine}
                dag_id_to_team_name = self._get_team_names_for_dag_ids(unique_dag_ids, session)
                self.log.debug(
                    "Batch resolved team names for %d unique DAG IDs in scheduling loop: %s",
                    len(unique_dag_ids),
                    list(unique_dag_ids),
                )

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
                    serialized_dag = self.scheduler_dag_bag.get_dag_for_run(
                        dag_run=task_instance.dag_run, session=session
                    )
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
                                "Not executing %s since the task concurrency for this task has been reached.",
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

                if executor_obj := self._try_to_load_executor(
                    task_instance, session, team_name=dag_id_to_team_name.get(task_instance.dag_id, NOTSET)
                ):
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
            if filter_for_tis is None:
                return []
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

        def _get_sentry_integration(executor: BaseExecutor) -> str:
            try:
                sentry_integration = executor.sentry_integration
            except AttributeError:
                # Old executor interface hard-codes the supports_sentry flag.
                if getattr(executor, "supports_sentry", False):
                    return "sentry_sdk.integrations.celery.CeleryIntegration"
                return ""
            if not isinstance(sentry_integration, str):
                self.log.warning(
                    "Ignoring invalid sentry_integration on executor",
                    executor=executor,
                    sentry_integration=sentry_integration,
                )
                return ""
            return sentry_integration

        # actually enqueue them
        for ti in task_instances:
            if ti.dag_run.state in State.finished_dr_states:
                ti.set_state(None, session=session)
                continue

            workload = workloads.ExecuteTask.make(
                ti,
                generator=executor.jwt_generator,
                sentry_integration=_get_sentry_integration(executor),
            )
            executor.queue_workload(workload, session=session)

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
        if self.job.max_tis_per_query == 0:
            max_tis = parallelism - num_occupied_slots
        else:
            max_tis = min(self.job.max_tis_per_query, parallelism - num_occupied_slots)
        if max_tis <= 0:
            self.log.debug("max_tis query size is less than or equal to zero. No query will be performed!")
            return 0

        queued_tis = self._executable_task_instances_to_queued(max_tis, session=session)

        # Sort queued TIs to their respective executor
        executor_to_queued_tis = self._executor_to_tis(queued_tis, session)
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

    @staticmethod
    def _is_metrics_enabled():
        return any(
            [
                conf.getboolean("metrics", "statsd_datadog_enabled", fallback=False),
                conf.getboolean("metrics", "statsd_on", fallback=False),
                conf.getboolean("metrics", "otel_on", fallback=False),
            ]
        )

    @staticmethod
    def _is_tracing_enabled():
        return conf.getboolean("traces", "otel_on")

    def _process_executor_events(self, executor: BaseExecutor, session: Session) -> int:
        return SchedulerJobRunner.process_executor_events(
            executor=executor,
            job_id=self.job.id,
            scheduler_dag_bag=self.scheduler_dag_bag,
            session=session,
        )

    @classmethod
    def process_executor_events(
        cls, executor: BaseExecutor, job_id: int | None, scheduler_dag_bag: DBDagBag, session: Session
    ) -> int:
        """
        Process task completion events from the executor and update task instance states.

        This method handles task state transitions reported by executors, ensuring proper
        state management, callback execution, and notification processing. It maintains
        scheduler architectural principles by delegating user code execution to appropriate
        isolated processes.

        The method handles several key scenarios:
        1. **Normal task completion**: Updates task states for successful/failed tasks
        2. **External termination**: Detects tasks killed outside Airflow and marks them as failed
        3. **Task requeuing**: Handles tasks that were requeued by other schedulers or executors
        4. **Callback processing**: Sends task callback requests to DAG Processor for execution
        5. **Email notifications**: Sends email notification requests to DAG Processor

        :param executor: The executor reporting task completion events
        :param job_id: The scheduler job ID, used to detect task requeuing by other schedulers
        :param scheduler_dag_bag: Serialized DAG bag for retrieving task definitions
        :param session: Database session for task instance updates

        :return: Number of events processed from the executor event buffer

        :raises Exception: If DAG retrieval or task processing fails, logs error and continues

        This is a classmethod because this is also used in `dag.test()`.
        `dag.test` execute DAGs with no scheduler, therefore it needs to handle the events pushed by the
        executors as well.
        """
        ti_primary_key_to_try_number_map: dict[tuple[str, str, str, int], int] = {}
        event_buffer = executor.get_event_buffer()
        tis_with_right_state: list[TaskInstanceKey] = []

        # Report execution
        for ti_key, (state, _) in event_buffer.items():
            # We create map (dag_id, task_id, logical_date) -> in-memory try_number
            ti_primary_key_to_try_number_map[ti_key.primary] = ti_key.try_number

            cls.logger().info("Received executor event with state %s for task instance %s", state, ti_key)
            if state in (
                TaskInstanceState.FAILED,
                TaskInstanceState.SUCCESS,
                TaskInstanceState.QUEUED,
                TaskInstanceState.RUNNING,
                TaskInstanceState.RESTARTING,
            ):
                tis_with_right_state.append(ti_key)

        # Return if no finished tasks
        if not tis_with_right_state:
            return len(event_buffer)

        # Check state of finished tasks
        filter_for_tis = TI.filter_for_tis(tis_with_right_state)
        if filter_for_tis is None:
            return len(event_buffer)
        asset_loader, _ = _eager_load_dag_run_for_validation()
        query = (
            select(TI)
            .where(filter_for_tis)
            .options(selectinload(TI.dag_model))
            .options(asset_loader)
            .options(joinedload(TI.dag_run).selectinload(DagRun.created_dag_version))
            .options(joinedload(TI.dag_version))
        )
        # row lock this entire set of taskinstances to make sure the scheduler doesn't fail when we have
        # multi-schedulers
        locked_query = with_row_locks(query, of=TI, session=session, skip_locked=True)
        tis: Iterator[TI] = session.scalars(locked_query)
        for ti in tis:
            try_number = ti_primary_key_to_try_number_map[ti.key.primary]
            buffer_key = ti.key.with_try_number(try_number)
            state, info = event_buffer.pop(buffer_key)

            if state in (TaskInstanceState.QUEUED, TaskInstanceState.RUNNING):
                ti.external_executor_id = info
                cls.logger().info("Setting external_executor_id for %s to %s", ti, info)
                continue

            msg = (
                "TaskInstance Finished: dag_id=%s, task_id=%s, run_id=%s, map_index=%s, "
                "run_start_date=%s, run_end_date=%s, "
                "run_duration=%s, state=%s, executor=%s, executor_state=%s, try_number=%s, max_tries=%s, "
                "pool=%s, queue=%s, priority_weight=%d, operator=%s, queued_dttm=%s, scheduled_dttm=%s,"
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
                ti.pool,
                ti.queue,
                ti.priority_weight,
                ti.operator,
                ti.queued_dttm,
                ti.scheduled_dttm,
                ti.queued_by_job_id,
                ti.pid,
            )

            if (active_ti_span := cls.active_spans.get("ti:" + str(ti.id))) is not None:
                cls.set_ti_span_attrs(span=active_ti_span, state=state, ti=ti)
                # End the span and remove it from the active_spans dict.
                active_ti_span.end(end_time=datetime_to_nano(ti.end_date))
                cls.active_spans.delete("ti:" + str(ti.id))
                ti.span_status = SpanStatus.ENDED
            else:
                if ti.span_status == SpanStatus.ACTIVE:
                    # Another scheduler has started the span.
                    # Update the SpanStatus to let the process know that it must end it.
                    ti.span_status = SpanStatus.SHOULD_END

            # There are two scenarios why the same TI with the same try_number is queued
            # after executor is finished with it:
            # 1) the TI was killed externally and it had no time to mark itself failed
            # - in this case we should mark it as failed here.
            # 2) the TI has been requeued after getting deferred - in this case either our executor has it
            # or the TI is queued by another job. Either ways we should not fail it.

            # All of this could also happen if the state is "running",
            # but that is handled by the scheduler detecting task instances without heartbeats.

            ti_queued = ti.try_number == buffer_key.try_number and ti.state in (
                TaskInstanceState.SCHEDULED,
                TaskInstanceState.QUEUED,
                TaskInstanceState.RUNNING,
                TaskInstanceState.RESTARTING,
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
                session.add(Log(event="state mismatch", extra=msg, task_instance=ti.key))

                # Get task from the Serialized DAG
                try:
                    dag = scheduler_dag_bag.get_dag_for_run(dag_run=ti.dag_run, session=session)
                    if not dag:
                        cls.logger().error(
                            "DAG '%s' for task instance %s not found in serialized_dag table",
                            ti.dag_id,
                            ti,
                        )
                        raise DagNotFound(f"DAG '{ti.dag_id}' not found in serialized_dag table")

                    task = dag.get_task(ti.task_id)
                except Exception:
                    cls.logger().exception("Marking task instance %s as %s", ti, state)
                    ti.set_state(state)
                    continue
                ti.task = task
                if task.has_on_retry_callback or task.has_on_failure_callback:
                    # Only log the error/extra info here, since the `ti.handle_failure()` path will log it
                    # too, which would lead to double logging
                    cls.logger().error(msg)
                    request = TaskCallbackRequest(
                        filepath=ti.dag_model.relative_fileloc or "",
                        bundle_name=ti.dag_version.bundle_name,
                        bundle_version=ti.dag_version.bundle_version,
                        ti=ti,
                        msg=msg,
                        task_callback_type=(
                            TaskInstanceState.UP_FOR_RETRY
                            if ti.is_eligible_to_retry()
                            else TaskInstanceState.FAILED
                        ),
                        context_from_server=TIRunContext(
                            dag_run=DRDataModel.model_validate(ti.dag_run, from_attributes=True),
                            max_tries=ti.max_tries,
                            variables=[],
                            connections=[],
                            xcom_keys_to_clear=[],
                        ),
                    )
                    executor.send_callback(request)

                # Handle cleared tasks that were successfully terminated by executor
                if ti.state == TaskInstanceState.RESTARTING and state == TaskInstanceState.SUCCESS:
                    cls.logger().info(
                        "Task %s was cleared and successfully terminated. Setting to scheduled for retry.", ti
                    )
                    # Adjust max_tries to allow retry beyond normal limits (like clearing does)
                    ti.max_tries = ti.try_number + ti.task.retries
                    ti.set_state(None)
                    continue

                # Send email notification request to DAG processor via DB
                if task.email and (task.email_on_failure or task.email_on_retry):
                    cls.logger().info(
                        "Sending email request for task %s to DAG Processor",
                        ti,
                    )
                    email_request = EmailRequest(
                        filepath=ti.dag_model.relative_fileloc or "",
                        bundle_name=ti.dag_version.bundle_name,
                        bundle_version=ti.dag_version.bundle_version,
                        ti=ti,
                        msg=msg,
                        email_type="retry" if ti.is_eligible_to_retry() else "failure",
                        context_from_server=TIRunContext(
                            dag_run=DRDataModel.model_validate(ti.dag_run, from_attributes=True),
                            max_tries=ti.max_tries,
                            variables=[],
                            connections=[],
                            xcom_keys_to_clear=[],
                        ),
                    )
                    executor.send_callback(email_request)

                # Update task state - emails are handled by DAG processor now
                ti.handle_failure(error=msg, session=session)

        return len(event_buffer)

    @classmethod
    def set_ti_span_attrs(cls, span, state, ti):
        span.set_attributes(
            {
                "airflow.category": "scheduler",
                "airflow.task.id": ti.id,
                "airflow.task.task_id": ti.task_id,
                "airflow.task.dag_id": ti.dag_id,
                "airflow.task.state": ti.state,
                "airflow.task.error": state == TaskInstanceState.FAILED,
                "airflow.task.start_date": str(ti.start_date),
                "airflow.task.end_date": str(ti.end_date),
                "airflow.task.duration": ti.duration,
                "airflow.task.executor_config": str(ti.executor_config),
                "airflow.task.logical_date": str(ti.logical_date),
                "airflow.task.hostname": ti.hostname,
                "airflow.task.log_url": ti.log_url,
                "airflow.task.operator": str(ti.operator),
                "airflow.task.try_number": ti.try_number,
                "airflow.task.executor_state": state,
                "airflow.task.pool": ti.pool,
                "airflow.task.queue": ti.queue,
                "airflow.task.priority_weight": ti.priority_weight,
                "airflow.task.queued_dttm": str(ti.queued_dttm),
                "airflow.task.queued_by_job_id": ti.queued_by_job_id,
                "airflow.task.pid": ti.pid,
            }
        )
        if span.is_recording():
            span.add_event(name="airflow.task.queued", timestamp=datetime_to_nano(ti.queued_dttm))
            span.add_event(name="airflow.task.started", timestamp=datetime_to_nano(ti.start_date))
            span.add_event(name="airflow.task.ended", timestamp=datetime_to_nano(ti.end_date))

    def _execute(self) -> int | None:
        import os

        # Mark this as a server context for secrets backend detection
        os.environ["_AIRFLOW_PROCESS_CONTEXT"] = "server"

        self.log.info("Starting the scheduler")

        reset_signals = self.register_signals()
        try:
            callback_sink: DatabaseCallbackSink

            from airflow.callbacks.database_callback_sink import DatabaseCallbackSink

            self.log.debug("Using DatabaseCallbackSink as callback sink.")
            callback_sink = DatabaseCallbackSink()

            for executor in self.job.executors:
                executor.job_id = self.job.id
                executor.callback_sink = callback_sink
                executor.start()

            # local import due to type_checking.
            from airflow.executors.base_executor import BaseExecutor

            # Pass a reference to the dictionary.
            # Any changes made by a dag_run instance, will be reflected to the dictionary of this class.
            DagRun.set_active_spans(active_spans=self.active_spans)
            BaseExecutor.set_active_spans(active_spans=self.active_spans)

            self._run_scheduler_loop()

            if settings.Session is not None:
                settings.Session.remove()
        except Exception:
            self.log.exception("Exception when executing SchedulerJob._run_scheduler_loop")
            raise
        finally:
            for executor in self.job.executors:
                try:
                    executor.end()
                except Exception:
                    self.log.exception("Exception when executing Executor.end on %s", executor)

            # Under normal execution, this doesn't matter, but by resetting signals it lets us run more things
            # in the same process under testing without leaking global state
            reset_signals.close()
            self.log.info("Exited execute loop")
        return None

    @provide_session
    def _update_dag_run_state_for_paused_dags(self, session: Session = NEW_SESSION) -> None:
        try:
            paused_runs = list(
                session.scalars(
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
            )
            for dag_run in paused_runs:
                dag = self.scheduler_dag_bag.get_dag_for_run(dag_run=dag_run, session=session)
                if dag is not None:
                    dag_run.dag = dag
                    _, callback_to_run = dag_run.update_state(execute_callbacks=False, session=session)
                    if callback_to_run:
                        self._send_dag_callbacks_to_processor(dag, callback_to_run)
        except Exception as e:  # should not fail the scheduler
            self.log.exception("Failed to update dag run state for paused dags due to %s", e)

    @provide_session
    def _end_active_spans(self, session: Session = NEW_SESSION):
        # No need to do a commit for every update. The annotation will commit all of them once at the end.
        for prefixed_key, span in self.active_spans.get_all().items():
            # Use partition to split on the first occurrence of ':'.
            prefix, sep, key = prefixed_key.partition(":")

            if prefix == "ti":
                ti_result = session.get(TaskInstance, key)
                if ti_result is None:
                    continue
                ti: TaskInstance = ti_result

                if ti.state in State.finished:
                    self.set_ti_span_attrs(span=span, state=ti.state, ti=ti)
                    span.end(end_time=datetime_to_nano(ti.end_date))
                    ti.span_status = SpanStatus.ENDED
                else:
                    span.end()
                    ti.span_status = SpanStatus.NEEDS_CONTINUANCE
            elif prefix == "dr":
                dag_run: DagRun | None = session.scalars(
                    select(DagRun).where(DagRun.id == int(key))
                ).one_or_none()
                if dag_run is None:
                    continue
                if dag_run.state in State.finished_dr_states:
                    dag_run.set_dagrun_span_attrs(span=span)

                    span.end(end_time=datetime_to_nano(dag_run.end_date))
                    dag_run.span_status = SpanStatus.ENDED
                else:
                    span.end()
                    dag_run.span_status = SpanStatus.NEEDS_CONTINUANCE
                    initial_dag_run_context = Trace.extract(dag_run.context_carrier)
                    with Trace.start_child_span(
                        span_name="current_scheduler_exited", parent_context=initial_dag_run_context
                    ) as s:
                        s.set_attribute("trace_status", "needs continuance")
            else:
                self.log.error("Found key with unknown prefix: '%s'", prefixed_key)

        # Even if there is a key with an unknown prefix, clear the dict.
        # If this method has been called, the scheduler is exiting.
        self.active_spans.clear()

    def _end_spans_of_externally_ended_ops(self, session: Session):
        # The scheduler that starts a dag_run or a task is also the one that starts the spans.
        # Each scheduler should end the spans that it has started.
        #
        # Otel spans are implemented in a certain way so that the objects
        # can't be shared between processes or get recreated.
        # It is done so that the process that starts a span, is also the one that ends it.
        #
        # If another scheduler has finished processing a dag_run or a task and there is a reference
        # on the active_spans dictionary, then the current scheduler started the span,
        # and therefore must end it.
        dag_runs_should_end: list[DagRun] = list(
            session.scalars(select(DagRun).where(DagRun.span_status == SpanStatus.SHOULD_END))
        )
        tis_should_end: list[TaskInstance] = list(
            session.scalars(select(TaskInstance).where(TaskInstance.span_status == SpanStatus.SHOULD_END))
        )

        for dag_run in dag_runs_should_end:
            active_dagrun_span = self.active_spans.get("dr:" + str(dag_run.id))
            if active_dagrun_span is not None:
                if dag_run.state in State.finished_dr_states:
                    dag_run.set_dagrun_span_attrs(span=active_dagrun_span)

                    active_dagrun_span.end(end_time=datetime_to_nano(dag_run.end_date))
                else:
                    active_dagrun_span.end()
                self.active_spans.delete("dr:" + str(dag_run.id))
                dag_run.span_status = SpanStatus.ENDED

        for ti in tis_should_end:
            active_ti_span = self.active_spans.get("ti:" + ti.id)
            if active_ti_span is not None:
                if ti.state in State.finished:
                    self.set_ti_span_attrs(span=active_ti_span, state=ti.state, ti=ti)
                    active_ti_span.end(end_time=datetime_to_nano(ti.end_date))
                else:
                    active_ti_span.end()
                self.active_spans.delete("ti:" + ti.id)
                ti.span_status = SpanStatus.ENDED

    def _recreate_unhealthy_scheduler_spans_if_needed(self, dag_run: DagRun, session: Session):
        # There are two scenarios:
        #   1. scheduler is unhealthy but managed to update span_status
        #   2. scheduler is unhealthy and didn't manage to make any updates
        # Check the span_status first, in case the 2nd db query can be avoided (scenario 1).

        # If the dag_run is scheduled by a different scheduler, and it's still running and the span is active,
        # then check the Job table to determine if the initial scheduler is still healthy.
        if (
            dag_run.scheduled_by_job_id != self.job.id
            and dag_run.state in State.unfinished_dr_states
            and dag_run.span_status == SpanStatus.ACTIVE
        ):
            initial_scheduler_id = dag_run.scheduled_by_job_id
            job: Job | None = session.scalars(
                select(Job).where(
                    Job.id == initial_scheduler_id,
                    Job.job_type == "SchedulerJob",
                )
            ).one_or_none()
            if job is None:
                return

            if not job.is_alive():
                # Start a new span for the dag_run.
                dr_span = Trace.start_root_span(
                    span_name=f"{dag_run.dag_id}_recreated",
                    component="dag",
                    start_time=dag_run.queued_at,
                    start_as_current=False,
                )
                carrier = Trace.inject()
                # Update the context_carrier and leave the SpanStatus as ACTIVE.
                dag_run.context_carrier = carrier
                self.active_spans.set("dr:" + str(dag_run.id), dr_span)

                tis = dag_run.get_task_instances(session=session)

                # At this point, any tis will have been adopted by the current scheduler,
                # and ti.queued_by_job_id will point to the current id.
                # Any tis that have been executed by the unhealthy scheduler, will need a new span
                # so that it can be associated with the new dag_run span.
                tis_needing_spans = [
                    ti
                    for ti in tis
                    # If it has started and there is a reference on the active_spans dict,
                    # then it was started by the current scheduler.
                    if ti.start_date is not None and self.active_spans.get("ti:" + ti.id) is None
                ]

                dr_context = Trace.extract(dag_run.context_carrier)
                for ti in tis_needing_spans:
                    ti_span = Trace.start_child_span(
                        span_name=f"{ti.task_id}_recreated",
                        parent_context=dr_context,
                        start_time=ti.queued_dttm,
                        start_as_current=False,
                    )
                    ti_carrier = Trace.inject()
                    ti.context_carrier = ti_carrier

                    if ti.state in State.finished:
                        self.set_ti_span_attrs(span=ti_span, state=ti.state, ti=ti)
                        ti_span.end(end_time=datetime_to_nano(ti.end_date))
                        ti.span_status = SpanStatus.ENDED
                    else:
                        ti.span_status = SpanStatus.ACTIVE
                        self.active_spans.set("ti:" + ti.id, ti_span)

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
            #. Check for expired Deadlines
                #. Hand off processing the expired Deadlines if any are found
        """
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

        if self._is_metrics_enabled() or self._is_tracing_enabled():
            timers.call_regular_interval(
                conf.getfloat("scheduler", "pool_metrics_interval", fallback=5.0),
                self._emit_pool_metrics,
            )

        if self._is_metrics_enabled():
            timers.call_regular_interval(
                conf.getfloat("scheduler", "ti_metrics_interval", fallback=30.0),
                self._emit_ti_metrics,
            )

            timers.call_regular_interval(
                conf.getfloat("scheduler", "dagrun_metrics_interval", fallback=30.0),
                self._emit_running_dags_metric,
            )

        timers.call_regular_interval(
            conf.getfloat("scheduler", "task_instance_heartbeat_timeout_detection_interval", fallback=10.0),
            self._find_and_purge_task_instances_without_heartbeats,
        )

        timers.call_regular_interval(60.0, self._update_dag_run_state_for_paused_dags)

        timers.call_regular_interval(
            conf.getfloat("scheduler", "task_queued_timeout_check_interval"),
            self._handle_tasks_stuck_in_queued,
        )

        timers.call_regular_interval(
            conf.getfloat("scheduler", "parsing_cleanup_interval"),
            self._update_asset_orphanage,
        )
        timers.call_regular_interval(
            conf.getfloat("scheduler", "parsing_cleanup_interval"),
            self._remove_unreferenced_triggers,
        )

        if any(x.is_local for x in self.job.executors):
            bundle_cleanup_mgr = BundleUsageTrackingManager()
            check_interval = conf.getint(
                section="dag_processor",
                key="stale_bundle_cleanup_interval",
            )
            if check_interval > 0:
                timers.call_regular_interval(
                    delay=check_interval,
                    action=bundle_cleanup_mgr.remove_stale_bundle_versions,
                )

        for loop_count in itertools.count(start=1):
            with (
                DebugTrace.start_span(span_name="scheduler_job_loop", component="SchedulerJobRunner") as span,
                Stats.timer("scheduler.scheduler_loop_duration") as timer,
            ):
                span.set_attributes(
                    {
                        "category": "scheduler",
                        "loop_count": loop_count,
                    }
                )

                with create_session() as session:
                    if self._is_tracing_enabled():
                        self._end_spans_of_externally_ended_ops(session)

                    # This will schedule for as many executors as possible.
                    num_queued_tis = self._do_scheduling(session)
                    # Don't keep any objects alive -- we've possibly just looked at 500+ ORM objects!
                    session.expunge_all()

                # Heartbeat all executors, even if they're not receiving new tasks this loop. It will be
                # either a no-op, or they will check-in on currently running tasks and send out new
                # events to be processed below.
                for executor in self.job.executors:
                    executor.heartbeat()

                with create_session() as session:
                    num_finished_events = 0
                    for executor in self.job.executors:
                        num_finished_events += self._process_executor_events(
                            executor=executor, session=session
                        )

                for executor in self.job.executors:
                    try:
                        with create_session() as session:
                            self._process_task_event_logs(executor._task_event_logs, session)
                    except Exception:
                        self.log.exception("Something went wrong when trying to save task event logs.")

                with create_session() as session:
                    # Only retrieve expired deadlines that haven't been processed yet.
                    # `missed` is False by default until the handler sets it.
                    for deadline in session.scalars(
                        select(Deadline)
                        .where(Deadline.deadline_time < datetime.now(timezone.utc))
                        .where(~Deadline.missed)
                        .options(selectinload(Deadline.callback), selectinload(Deadline.dagrun))
                    ):
                        deadline.handle_miss(session)

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
            if conf.getboolean("scheduler", "use_job_schedule", fallback=True):
                self._create_dagruns_for_dags(guard, session)

            self._start_queued_dagruns(session)
            guard.commit()

            # Bulk fetch the currently active dag runs for the dags we are
            # examining, rather than making one query per DagRun
            dag_runs = DagRun.get_running_dag_runs_to_examine(session=session)

            callback_tuples = self._schedule_all_dag_runs(guard, dag_runs, session)

        # Send the callbacks after we commit to ensure the context is up to date when it gets run
        # cache saves time during scheduling of many dag_runs for same dag
        cached_get_dag: Callable[[DagRun], SerializedDAG | None] = lru_cache()(
            partial(self.scheduler_dag_bag.get_dag_for_run, session=session)
        )
        for dag_run, callback_to_run in callback_tuples:
            dag = cached_get_dag(dag_run)
            if dag:
                # Sending callbacks to the database, so it must be done outside of prohibit_commit.
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

    def _create_dagruns_for_partitioned_asset_dags(self, session: Session) -> set[str]:
        partition_dag_ids: set[str] = set()
        apdrs: Iterable[AssetPartitionDagRun] = session.scalars(
            select(AssetPartitionDagRun).where(AssetPartitionDagRun.created_dag_run_id.is_(None))
        )
        for apdr in apdrs:
            if TYPE_CHECKING:
                assert apdr.target_dag_id
            partition_dag_ids.add(apdr.target_dag_id)
            dag = _get_current_dag(dag_id=apdr.target_dag_id, session=session)
            if not dag:
                self.log.error("Dag '%s' not found in serialized_dag table", apdr.target_dag_id)
                continue

            run_after = timezone.utcnow()
            dag_run = dag.create_dagrun(
                run_id=DagRun.generate_run_id(
                    run_type=DagRunType.ASSET_TRIGGERED, logical_date=None, run_after=run_after
                ),
                logical_date=None,
                data_interval=None,
                partition_key=apdr.partition_key,
                run_after=run_after,
                run_type=DagRunType.ASSET_TRIGGERED,
                triggered_by=DagRunTriggeredByType.ASSET,
                state=DagRunState.QUEUED,
                creating_job_id=self.job.id,
                session=session,
            )
            session.flush()
            apdr.created_dag_run_id = dag_run.id
            session.flush()

        return partition_dag_ids

    @retry_db_transaction
    def _create_dagruns_for_dags(self, guard: CommitProhibitorGuard, session: Session) -> None:
        """Find Dag Models needing DagRuns and Create Dag Runs with retries in case of OperationalError."""
        partition_dag_ids: set[str] = self._create_dagruns_for_partitioned_asset_dags(session)

        query, triggered_date_by_dag = DagModel.dags_needing_dagruns(session)
        all_dags_needing_dag_runs = set(query.all())
        asset_triggered_dags = [d for d in all_dags_needing_dag_runs if d.dag_id in triggered_date_by_dag]
        non_asset_dags = {
            d
            # filter asset-triggered Dags
            for d in all_dags_needing_dag_runs.difference(asset_triggered_dags)
            # filter asset partition triggered Dags
            if d.dag_id not in partition_dag_ids
        }
        self._create_dag_runs(non_asset_dags, session)
        if asset_triggered_dags:
            self._create_dag_runs_asset_triggered(
                dag_models=[d for d in asset_triggered_dags if d.dag_id not in partition_dag_ids],
                triggered_date_by_dag=triggered_date_by_dag,
                session=session,
            )

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
        backfills = list(session.scalars(query))
        if not backfills:
            return
        self.log.info("marking %s backfills as complete", len(backfills))
        for b in backfills:
            b.completed_at = now

    @add_debug_span
    def _create_dag_runs(self, dag_models: Collection[DagModel], session: Session) -> None:
        """Create a DAG run and update the dag_model to control if/when the next DAGRun should be created."""
        # Bulk Fetch DagRuns with dag_id and logical_date same
        # as DagModel.dag_id and DagModel.next_dagrun
        # This list is used to verify if the DagRun already exist so that we don't attempt to create
        # duplicate DagRuns
        existing_dagruns = (
            session.execute(
                select(DagRun.dag_id, DagRun.logical_date).where(
                    tuple_(DagRun.dag_id, DagRun.logical_date).in_(
                        (dm.dag_id, dm.next_dagrun) for dm in dag_models
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
            if dag_model.next_dagrun is None:
                self.log.error(
                    "dag_model.next_dagrun is None; expected datetime",
                    dag_id=dag_model.dag_id,
                )
                continue
            if dag_model.next_dagrun_create_after is None:
                self.log.error(
                    "dag_model.next_dagrun_create_after is None; expected datetime",
                    dag_id=dag_model.dag_id,
                )
                continue

            serdag = _get_current_dag(dag_id=dag_model.dag_id, session=session)
            if not serdag:
                self.log.error("DAG '%s' not found in serialized_dag table", dag_model.dag_id)
                continue

            data_interval = get_next_data_interval(serdag.timetable, dag_model)
            # Explicitly check if the DagRun already exists. This is an edge case
            # where a Dag Run is created but `DagModel.next_dagrun` and `DagModel.next_dagrun_create_after`
            # are not updated.
            # We opted to check DagRun existence instead
            # of catching an Integrity error and rolling back the session i.e
            # we need to set DagModel.next_dagrun_info if the DagRun already exists or if we
            # create a new one. This is so that in the next scheduling loop we try to create new runs
            # instead of falling in a loop of IntegrityError.
            if (serdag.dag_id, dag_model.next_dagrun) not in existing_dagruns:
                try:
                    serdag.create_dagrun(
                        run_id=serdag.timetable.generate_run_id(
                            run_type=DagRunType.SCHEDULED,
                            run_after=timezone.coerce_datetime(dag_model.next_dagrun),
                            data_interval=data_interval,
                        ),
                        logical_date=dag_model.next_dagrun,
                        data_interval=data_interval,
                        run_after=dag_model.next_dagrun_create_after,
                        run_type=DagRunType.SCHEDULED,
                        triggered_by=DagRunTriggeredByType.TIMETABLE,
                        state=DagRunState.QUEUED,
                        creating_job_id=self.job.id,
                        session=session,
                    )
                    active_runs_of_dags[serdag.dag_id] += 1

                # Exceptions like ValueError, ParamValidationError, etc. are raised by
                # DagModel.create_dagrun() when dag is misconfigured. The scheduler should not
                # crash due to misconfigured dags. We should log any exception encountered
                # and continue to the next serdag.
                except Exception:
                    self.log.exception("Failed creating DagRun for %s", serdag.dag_id)
                    # todo: if you get a database error here, continuing does not work because
                    #  session needs rollback. you need either to make smaller transactions and
                    #  commit after every dag run or use savepoints.
                    #  https://github.com/apache/airflow/issues/59120
                    continue

            self._update_next_dagrun_fields(
                serdag=serdag,
                dag_model=dag_model,
                session=session,
                active_non_backfill_runs=active_runs_of_dags[serdag.dag_id],
                data_interval=data_interval,
            )

        # TODO[HA]: Should we do a session.flush() so we don't have to keep lots of state/object in
        #  memory for larger dags? or expunge_all()

    def _create_dag_runs_asset_triggered(
        self,
        dag_models: Collection[DagModel],
        triggered_date_by_dag: dict[str, datetime],
        session: Session,
    ) -> None:
        """For DAGs that are triggered by assets, create dag runs."""
        triggered_dates: dict[str, DateTime] = {
            dag_id: timezone.coerce_datetime(last_asset_event_time)
            for dag_id, last_asset_event_time in triggered_date_by_dag.items()
        }

        for dag_model in dag_models:
            dag = _get_current_dag(dag_id=dag_model.dag_id, session=session)
            if not dag:
                self.log.error("DAG '%s' not found in serialized_dag table", dag_model.dag_id)
                continue

            if not isinstance(dag.timetable, AssetTriggeredTimetable):
                self.log.error(
                    "DAG '%s' was asset-scheduled, but didn't have an AssetTriggeredTimetable!",
                    dag_model.dag_id,
                )
                continue

            triggered_date = triggered_dates[dag.dag_id]
            cte = (
                select(func.max(DagRun.run_after).label("previous_dag_run_run_after"))
                .where(
                    DagRun.dag_id == dag.dag_id,
                    DagRun.run_type == DagRunType.ASSET_TRIGGERED,
                    DagRun.run_after < triggered_date,
                )
                .cte()
            )

            asset_events = list(
                session.scalars(
                    select(AssetEvent)
                    .where(
                        or_(
                            AssetEvent.asset_id.in_(
                                select(DagScheduleAssetReference.asset_id).where(
                                    DagScheduleAssetReference.dag_id == dag.dag_id
                                )
                            ),
                            AssetEvent.source_aliases.any(
                                AssetAliasModel.scheduled_dags.any(
                                    DagScheduleAssetAliasReference.dag_id == dag.dag_id
                                )
                            ),
                        ),
                        AssetEvent.timestamp <= triggered_date,
                        AssetEvent.timestamp > func.coalesce(cte.c.previous_dag_run_run_after, date.min),
                    )
                    .order_by(AssetEvent.timestamp.asc(), AssetEvent.id.asc())
                )
            )

            dag_run = dag.create_dagrun(
                run_id=DagRun.generate_run_id(
                    run_type=DagRunType.ASSET_TRIGGERED, logical_date=None, run_after=triggered_date
                ),
                logical_date=None,
                data_interval=None,
                run_after=triggered_date,
                run_type=DagRunType.ASSET_TRIGGERED,
                triggered_by=DagRunTriggeredByType.ASSET,
                state=DagRunState.QUEUED,
                creating_job_id=self.job.id,
                session=session,
            )
            Stats.incr("asset.triggered_dagruns")
            dag_run.consumed_asset_events.extend(asset_events)
            session.execute(delete(AssetDagRunQueue).where(AssetDagRunQueue.target_dag_id == dag_run.dag_id))

    def _lock_backfills(self, dag_runs: Collection[DagRun], session: Session) -> dict[int, Backfill]:
        """
        Lock Backfill rows to prevent race conditions when multiple schedulers run concurrently.

        :param dag_runs: Collection of Dag runs to process
        :param session: DB session
        :return: Dict mapping backfill_id to locked Backfill objects
        """
        if not (backfill_ids := {dr.backfill_id for dr in dag_runs if dr.backfill_id is not None}):
            return {}

        locked_backfills = {
            b.id: b
            for b in session.scalars(
                select(Backfill).where(Backfill.id.in_(backfill_ids)).with_for_update(skip_locked=True)
            )
        }

        if skipped_backfills := backfill_ids - locked_backfills.keys():
            self.log.debug(
                "Skipping backfill runs for backfill_ids=%s - locked by another scheduler",
                skipped_backfills,
            )

        return locked_backfills

    @add_debug_span
    def _start_queued_dagruns(self, session: Session) -> None:
        """Find DagRuns in queued state and decide moving them to running state."""
        dag_runs: Collection[DagRun] = list(DagRun.get_queued_dag_runs_to_set_running(session))

        # Lock backfills to prevent race conditions with concurrent schedulers
        locked_backfills = self._lock_backfills(dag_runs, session)

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

        @add_debug_span
        def _update_state(dag: SerializedDAG, dag_run: DagRun):
            span = Trace.get_current_span()
            span.set_attributes(
                {
                    "state": str(DagRunState.RUNNING),
                    "run_id": dag_run.run_id,
                    "type": dag_run.run_type,
                    "dag_id": dag_run.dag_id,
                }
            )

            dag_run.state = DagRunState.RUNNING
            dag_run.start_date = timezone.utcnow()
            if (
                dag.timetable.periodic
                and dag_run.run_type != DagRunType.MANUAL
                and dag_run.triggered_by != DagRunTriggeredByType.ASSET
                and dag_run.clear_number < 1
            ):
                expected_start_date = dag_run.run_after
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
        cached_get_dag: Callable[[DagRun], SerializedDAG | None] = lru_cache()(
            partial(self.scheduler_dag_bag.get_dag_for_run, session=session)
        )

        span = Trace.get_current_span()
        for dag_run in dag_runs:
            dag_id = dag_run.dag_id
            run_id = dag_run.run_id
            backfill_id = dag_run.backfill_id
            dag = dag_run.dag = cached_get_dag(dag_run)
            if not dag:
                self.log.error("DAG '%s' not found in serialized_dag table", dag_run.dag_id)
                continue
            active_runs = active_runs_of_dags[(dag_id, backfill_id)]
            if backfill_id is not None:
                if backfill_id not in locked_backfills:
                    # Another scheduler has this backfill locked, skip this run
                    continue
                backfill = dag_run.backfill
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
            elif dag_run.max_active_runs:
                # Using dag_run.max_active_runs which links to DagModel to ensure we are checking
                # against the most recent changes on the dag and not using stale serialized dag
                if active_runs >= dag_run.max_active_runs:
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
            dag_run.notify_dagrun_state_changed(msg="started")

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
        with DebugTrace.start_root_span(
            span_name="_schedule_dag_run", component="SchedulerJobRunner"
        ) as span:
            span.set_attributes(
                {
                    "dag_id": dag_run.dag_id,
                    "run_id": dag_run.run_id,
                    "run_type": dag_run.run_type,
                }
            )
            callback: DagCallbackRequest | None = None

            dag = dag_run.dag = self.scheduler_dag_bag.get_dag_for_run(dag_run=dag_run, session=session)
            dag_model = DM.get_dagmodel(dag_run.dag_id, session)
            if not dag_model:
                self.log.error("Couldn't find DAG model %s in database!", dag_run.dag_id)
                return callback

            if not dag:
                self.log.error("Couldn't find DAG %s in DAG bag!", dag_run.dag_id)
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

                # TODO: questionable that this logic does what it is trying to do
                #  I think its intent is, in part, to do this when it's the latest scheduled run
                #  but it does not know that it is the latest. I think it could probably check that
                #  logical date is equal to or greater than DagModel.next_dagrun, or something
                if dag_run.state in State.finished_dr_states and dag_run.run_type == DagRunType.SCHEDULED:
                    self._update_next_dagrun_fields(
                        serdag=dag,
                        dag_model=dag_model,
                        session=session,
                        data_interval=get_run_data_interval(dag.timetable, dag_run),
                    )

                dag_run_reloaded = session.scalar(
                    select(DagRun)
                    .where(DagRun.id == dag_run.id)
                    .options(
                        selectinload(DagRun.consumed_asset_events).selectinload(AssetEvent.asset),
                        selectinload(DagRun.consumed_asset_events).selectinload(AssetEvent.source_aliases),
                    )
                )
                if dag_run_reloaded is None:
                    # This should never happen since we just had the dag_run
                    self.log.error("DagRun %s was deleted unexpectedly", dag_run.id)
                    return None
                dag_run = dag_run_reloaded
                callback_to_execute = DagCallbackRequest(
                    filepath=dag_model.relative_fileloc or "",
                    dag_id=dag.dag_id,
                    run_id=dag_run.run_id,
                    bundle_name=dag_model.bundle_name,
                    bundle_version=dag_run.bundle_version,
                    context_from_server=DagRunContext(
                        dag_run=dag_run,
                        last_ti=dag_run.get_last_ti(dag=dag, session=session),
                    ),
                    is_failure_callback=True,
                    msg="timed_out",
                )

                dag_run.notify_dagrun_state_changed(msg="timed_out")
                if dag_run.end_date and dag_run.start_date:
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

            if dag_run.logical_date and dag_run.logical_date > timezone.utcnow():
                self.log.error("Logical date is in future: %s", dag_run.logical_date)
                return callback

            if not dag_run.bundle_version and not self._verify_integrity_if_dag_changed(
                dag_run=dag_run, session=session
            ):
                self.log.warning(
                    "The DAG disappeared before verifying integrity: %s. Skipping.", dag_run.dag_id
                )
                return callback

            if (
                self._is_tracing_enabled()
                and dag_run.scheduled_by_job_id is not None
                and dag_run.scheduled_by_job_id != self.job.id
                and self.active_spans.get("dr:" + str(dag_run.id)) is None
            ):
                # If the dag_run has been previously scheduled by another job and there is no active span,
                # then check if the job is still healthy.
                # If it's not healthy, then recreate the spans.
                self._recreate_unhealthy_scheduler_spans_if_needed(dag_run, session)

            dag_run.scheduled_by_job_id = self.job.id

            # TODO[HA]: Rename update_state -> schedule_dag_run, ?? something else?
            schedulable_tis, callback_to_run = dag_run.update_state(session=session, execute_callbacks=False)

            # TODO: questionable that this logic does what it is trying to do
            #  I think its intent is, in part, to do this when it's the latest scheduled run
            #  but it does not know that it is the latest. I think it could probably check that
            #  logical date is equal to or greater than DagModel.next_dagrun, or something
            if dag_run.state in State.finished_dr_states and dag_run.run_type == DagRunType.SCHEDULED:
                self._update_next_dagrun_fields(
                    serdag=dag,
                    dag_model=dag_model,
                    session=session,
                    data_interval=get_run_data_interval(dag.timetable, dag_run),
                )

            # This will do one query per dag run. We "could" build up a complex
            # query to update all the TIs across all the logical dates and dag
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

    def _update_next_dagrun_fields(
        self,
        *,
        serdag: SerializedDAG,
        dag_model: DagModel,
        session: Session,
        active_non_backfill_runs: int | None = None,
        data_interval: DataInterval | None,
    ):
        """
        Conditionally update fields next_dagrun and next_dagrun_create_after on dag table.

        If dag exceeds max active runs, set to None.

        If dag's timetable not schedulable, don't update.

        Otherwise, update via ``DagModel.calculate_dagrun_date_fields``.
        """
        exceeds_max, active_runs = self._exceeds_max_active_runs(
            dag_model=dag_model,
            active_non_backfill_runs=active_non_backfill_runs,
            session=session,
        )
        if exceeds_max:
            self.log.info(
                "Dag exceeds max_active_runs; not creating any more runs",
                dag_id=dag_model.dag_id,
                active_runs=active_runs,
                max_active_runs=dag_model.max_active_runs,
            )
            # null out next_dagrun_create_after so scheduler will not examine this dag
            # this is periodically reconsidered in the scheduler and dag processor.
            dag_model.next_dagrun_create_after = None
            return

        # If the DAG never schedules skip save runtime
        if not serdag.timetable.can_be_scheduled:
            return

        dag_model.calculate_dagrun_date_fields(dag=serdag, last_automated_dag_run=data_interval)

    def _verify_integrity_if_dag_changed(self, dag_run: DagRun, session: Session) -> bool:
        """
        Only run DagRun.verify integrity if Serialized DAG has changed since it is slow.

        Return True if we determine that DAG still exists.
        """
        latest_dag_version = DagVersion.get_latest_version(dag_run.dag_id, session=session)
        if latest_dag_version is None:
            return False
        if TYPE_CHECKING:
            assert latest_dag_version

        if dag_run.check_version_id_exists_in_dr(latest_dag_version.id, session):
            self.log.debug("DAG %s not changed structure, skipping dagrun.verify_integrity", dag_run.dag_id)
            return True
        # Refresh the DAG
        dag_run.dag = self.scheduler_dag_bag.get_dag_for_run(dag_run=dag_run, session=session)
        if not dag_run.dag:
            return False
        # Select all TIs in State.unfinished and update the dag_version_id
        for ti in dag_run.task_instances:
            if ti.state in State.unfinished:
                ti.dag_version = latest_dag_version
        # Verify integrity also takes care of session.flush
        dag_run.verify_integrity(dag_version_id=latest_dag_version.id, session=session)

        return True

    def _send_dag_callbacks_to_processor(
        self,
        dag: SerializedDAG,
        callback: DagCallbackRequest | None = None,
    ) -> None:
        if callback:
            self.job.executor.send_callback(callback)
        else:
            self.log.debug("callback is empty")

    @provide_session
    def _handle_tasks_stuck_in_queued(self, session: Session = NEW_SESSION) -> None:
        """
        Handle the scenario where a task is queued for longer than `task_queued_timeout`.

        Tasks can get stuck in queued for a wide variety of reasons (e.g. celery loses
        track of a task, a cluster can't further scale up its workers, etc.), but tasks
        should not be stuck in queued for a long time.

        We will attempt to requeue the task (by revoking it from executor and setting to
        scheduled) up to 2 times before failing the task.
        """
        tasks_stuck_in_queued = self._get_tis_stuck_in_queued(session)
        for executor, stuck_tis in self._executor_to_tis(tasks_stuck_in_queued, session).items():
            try:
                for ti in stuck_tis:
                    executor.revoke_task(ti=ti)
                    self._maybe_requeue_stuck_ti(
                        ti=ti,
                        session=session,
                        executor=executor,
                    )
                    session.commit()
            except NotImplementedError:
                continue

    def _get_tis_stuck_in_queued(self, session) -> Iterable[TaskInstance]:
        """Query db for TIs that are stuck in queued."""
        return session.scalars(
            select(TI).where(
                TI.state == TaskInstanceState.QUEUED,
                TI.queued_dttm < (timezone.utcnow() - timedelta(seconds=self._task_queued_timeout)),
                TI.queued_by_job_id == self.job.id,
            )
        )

    def _maybe_requeue_stuck_ti(self, *, ti, session, executor):
        """
        Requeue task if it has not been attempted too many times.

        Otherwise, fail it.
        """
        num_times_stuck = self._get_num_times_stuck_in_queued(ti, session)
        if num_times_stuck < self._num_stuck_queued_retries:
            self.log.info("Task stuck in queued; will try to requeue. task_instance=%s", ti)
            session.add(
                Log(
                    event=TASK_STUCK_IN_QUEUED_RESCHEDULE_EVENT,
                    task_instance=ti.key,
                    extra=(
                        f"Task was in queued state for longer than {self._task_queued_timeout} "
                        "seconds; task state will be set back to scheduled."
                    ),
                )
            )
            self._reschedule_stuck_task(ti, session=session)
        else:
            self.log.info(
                "Task requeue attempts exceeded max; marking failed. task_instance=%s",
                ti,
            )
            msg = f"Task was requeued more than {self._num_stuck_queued_retries} times and will be failed."
            session.add(
                Log(
                    event="stuck in queued tries exceeded",
                    task_instance=ti.key,
                    extra=msg,
                )
            )

            try:
                dag = self.scheduler_dag_bag.get_dag_for_run(dag_run=ti.dag_run, session=session)
                task = dag.get_task(ti.task_id)
            except Exception:
                self.log.warning(
                    "The DAG or task could not be found. If a failure callback exists, it will not be run.",
                    exc_info=True,
                )
            else:
                if task.has_on_failure_callback:
                    if inspect(ti).detached:
                        ti = session.merge(ti)
                    request = TaskCallbackRequest(
                        filepath=ti.dag_model.relative_fileloc,
                        bundle_name=ti.dag_version.bundle_name,
                        bundle_version=ti.dag_version.bundle_version,
                        ti=ti,
                        msg=msg,
                        context_from_server=TIRunContext(
                            dag_run=ti.dag_run,
                            max_tries=ti.max_tries,
                            variables=[],
                            connections=[],
                            xcom_keys_to_clear=[],
                        ),
                    )
                    executor.send_callback(request)
            finally:
                ti.set_state(TaskInstanceState.FAILED, session=session)
                executor.fail(ti.key)

    def _reschedule_stuck_task(self, ti: TaskInstance, session: Session):
        filter_for_tis = TI.filter_for_tis([ti])
        if filter_for_tis is None:
            return
        session.execute(
            update(TI)
            .where(filter_for_tis)
            .values(
                state=TaskInstanceState.SCHEDULED,
                queued_dttm=None,
                queued_by_job_id=None,
                scheduled_dttm=timezone.utcnow(),
            )
            .execution_options(synchronize_session=False)
        )

    @provide_session
    def _get_num_times_stuck_in_queued(self, ti: TaskInstance, session: Session = NEW_SESSION) -> int:
        """
        Check the Log table to see how many times a task instance has been stuck in queued.

        We can then use this information to determine whether to reschedule a task or fail it.
        """
        last_running_time = session.scalar(
            select(Log.dttm)
            .where(
                Log.dag_id == ti.dag_id,
                Log.task_id == ti.task_id,
                Log.run_id == ti.run_id,
                Log.map_index == ti.map_index,
                Log.try_number == ti.try_number,
                Log.event == "running",
            )
            .order_by(desc(Log.dttm))
            .limit(1)
        )

        query = session.query(Log).where(
            Log.task_id == ti.task_id,
            Log.dag_id == ti.dag_id,
            Log.run_id == ti.run_id,
            Log.map_index == ti.map_index,
            Log.try_number == ti.try_number,
            Log.event == TASK_STUCK_IN_QUEUED_RESCHEDULE_EVENT,
        )

        if last_running_time is not None:
            query = query.where(Log.dttm > last_running_time)

        count_result: int | None = query.count()
        return count_result if count_result is not None else 0

    previous_ti_metrics: dict[TaskInstanceState, dict[tuple[str, str, str], int]] = {}

    @provide_session
    def _emit_ti_metrics(self, session: Session = NEW_SESSION) -> None:
        metric_states = {State.SCHEDULED, State.QUEUED, State.RUNNING, State.DEFERRED}
        stmt = (
            select(
                TaskInstance.state,
                TaskInstance.dag_id,
                TaskInstance.task_id,
                TaskInstance.queue,
                func.count(TaskInstance.task_id).label("count"),
            )
            .filter(TaskInstance.state.in_(metric_states))
            .group_by(TaskInstance.state, TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.queue)
        )
        all_states_metric = session.execute(stmt).all()

        for state in metric_states:
            if state not in self.previous_ti_metrics:
                self.previous_ti_metrics[state] = {}

            ti_metrics = {
                (dag_id, task_id, queue): count
                for row_state, dag_id, task_id, queue, count in all_states_metric
                if row_state == state
            }

            for (dag_id, task_id, queue), count in ti_metrics.items():
                Stats.gauge(f"ti.{state}.{queue}.{dag_id}.{task_id}", float(count))
                Stats.gauge(
                    f"ti.{state}", float(count), tags={"queue": queue, "dag_id": dag_id, "task_id": task_id}
                )

            for prev_key in self.previous_ti_metrics[state]:
                # Reset previously exported stats that are no longer present in current metrics to zero
                if prev_key not in ti_metrics:
                    dag_id, task_id, queue = prev_key
                    Stats.gauge(f"ti.{state}.{queue}.{dag_id}.{task_id}", 0)
                    Stats.gauge(f"ti.{state}", 0, tags={"queue": queue, "dag_id": dag_id, "task_id": task_id})

            self.previous_ti_metrics[state] = ti_metrics

    @provide_session
    def _emit_running_dags_metric(self, session: Session = NEW_SESSION) -> None:
        stmt = select(func.count()).select_from(DagRun).where(DagRun.state == DagRunState.RUNNING)
        running_dags = float(session.scalar(stmt))
        Stats.gauge("scheduler.dagruns.running", running_dags)

    @provide_session
    def _emit_pool_metrics(self, session: Session = NEW_SESSION) -> None:
        from airflow.models.pool import Pool

        with DebugTrace.start_span(span_name="emit_pool_metrics", component="SchedulerJobRunner") as span:
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

                span.set_attributes(
                    {
                        "category": "scheduler",
                        f"pool.open_slots.{pool_name}": slot_stats["open"],
                        f"pool.queued_slots.{pool_name}": slot_stats["queued"],
                        f"pool.running_slots.{pool_name}": slot_stats["running"],
                        f"pool.deferred_slots.{pool_name}": slot_stats["deferred"],
                        f"pool.scheduled_slots.{pool_name}": slot_stats["scheduled"],
                    }
                )

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
                    result = session.execute(
                        update(Job)
                        .where(
                            Job.job_type == "SchedulerJob",
                            Job.state == JobState.RUNNING,
                            Job.latest_heartbeat < (timezone.utcnow() - timedelta(seconds=timeout)),
                        )
                        .values(state=JobState.FAILED)
                    )
                    num_failed: int = getattr(result, "rowcount", 0)

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
                    tis_to_adopt_or_reset_query = with_row_locks(
                        query, of=TI, session=session, skip_locked=True
                    )
                    tis_to_adopt_or_reset: list[TaskInstance] = list(
                        session.scalars(tis_to_adopt_or_reset_query)
                    )

                    to_reset: list[TaskInstance] = []
                    exec_to_tis = self._executor_to_tis(tis_to_adopt_or_reset, session)
                    for executor, tis in exec_to_tis.items():
                        to_reset.extend(executor.try_adopt_task_instances(tis))

                    reset_tis_message = []
                    for ti in to_reset:
                        reset_tis_message.append(repr(ti))
                        # If we reset a TI, it will be eligible to be scheduled again.
                        # This can cause the scheduler to increase the try_number on the TI.
                        # Record the current try to TaskInstanceHistory first so users have an audit trail for
                        # the attempt that was abandoned.
                        ti.prepare_db_for_next_try(session=session)

                        ti.state = None
                        ti.queued_by_job_id = None
                        ti.external_executor_id = None
                        ti.clear_next_method_args()

                    for ti in set(tis_to_adopt_or_reset) - set(to_reset):
                        ti.queued_by_job_id = self.job.id
                        # If old ti from Airflow 2 and last_heartbeat_at is None, set last_heartbeat_at to now
                        if ti.last_heartbeat_at is None:
                            ti.last_heartbeat_at = timezone.utcnow()
                        # If old ti from Airflow 2 and dag_run.conf is None, set dag_run.conf to {}
                        if ti.dag_run.conf is None:
                            ti.dag_run.conf = {}

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
                result = session.execute(
                    update(TI)
                    .where(
                        TI.state == TaskInstanceState.DEFERRED,
                        TI.trigger_timeout < timezone.utcnow(),
                    )
                    .values(
                        state=TaskInstanceState.SCHEDULED,
                        next_method=TRIGGER_FAIL_REPR,
                        next_kwargs={"error": TriggerFailureReason.TRIGGER_TIMEOUT},
                        scheduled_dttm=timezone.utcnow(),
                        trigger_id=None,
                    )
                )
                num_timed_out_tasks = getattr(result, "rowcount", 0)
                if num_timed_out_tasks:
                    self.log.info("Timed out %i deferred tasks without fired triggers", num_timed_out_tasks)

    # [START find_and_purge_task_instances_without_heartbeats]
    def _find_and_purge_task_instances_without_heartbeats(self) -> None:
        """
        Find and purge task instances without heartbeats.

        Task instances that failed to heartbeat for too long, or
        have a no-longer-running LocalTaskJob will be failed by the scheduler.

        A TaskCallbackRequest is also created for the killed task instance to be
        handled by the DAG processor, and the executor is informed to no longer
        count the task instance as running when it calculates parallelism.
        """
        with create_session() as session:
            if task_instances_without_heartbeats := self._find_task_instances_without_heartbeats(
                session=session
            ):
                self._purge_task_instances_without_heartbeats(
                    task_instances_without_heartbeats, session=session
                )

    def _find_task_instances_without_heartbeats(self, *, session: Session) -> list[TI]:
        self.log.debug("Finding 'running' jobs without a recent heartbeat")
        limit_dttm = timezone.utcnow() - timedelta(seconds=self._task_instance_heartbeat_timeout_secs)
        asset_loader, alias_loader = _eager_load_dag_run_for_validation()
        task_instances_without_heartbeats = list(
            session.scalars(
                select(TI)
                .options(selectinload(TI.dag_model))
                .options(asset_loader)
                .options(alias_loader)
                .options(selectinload(TI.dag_version))
                .with_hint(TI, "USE INDEX (ti_state)", dialect_name="mysql")
                .join(DM, TI.dag_id == DM.dag_id)
                .where(
                    TI.state.in_((TaskInstanceState.RUNNING, TaskInstanceState.RESTARTING)),
                    TI.last_heartbeat_at < limit_dttm,
                )
                .where(TI.queued_by_job_id == self.job.id)
            )
        )
        if task_instances_without_heartbeats:
            self.log.warning(
                "Failing %s TIs without heartbeat after %s",
                len(task_instances_without_heartbeats),
                limit_dttm,
            )
        return list(task_instances_without_heartbeats)

    def _purge_task_instances_without_heartbeats(
        self, task_instances_without_heartbeats: list[TI], *, session: Session
    ) -> None:
        for ti in task_instances_without_heartbeats:
            task_instance_heartbeat_timeout_message_details = (
                self._generate_task_instance_heartbeat_timeout_message_details(ti)
            )
            if not ti.dag_version:
                # If old ti from Airflow 2 and dag_version is None, skip heartbeat timeout handling.
                self.log.warning(
                    "DAG Version not found for TaskInstance %s. Skipping heartbeat timeout handling.",
                    ti,
                )
                continue
            request = TaskCallbackRequest(
                filepath=ti.dag_model.relative_fileloc or "",
                bundle_name=ti.dag_version.bundle_name,
                bundle_version=ti.dag_run.bundle_version,
                ti=ti,
                msg=str(task_instance_heartbeat_timeout_message_details),
                context_from_server=TIRunContext(
                    dag_run=DRDataModel.model_validate(ti.dag_run, from_attributes=True),
                    max_tries=ti.max_tries,
                    variables=[],
                    connections=[],
                    xcom_keys_to_clear=[],
                ),
            )
            session.add(
                Log(
                    event="heartbeat timeout",
                    task_instance=ti.key,
                    extra=(
                        f"Task did not emit heartbeat within time limit ({self._task_instance_heartbeat_timeout_secs} "
                        "seconds) and will be terminated. "
                        "See https://airflow.apache.org/docs/apache-airflow/"
                        "stable/core-concepts/tasks.html#task-instance-heartbeat-timeout"
                    ),
                )
            )
            self.log.error(
                "Detected a task instance without a heartbeat: %s "
                "(See https://airflow.apache.org/docs/apache-airflow/"
                "stable/core-concepts/tasks.html#task-instance-heartbeat-timeout)",
                request,
            )
            self.job.executor.send_callback(request)
            if (executor := self._try_to_load_executor(ti, session)) is None:
                self.log.warning(
                    "Cannot clean up task instance without heartbeat %r with non-existent executor %s",
                    ti,
                    ti.executor,
                )
                continue
            executor.change_state(ti.key, TaskInstanceState.FAILED, remove_running=True)
            Stats.incr(
                "task_instances_without_heartbeats_killed", tags={"dag_id": ti.dag_id, "task_id": ti.task_id}
            )

    # [END find_and_purge_task_instances_without_heartbeats]

    @staticmethod
    def _generate_task_instance_heartbeat_timeout_message_details(ti: TI) -> dict[str, Any]:
        task_instance_heartbeat_timeout_message_details: dict[str, Any] = {
            "DAG Id": ti.dag_id,
            "Task Id": ti.task_id,
            "Run Id": ti.run_id,
        }

        if ti.map_index != -1:
            task_instance_heartbeat_timeout_message_details["Map Index"] = ti.map_index
        if ti.hostname:
            task_instance_heartbeat_timeout_message_details["Hostname"] = ti.hostname
        if ti.external_executor_id:
            task_instance_heartbeat_timeout_message_details["External Executor Id"] = ti.external_executor_id

        return task_instance_heartbeat_timeout_message_details

    @provide_session
    def _remove_unreferenced_triggers(self, *, session: Session = NEW_SESSION) -> None:
        """Remove triggers that are no longer used by anything."""
        session.execute(
            delete(Trigger)
            .where(
                Trigger.id.not_in(select(AssetWatcherModel.trigger_id)),
                Trigger.id.not_in(select(Callback.trigger_id)),
                Trigger.id.not_in(select(TaskInstance.trigger_id)),
            )
            .execution_options(synchronize_session="fetch")
        )

    @provide_session
    def _update_asset_orphanage(self, session: Session = NEW_SESSION) -> None:
        """
        Check assets orphanization and update their active entry.

        An orphaned asset is no longer referenced in any DAG schedule parameters,
        task outlets, or task inlets. Active assets (non-orphaned) have entries in
        AssetActive and must have unique names and URIs.

        :seealso: :meth:`AssetModelOperation.activate_assets_if_possible`.
        """
        # Group assets into orphaned=True and orphaned=False groups.
        orphaned = (
            (
                func.count(DagScheduleAssetReference.dag_id)
                + func.count(TaskOutletAssetReference.dag_id)
                + func.count(TaskInletAssetReference.dag_id)
            )
            == 0
        ).label("orphaned")
        asset_reference_query = session.execute(
            select(orphaned, AssetModel)
            .outerjoin(DagScheduleAssetReference)
            .outerjoin(TaskOutletAssetReference)
            .outerjoin(TaskInletAssetReference)
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
                    tuple_(AssetActive.name, AssetActive.uri).in_((a.name, a.uri) for a in assets)
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
                    tuple_(AssetActive.name, AssetActive.uri).in_((a.name, a.uri) for a in assets)
                )
            )
        )

        active_name_to_uri: dict[str, str] = {name: uri for name, uri in active_assets}
        active_uri_to_name: dict[str, str] = {uri: name for name, uri in active_assets}

        def _generate_warning_message(
            offending: AssetModel, attr: str, value: str
        ) -> Iterator[tuple[str, str]]:
            offending_references = itertools.chain(
                offending.scheduled_dags,
                offending.producing_tasks,
                offending.consuming_tasks,
            )
            for ref in offending_references:
                yield (
                    ref.dag_id,
                    (
                        "Cannot activate asset "
                        f'Asset(name="{offending.name}", uri="{offending.uri}", group="{offending.group}"); '
                        f"{attr} is already associated to {value!r}"
                    ),
                )

        def _activate_assets_generate_warnings() -> Iterator[tuple[str, str]]:
            incoming_name_to_uri: dict[str, str] = {}
            incoming_uri_to_name: dict[str, str] = {}
            for asset in assets:
                if (asset.name, asset.uri) in active_assets:
                    continue
                existing_uri = active_name_to_uri.get(asset.name) or incoming_name_to_uri.get(asset.name)
                if existing_uri is not None and existing_uri != asset.uri:
                    yield from _generate_warning_message(asset, "name", existing_uri)
                    continue
                existing_name = active_uri_to_name.get(asset.uri) or incoming_uri_to_name.get(asset.uri)
                if existing_name is not None and existing_name != asset.name:
                    yield from _generate_warning_message(asset, "uri", existing_name)
                    continue
                incoming_name_to_uri[asset.name] = asset.uri
                incoming_uri_to_name[asset.uri] = asset.name
                session.add(AssetActive.for_asset(asset))

        warnings_to_have = {
            dag_id: DagWarning(
                dag_id=dag_id,
                warning_type=DagWarningType.ASSET_CONFLICT,
                message="\n".join([message for _, message in group]),
            )
            for dag_id, group in groupby(
                sorted(_activate_assets_generate_warnings()), key=operator.itemgetter(0)
            )
        }

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
                    DagWarning.dag_id.in_(warnings_to_have),
                )
            )
        )
        for dag_id, warning in warnings_to_have.items():
            if dag_id in existing_warned_dag_ids:
                session.merge(warning)
                continue
            session.add(warning)
            existing_warned_dag_ids.add(warning.dag_id)

    def _executor_to_tis(
        self, tis: Iterable[TaskInstance], session
    ) -> dict[BaseExecutor, list[TaskInstance]]:
        """Organize TIs into lists per their respective executor."""
        _executor_to_tis: defaultdict[BaseExecutor, list[TaskInstance]] = defaultdict(list)
        for ti in tis:
            if executor_obj := self._try_to_load_executor(ti, session):
                _executor_to_tis[executor_obj].append(ti)

        return _executor_to_tis

    def _try_to_load_executor(self, ti: TaskInstance, session, team_name=NOTSET) -> BaseExecutor | None:
        """
        Try to load the given executor.

        In this context, we don't want to fail if the executor does not exist. Catch the exception and
        log to the user.

        :param ti: TaskInstance to load executor for
        :param session: Database session for queries
        :param team_name: Optional pre-resolved team name. If NOTSET and multi-team is enabled,
                         will query the database to resolve team name. None indicates global team.
        """
        executor = None
        if conf.getboolean("core", "multi_team"):
            # Use provided team_name if available, otherwise query the database
            if team_name is NOTSET:
                team_name = self._get_task_team_name(ti, session)
        else:
            team_name = None
        # Firstly, check if there is no executor set on the TaskInstance, if not, we need to fetch the default
        # (either globally or for the team)
        if ti.executor is None:
            if not team_name:
                # No team is specified, so just use the global default executor
                executor = self.job.executor
            else:
                # We do have a team, so we need to find the default executor for that team
                for _executor in self.job.executors:
                    # First executor that resolves should be the default for that team
                    if _executor.team_name == team_name:
                        executor = _executor
                        break
                else:
                    # No executor found for that team, fall back to global default
                    executor = self.job.executor
        else:
            # An executor is specified on the TaskInstance (as a str), so we need to find it in the list of executors
            for _executor in self.job.executors:
                if ti.executor in (_executor.name.alias, _executor.name.module_path):
                    # The executor must either match the team or be global (i.e. team_name is None)
                    if team_name and _executor.team_name == team_name or _executor.team_name is None:
                        executor = _executor

        if executor is not None:
            self.log.debug("Found executor %s for task %s (team: %s)", executor.name, ti, team_name)
        else:
            # This case should not happen unless some (as of now unknown) edge case occurs or direct DB
            # modification, since the DAG parser will validate the tasks in the DAG and ensure the executor
            # they request is available and if not, disallow the DAG to be scheduled.
            # Keeping this exception handling because this is a critical issue if we do somehow find
            # ourselves here and the user should get some feedback about that.
            self.log.warning("Executor, %s, was not found but a Task was configured to use it", ti.executor)

        return executor

    def _exceeds_max_active_runs(
        self,
        *,
        dag_model: DagModel,
        active_non_backfill_runs: int | None = None,
        session: Session,
    ):
        if active_non_backfill_runs is None:
            runs_dict = DagRun.active_runs_of_dags(
                dag_ids=[dag_model.dag_id],
                exclude_backfill=True,
                session=session,
            )
            active_non_backfill_runs = runs_dict.get(dag_model.dag_id, 0)
        exceeds = active_non_backfill_runs >= dag_model.max_active_runs
        return exceeds, active_non_backfill_runs


# Backcompat for older versions of task sdk import SchedulerDagBag from here
SchedulerDagBag = DBDagBag
