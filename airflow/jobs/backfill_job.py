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
#

import time
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

import attr
import pendulum
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.session import Session, make_transient
from tabulate import tabulate

from airflow import models
from airflow.exceptions import (
    AirflowException,
    BackfillUnfinished,
    DagConcurrencyLimitReached,
    NoAvailablePoolSlot,
    PoolNotFound,
    TaskConcurrencyLimitReached,
)
from airflow.executors import executor_constants
from airflow.jobs.base_job import BaseJob
from airflow.models import DAG, DagPickle
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import BACKFILL_QUEUED_DEPS
from airflow.timetables.base import DagRunInfo
from airflow.utils import helpers, timezone
from airflow.utils.configuration import conf as airflow_conf, tmp_configuration_copy
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from airflow.models.mappedoperator import MappedOperator


class BackfillJob(BaseJob):
    """
    A backfill job consists of a dag or subdag for a specific time range. It
    triggers a set of task instance runs, in the right order and lasts for
    as long as it takes for the set of task instance to be completed.
    """

    STATES_COUNT_AS_RUNNING = (State.RUNNING, State.QUEUED)

    __mapper_args__ = {'polymorphic_identity': 'BackfillJob'}

    @attr.define
    class _DagRunTaskStatus:
        """
        Internal status of the backfill job. This class is intended to be instantiated
        only within a BackfillJob instance and will track the execution of tasks,
        e.g. running, skipped, succeeded, failed, etc. Information about the dag runs
        related to the backfill job are also being tracked in this structure,
        .e.g finished runs, etc. Any other status related information related to the
        execution of dag runs / tasks can be included in this structure since it makes
        it easier to pass it around.

        :param to_run: Tasks to run in the backfill
        :param running: Maps running task instance key to task instance object
        :param skipped: Tasks that have been skipped
        :param succeeded: Tasks that have succeeded so far
        :param failed: Tasks that have failed
        :param not_ready: Tasks not ready for execution
        :param deadlocked: Deadlocked tasks
        :param active_runs: Active dag runs at a certain point in time
        :param executed_dag_run_dates: Datetime objects for the executed dag runs
        :param finished_runs: Number of finished runs so far
        :param total_runs: Number of total dag runs able to run
        """

        to_run: Dict[TaskInstanceKey, TaskInstance] = attr.ib(factory=dict)
        running: Dict[TaskInstanceKey, TaskInstance] = attr.ib(factory=dict)
        skipped: Set[TaskInstanceKey] = attr.ib(factory=set)
        succeeded: Set[TaskInstanceKey] = attr.ib(factory=set)
        failed: Set[TaskInstanceKey] = attr.ib(factory=set)
        not_ready: Set[TaskInstanceKey] = attr.ib(factory=set)
        deadlocked: Set[TaskInstance] = attr.ib(factory=set)
        active_runs: List[DagRun] = attr.ib(factory=list)
        executed_dag_run_dates: Set[pendulum.DateTime] = attr.ib(factory=set)
        finished_runs: int = 0
        total_runs: int = 0

    def __init__(
        self,
        dag: DAG,
        start_date=None,
        end_date=None,
        mark_success=False,
        donot_pickle=False,
        ignore_first_depends_on_past=False,
        ignore_task_deps=False,
        pool=None,
        delay_on_limit_secs=1.0,
        verbose=False,
        conf=None,
        rerun_failed_tasks=False,
        run_backwards=False,
        run_at_least_once=False,
        continue_on_failures=False,
        *args,
        **kwargs,
    ):
        """
        :param dag: DAG object.
        :param start_date: start date for the backfill date range.
        :param end_date: end date for the backfill date range.
        :param mark_success: flag whether to mark the task auto success.
        :param donot_pickle: whether pickle
        :param ignore_first_depends_on_past: whether to ignore depend on past
        :param ignore_task_deps: whether to ignore the task dependency
        :param pool: pool to backfill
        :param delay_on_limit_secs:
        :param verbose:
        :param conf: a dictionary which user could pass k-v pairs for backfill
        :param rerun_failed_tasks: flag to whether to
                                   auto rerun the failed task in backfill
        :param run_backwards: Whether to process the dates from most to least recent
        :param run_at_least_once: If true, always run the DAG at least once even
            if no logical run exists within the time range.
        :param args:
        :param kwargs:
        """
        self.dag = dag
        self.dag_id = dag.dag_id
        self.bf_start_date = start_date
        self.bf_end_date = end_date
        self.mark_success = mark_success
        self.donot_pickle = donot_pickle
        self.ignore_first_depends_on_past = ignore_first_depends_on_past
        self.ignore_task_deps = ignore_task_deps
        self.pool = pool
        self.delay_on_limit_secs = delay_on_limit_secs
        self.verbose = verbose
        self.conf = conf
        self.rerun_failed_tasks = rerun_failed_tasks
        self.run_backwards = run_backwards
        self.run_at_least_once = run_at_least_once
        self.continue_on_failures = continue_on_failures
        super().__init__(*args, **kwargs)

    def _update_counters(self, ti_status, session=None):
        """
        Updates the counters per state of the tasks that were running. Can re-add
        to tasks to run in case required.

        :param ti_status: the internal status of the backfill job tasks
        """
        tis_to_be_scheduled = []
        refreshed_tis = []
        TI = TaskInstance

        filter_for_tis = TI.filter_for_tis(list(ti_status.running.values()))
        if filter_for_tis is not None:
            refreshed_tis = session.query(TI).filter(filter_for_tis).all()

        for ti in refreshed_tis:
            # Here we remake the key by subtracting 1 to match in memory information
            reduced_key = ti.key.reduced
            if ti.state == TaskInstanceState.SUCCESS:
                ti_status.succeeded.add(reduced_key)
                self.log.debug("Task instance %s succeeded. Don't rerun.", ti)
                ti_status.running.pop(reduced_key)
                continue
            if ti.state == TaskInstanceState.SKIPPED:
                ti_status.skipped.add(reduced_key)
                self.log.debug("Task instance %s skipped. Don't rerun.", ti)
                ti_status.running.pop(reduced_key)
                continue
            if ti.state == TaskInstanceState.FAILED:
                self.log.error("Task instance %s failed", ti)
                ti_status.failed.add(reduced_key)
                ti_status.running.pop(reduced_key)
                continue
            # special case: if the task needs to run again put it back
            if ti.state == TaskInstanceState.UP_FOR_RETRY:
                self.log.warning("Task instance %s is up for retry", ti)
                ti_status.running.pop(reduced_key)
                ti_status.to_run[ti.key] = ti
            # special case: if the task needs to be rescheduled put it back
            elif ti.state == TaskInstanceState.UP_FOR_RESCHEDULE:
                self.log.warning("Task instance %s is up for reschedule", ti)
                # During handling of reschedule state in ti._handle_reschedule, try number is reduced
                # by one, so we should not use reduced_key to avoid key error
                ti_status.running.pop(ti.key)
                ti_status.to_run[ti.key] = ti
            # special case: The state of the task can be set to NONE by the task itself
            # when it reaches concurrency limits. It could also happen when the state
            # is changed externally, e.g. by clearing tasks from the ui. We need to cover
            # for that as otherwise those tasks would fall outside of the scope of
            # the backfill suddenly.
            elif ti.state == State.NONE:
                self.log.warning(
                    "FIXME: task instance %s state was set to none externally or "
                    "reaching concurrency limits. Re-adding task to queue.",
                    ti,
                )
                tis_to_be_scheduled.append(ti)
                ti_status.running.pop(reduced_key)
                ti_status.to_run[ti.key] = ti

        # Batch schedule of task instances
        if tis_to_be_scheduled:
            filter_for_tis = TI.filter_for_tis(tis_to_be_scheduled)
            session.query(TI).filter(filter_for_tis).update(
                values={TI.state: TaskInstanceState.SCHEDULED}, synchronize_session=False
            )
            session.flush()

    def _manage_executor_state(
        self, running, session
    ) -> Iterator[Tuple["MappedOperator", str, Sequence[TaskInstance], int]]:
        """
        Checks if the executor agrees with the state of task instances
        that are running.

        Expands downstream mapped tasks when necessary

        :param running: dict of key, task to verify
        :return: An iterable of expanded TaskInstance per MappedTask
        """
        executor = self.executor

        # TODO: query all instead of refresh from db
        for key, value in list(executor.get_event_buffer().items()):
            state, info = value
            if key not in running:
                self.log.warning("%s state %s not in running=%s", key, state, running.values())
                continue

            ti = running[key]
            ti.refresh_from_db()

            self.log.debug("Executor state: %s task %s", state, ti)

            if (
                state in (TaskInstanceState.FAILED, TaskInstanceState.SUCCESS)
                and ti.state in self.STATES_COUNT_AS_RUNNING
            ):
                msg = (
                    f"Executor reports task instance {ti} finished ({state}) although the task says its "
                    f"{ti.state}. Was the task killed externally? Info: {info}"
                )
                self.log.error(msg)
                ti.handle_failure(error=msg)
                continue
            if ti.state not in self.STATES_COUNT_AS_RUNNING:
                # Don't use ti.task; if this task is mapped, that attribute
                # would hold the unmapped task. We need to original task here.
                for node in self.dag.get_task(ti.task_id, include_subdags=True).iter_mapped_dependants():
                    new_tis, num_mapped_tis = node.expand_mapped_task(ti.run_id, session=session)
                    yield node, ti.run_id, new_tis, num_mapped_tis

    @provide_session
    def _get_dag_run(self, dagrun_info: DagRunInfo, dag: DAG, session: Session = None):
        """
        Returns a dag run for the given run date, which will be matched to an existing
        dag run if available or create a new dag run otherwise. If the max_active_runs
        limit is reached, this function will return None.

        :param dagrun_info: Schedule information for the dag run
        :param dag: DAG
        :param session: the database session object
        :return: a DagRun in state RUNNING or None
        """
        run_date = dagrun_info.logical_date

        # consider max_active_runs but ignore when running subdags
        respect_dag_max_active_limit = bool(dag.timetable.can_run and not dag.is_subdag)

        current_active_dag_count = dag.get_num_active_runs(external_trigger=False)

        # check if we are scheduling on top of a already existing dag_run
        # we could find a "scheduled" run instead of a "backfill"
        runs = DagRun.find(dag_id=dag.dag_id, execution_date=run_date, session=session)
        run: Optional[DagRun]
        if runs:
            run = runs[0]
            if run.state == DagRunState.RUNNING:
                respect_dag_max_active_limit = False
            # Fixes --conf overwrite for backfills with already existing DagRuns
            run.conf = self.conf or {}
            # start_date is cleared for existing DagRuns
            run.start_date = timezone.utcnow()
        else:
            run = None

        # enforce max_active_runs limit for dag, special cases already
        # handled by respect_dag_max_active_limit
        if respect_dag_max_active_limit and current_active_dag_count >= dag.max_active_runs:
            return None

        run = run or dag.create_dagrun(
            execution_date=run_date,
            data_interval=dagrun_info.data_interval,
            start_date=timezone.utcnow(),
            state=DagRunState.RUNNING,
            external_trigger=False,
            session=session,
            conf=self.conf,
            run_type=DagRunType.BACKFILL_JOB,
            creating_job_id=self.id,
        )

        # set required transient field
        run.dag = dag

        # explicitly mark as backfill and running
        run.state = DagRunState.RUNNING
        run.run_type = DagRunType.BACKFILL_JOB
        run.verify_integrity(session=session)
        return run

    @provide_session
    def _task_instances_for_dag_run(self, dag, dag_run, session=None):
        """
        Returns a map of task instance key to task instance object for the tasks to
        run in the given dag run.

        :param dag_run: the dag run to get the tasks from
        :param session: the database session object
        """
        tasks_to_run = {}

        if dag_run is None:
            return tasks_to_run

        # check if we have orphaned tasks
        self.reset_state_for_orphaned_tasks(filter_by_dag_run=dag_run, session=session)

        # for some reason if we don't refresh the reference to run is lost
        dag_run.refresh_from_db()
        make_transient(dag_run)

        dag_run.dag = dag
        info = dag_run.task_instance_scheduling_decisions(session=session)
        schedulable_tis = info.schedulable_tis
        try:
            for ti in dag_run.get_task_instances(session=session):
                if ti in schedulable_tis:
                    ti.set_state(TaskInstanceState.SCHEDULED)
                if ti.state != TaskInstanceState.REMOVED:
                    tasks_to_run[ti.key] = ti
            session.commit()
        except Exception:
            session.rollback()
            raise
        return tasks_to_run

    def _log_progress(self, ti_status):
        self.log.info(
            '[backfill progress] | finished run %s of %s | tasks waiting: %s | succeeded: %s | '
            'running: %s | failed: %s | skipped: %s | deadlocked: %s | not ready: %s',
            ti_status.finished_runs,
            ti_status.total_runs,
            len(ti_status.to_run),
            len(ti_status.succeeded),
            len(ti_status.running),
            len(ti_status.failed),
            len(ti_status.skipped),
            len(ti_status.deadlocked),
            len(ti_status.not_ready),
        )

        self.log.debug("Finished dag run loop iteration. Remaining tasks %s", ti_status.to_run.values())

    @provide_session
    def _process_backfill_task_instances(
        self,
        ti_status,
        executor,
        pickle_id,
        start_date=None,
        session=None,
    ):
        """
        Process a set of task instances from a set of dag runs. Special handling is done
        to account for different task instance states that could be present when running
        them in a backfill process.

        :param ti_status: the internal status of the job
        :param executor: the executor to run the task instances
        :param pickle_id: the pickle_id if dag is pickled, None otherwise
        :param start_date: the start date of the backfill job
        :param session: the current session object
        :return: the list of execution_dates for the finished dag runs
        :rtype: list
        """
        executed_run_dates = []

        is_unit_test = airflow_conf.getboolean('core', 'unit_test_mode')

        while (len(ti_status.to_run) > 0 or len(ti_status.running) > 0) and len(ti_status.deadlocked) == 0:
            self.log.debug("*** Clearing out not_ready list ***")
            ti_status.not_ready.clear()

            # we need to execute the tasks bottom to top
            # or leaf to root, as otherwise tasks might be
            # determined deadlocked while they are actually
            # waiting for their upstream to finish
            def _per_task_process(key, ti: TaskInstance, session=None):
                ti.refresh_from_db(lock_for_update=True, session=session)

                task = self.dag.get_task(ti.task_id, include_subdags=True)
                ti.task = task

                self.log.debug("Task instance to run %s state %s", ti, ti.state)

                # The task was already marked successful or skipped by a
                # different Job. Don't rerun it.
                if ti.state == TaskInstanceState.SUCCESS:
                    ti_status.succeeded.add(key)
                    self.log.debug("Task instance %s succeeded. Don't rerun.", ti)
                    ti_status.to_run.pop(key)
                    if key in ti_status.running:
                        ti_status.running.pop(key)
                    return
                elif ti.state == TaskInstanceState.SKIPPED:
                    ti_status.skipped.add(key)
                    self.log.debug("Task instance %s skipped. Don't rerun.", ti)
                    ti_status.to_run.pop(key)
                    if key in ti_status.running:
                        ti_status.running.pop(key)
                    return

                if self.rerun_failed_tasks:
                    # Rerun failed tasks or upstreamed failed tasks
                    if ti.state in (TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED):
                        self.log.error("Task instance %s with state %s", ti, ti.state)
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        # Reset the failed task in backfill to scheduled state
                        ti.set_state(TaskInstanceState.SCHEDULED, session=session)
                else:
                    # Default behaviour which works for subdag.
                    if ti.state in (TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED):
                        self.log.error("Task instance %s with state %s", ti, ti.state)
                        ti_status.failed.add(key)
                        ti_status.to_run.pop(key)
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        return

                if self.ignore_first_depends_on_past:
                    dagrun = ti.get_dagrun(session=session)
                    ignore_depends_on_past = dagrun.execution_date == (start_date or ti.start_date)
                else:
                    ignore_depends_on_past = False

                backfill_context = DepContext(
                    deps=BACKFILL_QUEUED_DEPS,
                    ignore_depends_on_past=ignore_depends_on_past,
                    ignore_task_deps=self.ignore_task_deps,
                    flag_upstream_failed=True,
                )

                # Is the task runnable? -- then run it
                # the dependency checker can change states of tis
                if ti.are_dependencies_met(
                    dep_context=backfill_context, session=session, verbose=self.verbose
                ):
                    if executor.has_task(ti):
                        self.log.debug("Task Instance %s already in executor waiting for queue to clear", ti)
                    else:
                        self.log.debug('Sending %s to executor', ti)
                        # Skip scheduled state, we are executing immediately
                        ti.state = TaskInstanceState.QUEUED
                        ti.queued_by_job_id = self.id
                        ti.queued_dttm = timezone.utcnow()
                        session.merge(ti)
                        try:
                            session.commit()
                        except OperationalError:
                            self.log.exception("Failed to commit task state change due to operational error")
                            session.rollback()
                            # early exit so the outer loop can retry
                            return

                        cfg_path = None
                        if self.executor_class in (
                            executor_constants.LOCAL_EXECUTOR,
                            executor_constants.SEQUENTIAL_EXECUTOR,
                        ):
                            cfg_path = tmp_configuration_copy()

                        executor.queue_task_instance(
                            ti,
                            mark_success=self.mark_success,
                            pickle_id=pickle_id,
                            ignore_task_deps=self.ignore_task_deps,
                            ignore_depends_on_past=ignore_depends_on_past,
                            pool=self.pool,
                            cfg_path=cfg_path,
                        )
                        ti_status.running[key] = ti
                        ti_status.to_run.pop(key)
                    return

                if ti.state == TaskInstanceState.UPSTREAM_FAILED:
                    self.log.error("Task instance %s upstream failed", ti)
                    ti_status.failed.add(key)
                    ti_status.to_run.pop(key)
                    if key in ti_status.running:
                        ti_status.running.pop(key)
                    return

                # special case
                if ti.state == TaskInstanceState.UP_FOR_RETRY:
                    self.log.debug("Task instance %s retry period not expired yet", ti)
                    if key in ti_status.running:
                        ti_status.running.pop(key)
                    ti_status.to_run[key] = ti
                    return

                # special case
                if ti.state == TaskInstanceState.UP_FOR_RESCHEDULE:
                    self.log.debug("Task instance %s reschedule period not expired yet", ti)
                    if key in ti_status.running:
                        ti_status.running.pop(key)
                    ti_status.to_run[key] = ti
                    return

                # all remaining tasks
                self.log.debug('Adding %s to not_ready', ti)
                ti_status.not_ready.add(key)

            try:
                for task in self.dag.topological_sort(include_subdag_tasks=True):
                    for key, ti in list(ti_status.to_run.items()):
                        if task.task_id != ti.task_id:
                            continue

                        pool = session.query(models.Pool).filter(models.Pool.pool == task.pool).first()
                        if not pool:
                            raise PoolNotFound(f'Unknown pool: {task.pool}')

                        open_slots = pool.open_slots(session=session)
                        if open_slots <= 0:
                            raise NoAvailablePoolSlot(
                                f"Not scheduling since there are {open_slots} open slots in pool {task.pool}"
                            )

                        num_running_task_instances_in_dag = DAG.get_num_task_instances(
                            self.dag_id,
                            states=self.STATES_COUNT_AS_RUNNING,
                            session=session,
                        )

                        if num_running_task_instances_in_dag >= self.dag.max_active_tasks:
                            raise DagConcurrencyLimitReached(
                                "Not scheduling since DAG max_active_tasks limit is reached."
                            )

                        if task.max_active_tis_per_dag:
                            num_running_task_instances_in_task = DAG.get_num_task_instances(
                                dag_id=self.dag_id,
                                task_ids=[task.task_id],
                                states=self.STATES_COUNT_AS_RUNNING,
                                session=session,
                            )

                            if num_running_task_instances_in_task >= task.max_active_tis_per_dag:
                                raise TaskConcurrencyLimitReached(
                                    "Not scheduling since Task concurrency limit is reached."
                                )

                        _per_task_process(key, ti, session)
                        session.commit()
            except (NoAvailablePoolSlot, DagConcurrencyLimitReached, TaskConcurrencyLimitReached) as e:
                self.log.debug(e)

            self.heartbeat(only_if_necessary=is_unit_test)
            # execute the tasks in the queue
            executor.heartbeat()

            # If the set of tasks that aren't ready ever equals the set of
            # tasks to run and there are no running tasks then the backfill
            # is deadlocked
            if (
                ti_status.not_ready
                and ti_status.not_ready == set(ti_status.to_run)
                and len(ti_status.running) == 0
            ):
                self.log.warning("Deadlock discovered for ti_status.to_run=%s", ti_status.to_run.values())
                ti_status.deadlocked.update(ti_status.to_run.values())
                ti_status.to_run.clear()

            # check executor state -- and expand any mapped TIs
            for node, run_id, new_mapped_tis, max_map_index in self._manage_executor_state(
                ti_status.running, session
            ):

                def to_keep(key: TaskInstanceKey) -> bool:
                    if key.dag_id != node.dag_id or key.task_id != node.task_id or key.run_id != run_id:
                        # For another Dag/Task/Run -- don't remove
                        return True
                    return 0 <= key.map_index <= max_map_index

                # remove the old unmapped TIs for node -- they have been replaced with the mapped TIs
                ti_status.to_run = {key: ti for (key, ti) in ti_status.to_run.items() if to_keep(key)}

                ti_status.to_run.update({ti.key: ti for ti in new_mapped_tis})

                for new_ti in new_mapped_tis:
                    new_ti.set_state(TaskInstanceState.SCHEDULED, session=session)

            # update the task counters
            self._update_counters(ti_status=ti_status, session=session)
            session.commit()

            # update dag run state
            _dag_runs = ti_status.active_runs[:]
            for run in _dag_runs:
                run.update_state(session=session)
                if run.state in State.finished:
                    ti_status.finished_runs += 1
                    ti_status.active_runs.remove(run)
                    executed_run_dates.append(run.execution_date)

            self._log_progress(ti_status)
            session.commit()

        # return updated status
        return executed_run_dates

    @provide_session
    def _collect_errors(self, ti_status: _DagRunTaskStatus, session=None):
        def tabulate_ti_keys_set(ti_keys: Iterable[TaskInstanceKey]) -> str:
            # Sorting by execution date first
            sorted_ti_keys: Any = sorted(
                ti_keys,
                key=lambda ti_key: (
                    ti_key.run_id,
                    ti_key.dag_id,
                    ti_key.task_id,
                    ti_key.map_index,
                    ti_key.try_number,
                ),
            )

            if all(key.map_index == -1 for key in ti_keys):
                headers = ["DAG ID", "Task ID", "Run ID", "Try number"]
                sorted_ti_keys = map(lambda k: k[0:4], sorted_ti_keys)
            else:
                headers = ["DAG ID", "Task ID", "Run ID", "Map Index", "Try number"]

            return tabulate(sorted_ti_keys, headers=headers)

        err = ''
        if ti_status.failed:
            err += "Some task instances failed:\n"
            err += tabulate_ti_keys_set(ti_status.failed)
        if ti_status.deadlocked:
            err += 'BackfillJob is deadlocked.'
            deadlocked_depends_on_past = any(
                t.are_dependencies_met(
                    dep_context=DepContext(ignore_depends_on_past=False),
                    session=session,
                    verbose=self.verbose,
                )
                != t.are_dependencies_met(
                    dep_context=DepContext(ignore_depends_on_past=True), session=session, verbose=self.verbose
                )
                for t in ti_status.deadlocked
            )
            if deadlocked_depends_on_past:
                err += (
                    'Some of the deadlocked tasks were unable to run because '
                    'of "depends_on_past" relationships. Try running the '
                    'backfill with the option '
                    '"ignore_first_depends_on_past=True" or passing "-I" at '
                    'the command line.'
                )
            err += '\nThese tasks have succeeded:\n'
            err += tabulate_ti_keys_set(ti_status.succeeded)
            err += '\n\nThese tasks are running:\n'
            err += tabulate_ti_keys_set(ti_status.running)
            err += '\n\nThese tasks have failed:\n'
            err += tabulate_ti_keys_set(ti_status.failed)
            err += '\n\nThese tasks are skipped:\n'
            err += tabulate_ti_keys_set(ti_status.skipped)
            err += '\n\nThese tasks are deadlocked:\n'
            err += tabulate_ti_keys_set([ti.key for ti in ti_status.deadlocked])

        return err

    def _get_dag_with_subdags(self) -> List[DAG]:
        return [self.dag] + self.dag.subdags

    @provide_session
    def _execute_dagruns(self, dagrun_infos, ti_status, executor, pickle_id, start_date, session=None):
        """
        Computes the dag runs and their respective task instances for
        the given run dates and executes the task instances.
        Returns a list of execution dates of the dag runs that were executed.

        :param dagrun_infos: Schedule information for dag runs
        :param ti_status: internal BackfillJob status structure to tis track progress
        :param executor: the executor to use, it must be previously started
        :param pickle_id: numeric id of the pickled dag, None if not pickled
        :param start_date: backfill start date
        :param session: the current session object
        """
        for dagrun_info in dagrun_infos:
            for dag in self._get_dag_with_subdags():
                dag_run = self._get_dag_run(dagrun_info, dag, session=session)
                tis_map = self._task_instances_for_dag_run(dag, dag_run, session=session)
                if dag_run is None:
                    continue

                ti_status.active_runs.append(dag_run)
                ti_status.to_run.update(tis_map or {})

        processed_dag_run_dates = self._process_backfill_task_instances(
            ti_status=ti_status,
            executor=executor,
            pickle_id=pickle_id,
            start_date=start_date,
            session=session,
        )

        ti_status.executed_dag_run_dates.update(processed_dag_run_dates)

    @provide_session
    def _set_unfinished_dag_runs_to_failed(self, dag_runs, session=None):
        """
        Go through the dag_runs and update the state based on the task_instance state.
        Then set DAG runs that are not finished to failed.

        :param dag_runs: DAG runs
        :param session: session
        :return: None
        """
        for dag_run in dag_runs:
            dag_run.update_state()
            if dag_run.state not in State.finished:
                dag_run.set_state(DagRunState.FAILED)
            session.merge(dag_run)

    @provide_session
    def _execute(self, session=None):
        """
        Initializes all components required to run a dag for a specified date range and
        calls helper method to execute the tasks.
        """
        ti_status = BackfillJob._DagRunTaskStatus()

        start_date = self.bf_start_date

        # Get DagRun schedule between the start/end dates, which will turn into dag runs.
        dagrun_start_date = timezone.coerce_datetime(start_date)
        if self.bf_end_date is None:
            dagrun_end_date = pendulum.now(timezone.utc)
        else:
            dagrun_end_date = pendulum.instance(self.bf_end_date)
        dagrun_infos = list(self.dag.iter_dagrun_infos_between(dagrun_start_date, dagrun_end_date))
        if self.run_backwards:
            tasks_that_depend_on_past = [t.task_id for t in self.dag.task_dict.values() if t.depends_on_past]
            if tasks_that_depend_on_past:
                raise AirflowException(
                    f'You cannot backfill backwards because one or more '
                    f'tasks depend_on_past: {",".join(tasks_that_depend_on_past)}'
                )
            dagrun_infos = dagrun_infos[::-1]

        if not dagrun_infos:
            if not self.run_at_least_once:
                self.log.info("No run dates were found for the given dates and dag interval.")
                return
            dagrun_infos = [DagRunInfo.interval(dagrun_start_date, dagrun_end_date)]

        dag_with_subdags_ids = [d.dag_id for d in self._get_dag_with_subdags()]
        running_dagruns = DagRun.find(
            dag_id=dag_with_subdags_ids,
            execution_start_date=self.bf_start_date,
            execution_end_date=self.bf_end_date,
            no_backfills=True,
            state=DagRunState.RUNNING,
        )

        if running_dagruns:
            for run in running_dagruns:
                self.log.error(
                    "Backfill cannot be created for DagRun %s in %s, as there's already %s in a RUNNING "
                    "state.",
                    run.run_id,
                    run.execution_date.strftime("%Y-%m-%dT%H:%M:%S"),
                    run.run_type,
                )
            self.log.error(
                "Changing DagRun into BACKFILL would cause scheduler to lose track of executing "
                "tasks. Not changing DagRun type into BACKFILL, and trying insert another DagRun into "
                "database would cause database constraint violation for dag_id + execution_date "
                "combination. Please adjust backfill dates or wait for this DagRun to finish.",
            )
            return
        # picklin'
        pickle_id = None

        if not self.donot_pickle and self.executor_class not in (
            executor_constants.LOCAL_EXECUTOR,
            executor_constants.SEQUENTIAL_EXECUTOR,
            executor_constants.DASK_EXECUTOR,
        ):
            pickle = DagPickle(self.dag)
            session.add(pickle)
            session.commit()
            pickle_id = pickle.id

        executor = self.executor
        executor.job_id = "backfill"
        executor.start()

        ti_status.total_runs = len(dagrun_infos)  # total dag runs in backfill

        try:
            remaining_dates = ti_status.total_runs
            while remaining_dates > 0:
                dagrun_infos_to_process = [
                    dagrun_info
                    for dagrun_info in dagrun_infos
                    if dagrun_info.logical_date not in ti_status.executed_dag_run_dates
                ]
                self._execute_dagruns(
                    dagrun_infos=dagrun_infos_to_process,
                    ti_status=ti_status,
                    executor=executor,
                    pickle_id=pickle_id,
                    start_date=start_date,
                    session=session,
                )

                remaining_dates = ti_status.total_runs - len(ti_status.executed_dag_run_dates)
                err = self._collect_errors(ti_status=ti_status, session=session)
                if err:
                    if not self.continue_on_failures or ti_status.deadlocked:
                        raise BackfillUnfinished(err, ti_status)

                if remaining_dates > 0:
                    self.log.info(
                        "max_active_runs limit for dag %s has been reached "
                        " - waiting for other dag runs to finish",
                        self.dag_id,
                    )
                    time.sleep(self.delay_on_limit_secs)
        except (KeyboardInterrupt, SystemExit):
            self.log.warning("Backfill terminated by user.")

            # TODO: we will need to terminate running task instances and set the
            # state to failed.
            self._set_unfinished_dag_runs_to_failed(ti_status.active_runs)
        finally:
            session.commit()
            executor.end()

        self.log.info("Backfill done for DAG %s. Exiting.", self.dag)

    @provide_session
    def reset_state_for_orphaned_tasks(self, filter_by_dag_run=None, session=None):
        """
        This function checks if there are any tasks in the dagrun (or all) that
        have a schedule or queued states but are not known by the executor. If
        it finds those it will reset the state to None so they will get picked
        up again.  The batch option is for performance reasons as the queries
        are made in sequence.

        :param filter_by_dag_run: the dag_run we want to process, None if all
        :return: the number of TIs reset
        :rtype: int
        """
        queued_tis = self.executor.queued_tasks
        # also consider running as the state might not have changed in the db yet
        running_tis = self.executor.running

        # Can't use an update here since it doesn't support joins.
        resettable_states = [TaskInstanceState.SCHEDULED, TaskInstanceState.QUEUED]
        if filter_by_dag_run is None:
            resettable_tis = (
                session.query(TaskInstance)
                .join(TaskInstance.dag_run)
                .filter(
                    DagRun.state == DagRunState.RUNNING,
                    DagRun.run_type != DagRunType.BACKFILL_JOB,
                    TaskInstance.state.in_(resettable_states),
                )
            ).all()
        else:
            resettable_tis = filter_by_dag_run.get_task_instances(state=resettable_states, session=session)

        tis_to_reset = [ti for ti in resettable_tis if ti.key not in queued_tis and ti.key not in running_tis]
        if not tis_to_reset:
            return 0

        def query(result, items):
            if not items:
                return result

            filter_for_tis = TaskInstance.filter_for_tis(items)
            reset_tis = (
                session.query(TaskInstance)
                .filter(filter_for_tis, TaskInstance.state.in_(resettable_states))
                .with_for_update()
                .all()
            )

            for ti in reset_tis:
                ti.state = State.NONE
                session.merge(ti)

            return result + reset_tis

        reset_tis = helpers.reduce_in_chunks(query, tis_to_reset, [], self.max_tis_per_query)

        task_instance_str = '\n\t'.join(repr(x) for x in reset_tis)
        session.flush()

        self.log.info("Reset the following %s TaskInstances:\n\t%s", len(reset_tis), task_instance_str)
        return len(reset_tis)
