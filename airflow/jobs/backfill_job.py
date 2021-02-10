# -*- coding: utf-8 -*-
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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import time
from collections import OrderedDict

from sqlalchemy.orm.session import make_transient
from tabulate import tabulate

from airflow import executors, models
from airflow.exceptions import (
    AirflowException,
    DagConcurrencyLimitReached,
    NoAvailablePoolSlot,
    PoolNotFound,
    TaskConcurrencyLimitReached,
)
from airflow.models import DAG, DagPickle, DagRun
from airflow.ti_deps.dep_context import DepContext, BACKFILL_QUEUED_DEPS
from airflow.utils import timezone
from airflow.utils.configuration import tmp_configuration_copy
from airflow.utils.db import provide_session
from airflow.jobs.base_job import BaseJob
from airflow.utils.state import State


class BackfillJob(BaseJob):
    """
    A backfill job consists of a dag or subdag for a specific time range. It
    triggers a set of task instance runs, in the right order and lasts for
    as long as it takes for the set of task instance to be completed.
    """
    ID_PREFIX = 'backfill_'
    ID_FORMAT_PREFIX = ID_PREFIX + '{0}'
    STATES_COUNT_AS_RUNNING = (State.RUNNING, State.QUEUED)

    __mapper_args__ = {
        'polymorphic_identity': 'BackfillJob'
    }

    class _DagRunTaskStatus:
        """
        Internal status of the backfill job. This class is intended to be instantiated
        only within a BackfillJob instance and will track the execution of tasks,
        e.g. running, skipped, succeeded, failed, etc. Information about the dag runs
        related to the backfill job are also being tracked in this structure,
        .e.g finished runs, etc. Any other status related information related to the
        execution of dag runs / tasks can be included in this structure since it makes
        it easier to pass it around.
        """
        # TODO(edgarRd): AIRFLOW-1444: Add consistency check on counts
        def __init__(self,
                     to_run=None,
                     running=None,
                     skipped=None,
                     succeeded=None,
                     failed=None,
                     not_ready=None,
                     deadlocked=None,
                     active_runs=None,
                     executed_dag_run_dates=None,
                     finished_runs=0,
                     total_runs=0,
                     ):
            """
            :param to_run: Tasks to run in the backfill
            :type to_run: dict[tuple[TaskInstanceKeyType], airflow.models.TaskInstance]
            :param running: Maps running task instance key to task instance object
            :type running: dict[tuple[TaskInstanceKeyType], airflow.models.TaskInstance]
            :param skipped: Tasks that have been skipped
            :type skipped: set[tuple[TaskInstanceKeyType]]
            :param succeeded: Tasks that have succeeded so far
            :type succeeded: set[tuple[TaskInstanceKeyType]]
            :param failed: Tasks that have failed
            :type failed: set[tuple[TaskInstanceKeyType]]
            :param not_ready: Tasks not ready for execution
            :type not_ready: set[tuple[TaskInstanceKeyType]]
            :param deadlocked: Deadlocked tasks
            :type deadlocked: set[airflow.models.TaskInstance]
            :param active_runs: Active dag runs at a certain point in time
            :type active_runs: list[DagRun]
            :param executed_dag_run_dates: Datetime objects for the executed dag runs
            :type executed_dag_run_dates: set[datetime.datetime]
            :param finished_runs: Number of finished runs so far
            :type finished_runs: int
            :param total_runs: Number of total dag runs able to run
            :type total_runs: int
            """
            self.to_run = to_run or OrderedDict()
            self.running = running or dict()
            self.skipped = skipped or set()
            self.succeeded = succeeded or set()
            self.failed = failed or set()
            self.not_ready = not_ready or set()
            self.deadlocked = deadlocked or set()
            self.active_runs = active_runs or list()
            self.executed_dag_run_dates = executed_dag_run_dates or set()
            self.finished_runs = finished_runs
            self.total_runs = total_runs

    def __init__(
            self,
            dag,
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
            *args, **kwargs):
        """
        :param dag: DAG object.
        :type dag: airflow.models.DAG
        :param start_date: start date for the backfill date range.
        :type start_date: datetime.datetime
        :param end_date: end date for the backfill date range.
        :type end_date: datetime.datetime
        :param mark_success: flag whether to mark the task auto success.
        :type mark_success: bool
        :param donot_pickle: whether pickle
        :type donot_pickle: bool
        :param ignore_first_depends_on_past: whether to ignore depend on past
        :type ignore_first_depends_on_past: bool
        :param ignore_task_deps: whether to ignore the task dependency
        :type ignore_task_deps: bool
        :param pool: pool to backfill
        :type pool: str
        :param delay_on_limit_secs:
        :param verbose:
        :type verbose: flag to whether display verbose message to backfill console
        :param conf: a dictionary which user could pass k-v pairs for backfill
        :type conf: dictionary
        :param rerun_failed_tasks: flag to whether to
                                   auto rerun the failed task in backfill
        :type rerun_failed_tasks: bool
        :param run_backwards: Whether to process the dates from most to least recent
        :type run_backwards bool
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
        super(BackfillJob, self).__init__(*args, **kwargs)

    def _update_counters(self, ti_status):
        """
        Updates the counters per state of the tasks that were running. Can re-add
        to tasks to run in case required.

        :param ti_status: the internal status of the backfill job tasks
        :type ti_status: BackfillJob._DagRunTaskStatus
        """
        for key, ti in list(ti_status.running.items()):
            ti.refresh_from_db()
            if ti.state == State.SUCCESS:
                ti_status.succeeded.add(key)
                self.log.debug("Task instance %s succeeded. Don't rerun.", ti)
                ti_status.running.pop(key)
                continue
            elif ti.state == State.SKIPPED:
                ti_status.skipped.add(key)
                self.log.debug("Task instance %s skipped. Don't rerun.", ti)
                ti_status.running.pop(key)
                continue
            elif ti.state == State.FAILED:
                self.log.error("Task instance %s failed", ti)
                ti_status.failed.add(key)
                ti_status.running.pop(key)
                continue
            # special case: if the task needs to run again put it back
            elif ti.state == State.UP_FOR_RETRY:
                self.log.warning("Task instance %s is up for retry", ti)
                ti_status.running.pop(key)
                ti_status.to_run[key] = ti
            # special case: if the task needs to be rescheduled put it back
            elif ti.state == State.UP_FOR_RESCHEDULE:
                self.log.warning("Task instance %s is up for reschedule", ti)
                ti_status.running.pop(key)
                ti_status.to_run[key] = ti
            # special case: The state of the task can be set to NONE by the task itself
            # when it reaches concurrency limits. It could also happen when the state
            # is changed externally, e.g. by clearing tasks from the ui. We need to cover
            # for that as otherwise those tasks would fall outside of the scope of
            # the backfill suddenly.
            elif ti.state == State.NONE:
                self.log.warning(
                    "FIXME: task instance %s state was set to none externally or "
                    "reaching concurrency limits. Re-adding task to queue.",
                    ti
                )
                ti.set_state(State.SCHEDULED)
                ti_status.running.pop(key)
                ti_status.to_run[key] = ti

    def _manage_executor_state(self, running):
        """
        Checks if the executor agrees with the state of task instances
        that are running

        :param running: dict of key, task to verify
        """
        executor = self.executor

        for key, state in list(executor.get_event_buffer().items()):
            if key not in running:
                self.log.warning(
                    "%s state %s not in running=%s",
                    key, state, running.values()
                )
                continue

            ti = running[key]
            ti.refresh_from_db()

            self.log.debug("Executor state: %s task %s", state, ti)

            if state == State.FAILED or state == State.SUCCESS:
                if ti.state == State.RUNNING or ti.state == State.QUEUED:
                    msg = ("Executor reports task instance {} finished ({}) "
                           "although the task says its {}. Was the task "
                           "killed externally?".format(ti, state, ti.state))
                    self.log.error(msg)
                    ti.handle_failure(msg)

    @provide_session
    def _get_dag_run(self, run_date, session=None):
        """
        Returns a dag run for the given run date, which will be matched to an existing
        dag run if available or create a new dag run otherwise. If the max_active_runs
        limit is reached, this function will return None.

        :param run_date: the execution date for the dag run
        :type run_date: datetime.datetime
        :param session: the database session object
        :type session: sqlalchemy.orm.session.Session
        :return: a DagRun in state RUNNING or None
        """
        run_id = BackfillJob.ID_FORMAT_PREFIX.format(run_date.isoformat())

        # consider max_active_runs but ignore when running subdags
        respect_dag_max_active_limit = (True
                                        if (self.dag.schedule_interval and
                                            not self.dag.is_subdag)
                                        else False)

        current_active_dag_count = self.dag.get_num_active_runs(external_trigger=False)

        # check if we are scheduling on top of a already existing dag_run
        # we could find a "scheduled" run instead of a "backfill"
        run = DagRun.find(dag_id=self.dag.dag_id,
                          execution_date=run_date,
                          session=session)

        if run is not None and len(run) > 0:
            run = run[0]
            if run.state == State.RUNNING:
                respect_dag_max_active_limit = False
        else:
            run = None

        # enforce max_active_runs limit for dag, special cases already
        # handled by respect_dag_max_active_limit
        if (respect_dag_max_active_limit and
                current_active_dag_count >= self.dag.max_active_runs):
            return None

        run = run or self.dag.create_dagrun(
            run_id=run_id,
            execution_date=run_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            external_trigger=False,
            session=session,
            conf=self.conf,
        )

        # set required transient field
        run.dag = self.dag

        # explicitly mark as backfill and running
        run.state = State.RUNNING
        run.run_id = run_id
        run.verify_integrity(session=session)
        return run

    @provide_session
    def _task_instances_for_dag_run(self, dag_run, session=None):
        """
        Returns a map of task instance key to task instance object for the tasks to
        run in the given dag run.

        :param dag_run: the dag run to get the tasks from
        :type dag_run: airflow.models.DagRun
        :param session: the database session object
        :type session: sqlalchemy.orm.session.Session
        """
        tasks_to_run = {}

        if dag_run is None:
            return tasks_to_run

        # check if we have orphaned tasks
        self.reset_state_for_orphaned_tasks(filter_by_dag_run=dag_run, session=session)

        # for some reason if we don't refresh the reference to run is lost
        dag_run.refresh_from_db()
        make_transient(dag_run)

        # TODO(edgarRd): AIRFLOW-1464 change to batch query to improve perf
        for ti in dag_run.get_task_instances():
            # all tasks part of the backfill are scheduled to run
            if ti.state == State.NONE:
                ti.set_state(State.SCHEDULED, session=session)
            if ti.state != State.REMOVED:
                tasks_to_run[ti.key] = ti

        return tasks_to_run

    def _log_progress(self, ti_status):
        self.log.info(
            '[backfill progress] | finished run %s of %s | tasks waiting: %s | succeeded: %s | '
            'running: %s | failed: %s | skipped: %s | deadlocked: %s | not ready: %s',
            ti_status.finished_runs, ti_status.total_runs, len(ti_status.to_run), len(ti_status.succeeded),
            len(ti_status.running), len(ti_status.failed), len(ti_status.skipped), len(ti_status.deadlocked),
            len(ti_status.not_ready)
        )

        self.log.debug(
            "Finished dag run loop iteration. Remaining tasks %s",
            ti_status.to_run.values()
        )

    @provide_session
    def _process_backfill_task_instances(self,
                                         ti_status,
                                         executor,
                                         pickle_id,
                                         start_date=None, session=None):
        """
        Process a set of task instances from a set of dag runs. Special handling is done
        to account for different task instance states that could be present when running
        them in a backfill process.

        :param ti_status: the internal status of the job
        :type ti_status: BackfillJob._DagRunTaskStatus
        :param executor: the executor to run the task instances
        :type executor: BaseExecutor
        :param pickle_id: the pickle_id if dag is pickled, None otherwise
        :type pickle_id: int
        :param start_date: the start date of the backfill job
        :type start_date: datetime.datetime
        :param session: the current session object
        :type session: sqlalchemy.orm.session.Session
        :return: the list of execution_dates for the finished dag runs
        :rtype: list
        """

        executed_run_dates = []

        while ((len(ti_status.to_run) > 0 or len(ti_status.running) > 0) and
                len(ti_status.deadlocked) == 0):
            self.log.debug("*** Clearing out not_ready list ***")
            ti_status.not_ready.clear()

            # we need to execute the tasks bottom to top
            # or leaf to root, as otherwise tasks might be
            # determined deadlocked while they are actually
            # waiting for their upstream to finish
            @provide_session
            def _per_task_process(task, key, ti, session=None):
                ti.refresh_from_db()

                task = self.dag.get_task(ti.task_id)
                ti.task = task

                ignore_depends_on_past = (
                    self.ignore_first_depends_on_past and
                    ti.execution_date == (start_date or ti.start_date))
                self.log.debug(
                    "Task instance to run %s state %s", ti, ti.state)

                # The task was already marked successful or skipped by a
                # different Job. Don't rerun it.
                if ti.state == State.SUCCESS:
                    ti_status.succeeded.add(key)
                    self.log.debug("Task instance %s succeeded. Don't rerun.", ti)
                    ti_status.to_run.pop(key)
                    if key in ti_status.running:
                        ti_status.running.pop(key)
                    return
                elif ti.state == State.SKIPPED:
                    ti_status.skipped.add(key)
                    self.log.debug("Task instance %s skipped. Don't rerun.", ti)
                    ti_status.to_run.pop(key)
                    if key in ti_status.running:
                        ti_status.running.pop(key)
                    return

                # guard against externally modified tasks instances or
                # in case max concurrency has been reached at task runtime
                elif ti.state == State.NONE:
                    self.log.warning(
                        "FIXME: task instance {} state was set to None "
                        "externally. This should not happen"
                    )
                    ti.set_state(State.SCHEDULED, session=session)
                if self.rerun_failed_tasks:
                    # Rerun failed tasks or upstreamed failed tasks
                    if ti.state in (State.FAILED, State.UPSTREAM_FAILED):
                        self.log.error("Task instance {ti} "
                                       "with state {state}".format(ti=ti,
                                                                   state=ti.state))
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        # Reset the failed task in backfill to scheduled state
                        ti.set_state(State.SCHEDULED, session=session)
                else:
                    # Default behaviour which works for subdag.
                    if ti.state in (State.FAILED, State.UPSTREAM_FAILED):
                        self.log.error("Task instance {ti} "
                                       "with {state} state".format(ti=ti,
                                                                   state=ti.state))
                        ti_status.failed.add(key)
                        ti_status.to_run.pop(key)
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        return

                backfill_context = DepContext(
                    deps=BACKFILL_QUEUED_DEPS,
                    ignore_depends_on_past=ignore_depends_on_past,
                    ignore_task_deps=self.ignore_task_deps,
                    flag_upstream_failed=True)

                ti.refresh_from_db(lock_for_update=True, session=session)
                # Is the task runnable? -- then run it
                # the dependency checker can change states of tis
                if ti.are_dependencies_met(
                        dep_context=backfill_context,
                        session=session,
                        verbose=self.verbose):
                    if executor.has_task(ti):
                        self.log.debug(
                            "Task Instance %s already in executor "
                            "waiting for queue to clear",
                            ti
                        )
                    else:
                        self.log.debug('Sending %s to executor', ti)
                        # Skip scheduled state, we are executing immediately
                        ti.state = State.QUEUED
                        ti.queued_dttm = timezone.utcnow()
                        session.merge(ti)

                        cfg_path = None
                        if executor.__class__ in (executors.LocalExecutor,
                                                  executors.SequentialExecutor):
                            cfg_path = tmp_configuration_copy()

                        executor.queue_task_instance(
                            ti,
                            mark_success=self.mark_success,
                            pickle_id=pickle_id,
                            ignore_task_deps=self.ignore_task_deps,
                            ignore_depends_on_past=ignore_depends_on_past,
                            pool=self.pool,
                            cfg_path=cfg_path)
                        ti_status.running[key] = ti
                        ti_status.to_run.pop(key)
                    session.commit()
                    return

                if ti.state == State.UPSTREAM_FAILED:
                    self.log.error("Task instance %s upstream failed", ti)
                    ti_status.failed.add(key)
                    ti_status.to_run.pop(key)
                    if key in ti_status.running:
                        ti_status.running.pop(key)
                    return

                # special case
                if ti.state == State.UP_FOR_RETRY:
                    self.log.debug(
                        "Task instance %s retry period not "
                        "expired yet", ti)
                    if key in ti_status.running:
                        ti_status.running.pop(key)
                    ti_status.to_run[key] = ti
                    return

                # special case
                if ti.state == State.UP_FOR_RESCHEDULE:
                    self.log.debug(
                        "Task instance %s reschedule period not "
                        "expired yet", ti)
                    if key in ti_status.running:
                        ti_status.running.pop(key)
                    ti_status.to_run[key] = ti
                    return

                # all remaining tasks
                self.log.debug('Adding %s to not_ready', ti)
                ti_status.not_ready.add(key)

            try:
                for task in self.dag.topological_sort():
                    for key, ti in list(ti_status.to_run.items()):
                        if task.task_id != ti.task_id:
                            continue

                        pool = session.query(models.Pool) \
                            .filter(models.Pool.pool == task.pool) \
                            .first()
                        if not pool:
                            raise PoolNotFound('Unknown pool: {}'.format(task.pool))

                        open_slots = pool.open_slots(session=session)
                        if open_slots <= 0:
                            raise NoAvailablePoolSlot(
                                "Not scheduling since there are "  # noqa: F523
                                "%s open slots in pool %s".format(open_slots, task.pool))  # noqa: F523

                        num_running_task_instances_in_dag = DAG.get_num_task_instances(
                            self.dag_id,
                            states=self.STATES_COUNT_AS_RUNNING,
                        )

                        if num_running_task_instances_in_dag >= self.dag.concurrency:
                            raise DagConcurrencyLimitReached(
                                "Not scheduling since DAG concurrency limit "
                                "is reached."
                            )

                        if task.task_concurrency:
                            num_running_task_instances_in_task = DAG.get_num_task_instances(
                                dag_id=self.dag_id,
                                task_ids=[task.task_id],
                                states=self.STATES_COUNT_AS_RUNNING,
                            )

                            if num_running_task_instances_in_task >= task.task_concurrency:
                                raise TaskConcurrencyLimitReached(
                                    "Not scheduling since Task concurrency limit "
                                    "is reached."
                                )

                        _per_task_process(task, key, ti)
            except (NoAvailablePoolSlot, DagConcurrencyLimitReached, TaskConcurrencyLimitReached) as e:
                self.log.debug(e)

            # execute the tasks in the queue
            self.heartbeat()
            executor.heartbeat()

            # If the set of tasks that aren't ready ever equals the set of
            # tasks to run and there are no running tasks then the backfill
            # is deadlocked
            if (ti_status.not_ready and
                    ti_status.not_ready == set(ti_status.to_run) and
                    len(ti_status.running) == 0):
                self.log.warning(
                    "Deadlock discovered for ti_status.to_run=%s",
                    ti_status.to_run.values()
                )
                ti_status.deadlocked.update(ti_status.to_run.values())
                ti_status.to_run.clear()

            # check executor state
            self._manage_executor_state(ti_status.running)

            # update the task counters
            self._update_counters(ti_status=ti_status)

            # update dag run state
            _dag_runs = ti_status.active_runs[:]
            for run in _dag_runs:
                run.update_state(session=session)
                if run.state in State.finished():
                    ti_status.finished_runs += 1
                    ti_status.active_runs.remove(run)
                    executed_run_dates.append(run.execution_date)

            self._log_progress(ti_status)

        # return updated status
        return executed_run_dates

    @provide_session
    def _collect_errors(self, ti_status, session=None):
        def tabulate_ti_keys_set(set_ti_keys):
            # Sorting by execution date first
            sorted_ti_keys = sorted(
                set_ti_keys, key=lambda ti_key: (ti_key[2], ti_key[0], ti_key[1], ti_key[3]))
            return tabulate(sorted_ti_keys, headers=["DAG ID", "Task ID", "Execution date", "Try number"])

        def tabulate_tis_set(set_tis):
            # Sorting by execution date first
            sorted_tis = sorted(
                set_tis, key=lambda ti: (ti.execution_date, ti.dag_id, ti.task_id, ti.try_number))
            tis_values = (
                (ti.dag_id, ti.task_id, ti.execution_date, ti.try_number)
                for ti in sorted_tis
            )
            return tabulate(tis_values, headers=["DAG ID", "Task ID", "Execution date", "Try number"])

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
                    verbose=self.verbose) !=
                t.are_dependencies_met(
                    dep_context=DepContext(ignore_depends_on_past=True),
                    session=session,
                    verbose=self.verbose)
                for t in ti_status.deadlocked)
            if deadlocked_depends_on_past:
                err += (
                    'Some of the deadlocked tasks were unable to run because '
                    'of "depends_on_past" relationships. Try running the '
                    'backfill with the option '
                    '"ignore_first_depends_on_past=True" or passing "-I" at '
                    'the command line.')
            err += '\nThese tasks have succeeded:\n'
            err += tabulate_ti_keys_set(ti_status.succeeded)
            err += '\n\nThese tasks are running:\n'
            err += tabulate_ti_keys_set(ti_status.running)
            err += '\n\nThese tasks have failed:\n'
            err += tabulate_ti_keys_set(ti_status.failed)
            err += '\n\nThese tasks are skipped:\n'
            err += tabulate_ti_keys_set(ti_status.skipped)
            err += '\n\nThese tasks are deadlocked:\n'
            err += tabulate_tis_set(ti_status.deadlocked)

        return err

    @provide_session
    def _execute_for_run_dates(self, run_dates, ti_status, executor, pickle_id,
                               start_date, session=None):
        """
        Computes the dag runs and their respective task instances for
        the given run dates and executes the task instances.
        Returns a list of execution dates of the dag runs that were executed.

        :param run_dates: Execution dates for dag runs
        :type run_dates: list
        :param ti_status: internal BackfillJob status structure to tis track progress
        :type ti_status: BackfillJob._DagRunTaskStatus
        :param executor: the executor to use, it must be previously started
        :type executor: BaseExecutor
        :param pickle_id: numeric id of the pickled dag, None if not pickled
        :type pickle_id: int
        :param start_date: backfill start date
        :type start_date: datetime.datetime
        :param session: the current session object
        :type session: sqlalchemy.orm.session.Session
        """
        for next_run_date in run_dates:
            dag_run = self._get_dag_run(next_run_date, session=session)
            tis_map = self._task_instances_for_dag_run(dag_run,
                                                       session=session)
            if dag_run is None:
                continue

            ti_status.active_runs.append(dag_run)
            ti_status.to_run.update(tis_map or {})

        processed_dag_run_dates = self._process_backfill_task_instances(
            ti_status=ti_status,
            executor=executor,
            pickle_id=pickle_id,
            start_date=start_date,
            session=session)

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
            if dag_run.state not in State.finished():
                dag_run.set_state(State.FAILED)
            session.merge(dag_run)

    @provide_session
    def _execute(self, session=None):
        """
        Initializes all components required to run a dag for a specified date range and
        calls helper method to execute the tasks.
        """
        ti_status = BackfillJob._DagRunTaskStatus()

        start_date = self.bf_start_date

        # Get intervals between the start/end dates, which will turn into dag runs
        run_dates = self.dag.get_run_dates(start_date=start_date,
                                           end_date=self.bf_end_date)
        if self.run_backwards:
            tasks_that_depend_on_past = [t.task_id for t in self.dag.task_dict.values() if t.depends_on_past]
            if tasks_that_depend_on_past:
                raise AirflowException(
                    'You cannot backfill backwards because one or more tasks depend_on_past: {}'.format(
                        ",".join(tasks_that_depend_on_past)))
            run_dates = run_dates[::-1]

        if len(run_dates) == 0:
            self.log.info("No run dates were found for the given dates and dag interval.")
            return

        # picklin'
        pickle_id = None
        if not self.donot_pickle and self.executor.__class__ not in (
                executors.LocalExecutor, executors.SequentialExecutor):
            pickle = DagPickle(self.dag)
            session.add(pickle)
            session.commit()
            pickle_id = pickle.id

        executor = self.executor
        executor.start()

        ti_status.total_runs = len(run_dates)  # total dag runs in backfill

        try:
            remaining_dates = ti_status.total_runs
            while remaining_dates > 0:
                dates_to_process = [run_date for run_date in run_dates
                                    if run_date not in ti_status.executed_dag_run_dates]

                self._execute_for_run_dates(run_dates=dates_to_process,
                                            ti_status=ti_status,
                                            executor=executor,
                                            pickle_id=pickle_id,
                                            start_date=start_date,
                                            session=session)

                remaining_dates = (
                    ti_status.total_runs - len(ti_status.executed_dag_run_dates)
                )
                err = self._collect_errors(ti_status=ti_status, session=session)
                if err:
                    raise AirflowException(err)

                if remaining_dates > 0:
                    self.log.info(
                        "max_active_runs limit for dag %s has been reached "
                        " - waiting for other dag runs to finish",
                        self.dag_id
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

        self.log.info("Backfill done. Exiting.")
