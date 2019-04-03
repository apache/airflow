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

from sqlalchemy import case, func

import airflow
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.db import provide_session
from airflow.utils.state import State


class TriggerRuleDep(BaseTIDep):
    """
    Determines if a task's upstream tasks are in a state that allows a given task instance
    to run.
    """
    NAME = "Trigger Rule"
    IGNOREABLE = True
    IS_TASK_DEP = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        TI = airflow.models.TaskInstance
        TR = airflow.models.TriggerRule

        # Checking that all upstream dependencies have succeeded
        if not ti.task.upstream_list:
            yield self._passing_status(
                reason="The task instance did not have any upstream tasks.")
            return

        if ti.task.trigger_rule == TR.DUMMY:
            yield self._passing_status(reason="The task had a dummy trigger rule set.")
            return

        # TODO(unknown): this query becomes quite expensive with dags that have many
        # tasks. It should be refactored to let the task report to the dag run and get the
        # aggregates from there.
        qry = (
            session
            .query(
                func.coalesce(func.sum(
                    case([(TI.state == State.SUCCESS, 1)], else_=0)), 0),
                func.coalesce(func.sum(
                    case([(TI.state == State.SKIPPED, 1)], else_=0)), 0),
                func.coalesce(func.sum(
                    case([(TI.state == State.FAILED, 1)], else_=0)), 0),
                func.coalesce(func.sum(
                    case([(TI.state == State.UPSTREAM_FAILED, 1)], else_=0)), 0),
                func.count(TI.task_id),
            )
            .filter(
                TI.dag_id == ti.dag_id,
                TI.task_id.in_(ti.task.upstream_task_ids),
                TI.execution_date == ti.execution_date,
                TI.state.in_([
                    State.SUCCESS, State.FAILED,
                    State.UPSTREAM_FAILED, State.SKIPPED]),
            )
        )

        successes, skipped, failed, upstream_failed, done = qry.first()
        for dep_status in self._evaluate_trigger_rule(
                ti=ti,
                successes=successes,
                skipped=skipped,
                failed=failed,
                upstream_failed=upstream_failed,
                done=done,
                flag_upstream_failed=dep_context.flag_upstream_failed,
                session=session):
            yield dep_status

    @provide_session
    def _evaluate_trigger_rule(
            self,
            ti,
            successes,
            skipped,
            failed,
            upstream_failed,
            done,
            flag_upstream_failed,
            session):
        """
        Yields a dependency status that indicate whether the given task instance's trigger
        rule was met.

        :param ti: the task instance to evaluate the trigger rule of
        :type ti: airflow.models.TaskInstance
        :param successes: Number of successful upstream tasks
        :type successes: bool
        :param skipped: Number of skipped upstream tasks
        :type skipped: bool
        :param failed: Number of failed upstream tasks
        :type failed: bool
        :param upstream_failed: Number of upstream_failed upstream tasks
        :type upstream_failed: bool
        :param done: Number of completed upstream tasks
        :type done: bool
        :param flag_upstream_failed: This is a hack to generate
            the upstream_failed state creation while checking to see
            whether the task instance is runnable. It was the shortest
            path to add the feature
        :type flag_upstream_failed: bool
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """

        TR = airflow.models.TriggerRule

        task = ti.task
        upstream = len(task.upstream_task_ids)
        tr = task.trigger_rule
        upstream_done = done >= upstream
        upstream_tasks_state = {
            "total": upstream, "successes": successes, "skipped": skipped,
            "failed": failed, "upstream_failed": upstream_failed, "done": done
        }
        # TODO(aoen): Ideally each individual trigger rules would be its own class, but
        # this isn't very feasible at the moment since the database queries need to be
        # bundled together for efficiency.
        # handling instant state assignment based on trigger rules
        if flag_upstream_failed:
            if tr == TR.ALL_SUCCESS:
                if upstream_failed or failed:
                    ti.set_state(State.UPSTREAM_FAILED, session)
                elif skipped:
                    ti.set_state(State.SKIPPED, session)
            elif tr == TR.ALL_FAILED:
                if successes or skipped:
                    ti.set_state(State.SKIPPED, session)
            elif tr == TR.ONE_SUCCESS:
                if upstream_done and not successes:
                    ti.set_state(State.SKIPPED, session)
            elif tr == TR.ONE_FAILED:
                if upstream_done and not (failed or upstream_failed):
                    ti.set_state(State.SKIPPED, session)
            elif tr == TR.NONE_FAILED:
                if upstream_failed or failed:
                    ti.set_state(State.UPSTREAM_FAILED, session)
                elif skipped == upstream:
                    ti.set_state(State.SKIPPED, session)
            elif tr == TR.NONE_SKIPPED:
                if skipped:
                    ti.set_state(State.SKIPPED, session)

        if tr == TR.ONE_SUCCESS:
            if successes <= 0:
                yield self._failing_status(
                    reason="Task's trigger rule '{0}' requires one upstream "
                    "task success, but none were found. "
                    "upstream_tasks_state={1}, upstream_task_ids={2}"
                    .format(tr, upstream_tasks_state, task.upstream_task_ids))
        elif tr == TR.ONE_FAILED:
            if not failed and not upstream_failed:
                yield self._failing_status(
                    reason="Task's trigger rule '{0}' requires one upstream "
                    "task failure, but none were found. "
                    "upstream_tasks_state={1}, upstream_task_ids={2}"
                    .format(tr, upstream_tasks_state, task.upstream_task_ids))
        elif tr == TR.ALL_SUCCESS:
            num_failures = upstream - successes
            if num_failures > 0:
                yield self._failing_status(
                    reason="Task's trigger rule '{0}' requires all upstream "
                    "tasks to have succeeded, but found {1} non-success(es). "
                    "upstream_tasks_state={2}, upstream_task_ids={3}"
                    .format(tr, num_failures, upstream_tasks_state,
                            task.upstream_task_ids))
        elif tr == TR.ALL_FAILED:
            num_successes = upstream - failed - upstream_failed
            if num_successes > 0:
                yield self._failing_status(
                    reason="Task's trigger rule '{0}' requires all upstream "
                    "tasks to have failed, but found {1} non-failure(s). "
                    "upstream_tasks_state={2}, upstream_task_ids={3}"
                    .format(tr, num_successes, upstream_tasks_state,
                            task.upstream_task_ids))
        elif tr == TR.ALL_DONE:
            if not upstream_done:
                yield self._failing_status(
                    reason="Task's trigger rule '{0}' requires all upstream "
                    "tasks to have completed, but found {1} task(s) that "
                    "weren't done. upstream_tasks_state={2}, "
                    "upstream_task_ids={3}"
                    .format(tr, upstream_done, upstream_tasks_state,
                            task.upstream_task_ids))
        elif tr == TR.NONE_FAILED:
            num_failures = upstream - successes - skipped
            if num_failures > 0:
                yield self._failing_status(
                    reason="Task's trigger rule '{0}' requires all upstream "
                    "tasks to have succeeded or been skipped, but found {1} non-success(es). "
                    "upstream_tasks_state={2}, upstream_task_ids={3}"
                    .format(tr, num_failures, upstream_tasks_state,
                            task.upstream_task_ids))
        elif tr == TR.NONE_SKIPPED:
            if skipped > 0:
                yield self._failing_status(
                    reason="Task's trigger rule '{0}' requires all upstream "
                    "tasks to not have been skipped, but found {1} task(s) skipped. "
                    "upstream_tasks_state={2}, upstream_task_ids={3}"
                    .format(tr, skipped, upstream_tasks_state,
                            task.upstream_task_ids))
        else:
            yield self._failing_status(
                reason="No strategy to evaluate trigger rule '{0}'.".format(tr))
