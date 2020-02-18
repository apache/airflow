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

from collections import Counter

import airflow
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import provide_session
from airflow.utils.state import State


class TriggerRuleDep(BaseTIDep):
    """
    Determines if a task's upstream tasks are in a state that allows a given task instance
    to run.
    """
    NAME = "Trigger Rule"
    IGNOREABLE = True
    IS_TASK_DEP = True

    @staticmethod
    @provide_session
    def _get_states_count_upstream_ti(ti, finished_tasks, session):
        """
        This function returns the states of the upstream tis for a specific ti in order to determine
        whether this ti can run in this iteration

        :param ti: the ti that we want to calculate deps for
        :type ti: airflow.models.TaskInstance
        :param finished_tasks: all the finished tasks of the dag_run
        :type finished_tasks: list[airflow.models.TaskInstance]
        """
        if finished_tasks is None:
            # this is for the strange feature of running tasks without dag_run
            finished_tasks = ti.task.dag.get_task_instances(
                start_date=ti.execution_date,
                end_date=ti.execution_date,
                state=State.finished() + [State.UPSTREAM_FAILED],
                session=session)
        counter = Counter(task.state for task in finished_tasks if task.task_id in ti.task.upstream_task_ids)
        return counter.get(State.SUCCESS, 0), counter.get(State.SKIPPED, 0), counter.get(State.FAILED, 0), \
            counter.get(State.UPSTREAM_FAILED, 0), sum(counter.values())

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        TR = airflow.utils.trigger_rule.TriggerRule
        # Checking that all upstream dependencies have succeeded
        if not ti.task.upstream_list:
            yield self._passing_status(
                reason="The task instance did not have any upstream tasks.")
            return

        if ti.task.trigger_rule == TR.DUMMY:
            yield self._passing_status(reason="The task had a dummy trigger rule set.")
            return
        # see if the task name is in the task upstream for our task
        successes, skipped, failed, upstream_failed, done = self._get_states_count_upstream_ti(
            ti=ti,
            finished_tasks=dep_context.finished_tasks)

        yield from self._evaluate_trigger_rule(
            ti=ti,
            successes=successes,
            skipped=skipped,
            failed=failed,
            upstream_failed=upstream_failed,
            done=done,
            flag_upstream_failed=dep_context.flag_upstream_failed,
            session=session)

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
        :type successes: int
        :param skipped: Number of skipped upstream tasks
        :type skipped: int
        :param failed: Number of failed upstream tasks
        :type failed: int
        :param upstream_failed: Number of upstream_failed upstream tasks
        :type upstream_failed: int
        :param done: Number of completed upstream tasks
        :type done: int
        :param flag_upstream_failed: This is a hack to generate
            the upstream_failed state creation while checking to see
            whether the task instance is runnable. It was the shortest
            path to add the feature
        :type flag_upstream_failed: bool
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """

        TR = airflow.utils.trigger_rule.TriggerRule

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
            if not upstream_done or (skipped > 0):
                yield self._failing_status(
                    reason="Task's trigger rule '{0}' requires all upstream "
                    "tasks to not have been skipped, but found {1} task(s) skipped. "
                    "upstream_tasks_state={2}, upstream_task_ids={3}"
                    .format(tr, skipped, upstream_tasks_state,
                            task.upstream_task_ids))
        else:
            yield self._failing_status(
                reason="No strategy to evaluate trigger rule '{0}'.".format(tr))
