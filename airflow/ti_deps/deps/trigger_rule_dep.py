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

from collections import Counter
from typing import TYPE_CHECKING

from sqlalchemy import func

from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule as TR

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.taskinstance import TaskInstance


class TriggerRuleDep(BaseTIDep):
    """
    Determines if a task's upstream tasks are in a state that allows a given task instance
    to run.
    """

    NAME = "Trigger Rule"
    IGNORABLE = True
    IS_TASK_DEP = True

    @staticmethod
    def _get_states_count_upstream_ti(task, finished_tis):
        """
        This function returns the states of the upstream tis for a specific ti in order to determine
        whether this ti can run in this iteration

        :param ti: the ti that we want to calculate deps for
        :param finished_tis: all the finished tasks of the dag_run
        """
        counter = Counter(ti.state for ti in finished_tis if ti.task_id in task.upstream_task_ids)
        return (
            counter.get(State.SUCCESS, 0),
            counter.get(State.SKIPPED, 0),
            counter.get(State.FAILED, 0),
            counter.get(State.UPSTREAM_FAILED, 0),
            counter.get(State.REMOVED, 0),
            sum(counter.values()),
        )

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context: DepContext):
        # Checking that all upstream dependencies have succeeded
        if not ti.task.upstream_list:
            yield self._passing_status(reason="The task instance did not have any upstream tasks.")
            return

        if ti.task.trigger_rule == TR.ALWAYS:
            yield self._passing_status(reason="The task had a always trigger rule set.")
            return
        # see if the task name is in the task upstream for our task
        successes, skipped, failed, upstream_failed, removed, done = self._get_states_count_upstream_ti(
            task=ti.task, finished_tis=dep_context.ensure_finished_tis(ti.get_dagrun(session), session)
        )

        yield from self._evaluate_trigger_rule(
            ti=ti,
            successes=successes,
            skipped=skipped,
            failed=failed,
            upstream_failed=upstream_failed,
            removed=removed,
            done=done,
            flag_upstream_failed=dep_context.flag_upstream_failed,
            dep_context=dep_context,
            session=session,
        )

    @staticmethod
    def _count_upstreams(ti: TaskInstance, *, session: Session):
        from airflow.models.taskinstance import TaskInstance

        # Optimization: Don't need to hit the database if no upstreams are mapped.
        upstream_task_ids = ti.task.upstream_task_ids
        if ti.task.dag and not any(ti.task.dag.get_task(tid).is_mapped for tid in upstream_task_ids):
            return len(upstream_task_ids)

        # We don't naively count task instances because it is not guaranteed
        # that all upstreams have been created in the database at this point.
        # Instead, we look for already-expanded tasks, and add them to the raw
        # task count without considering mapping.
        mapped_tis_addition = (
            session.query(func.count())
            .filter(
                TaskInstance.dag_id == ti.dag_id,
                TaskInstance.run_id == ti.run_id,
                TaskInstance.task_id.in_(upstream_task_ids),
                TaskInstance.map_index > 0,
            )
            .scalar()
        )
        return len(upstream_task_ids) + mapped_tis_addition

    @provide_session
    def _evaluate_trigger_rule(
        self,
        ti: TaskInstance,
        successes,
        skipped,
        failed,
        upstream_failed,
        removed,
        done,
        flag_upstream_failed,
        dep_context: DepContext,
        session: Session = NEW_SESSION,
    ):
        """
        Yields a dependency status that indicate whether the given task instance's trigger
        rule was met.

        :param ti: the task instance to evaluate the trigger rule of
        :param successes: Number of successful upstream tasks
        :param skipped: Number of skipped upstream tasks
        :param failed: Number of failed upstream tasks
        :param upstream_failed: Number of upstream_failed upstream tasks
        :param done: Number of completed upstream tasks
        :param flag_upstream_failed: This is a hack to generate
            the upstream_failed state creation while checking to see
            whether the task instance is runnable. It was the shortest
            path to add the feature
        :param session: database session
        """
        task = ti.task
        upstream = self._count_upstreams(ti, session=session)
        trigger_rule = task.trigger_rule
        upstream_done = done >= upstream
        upstream_tasks_state = {
            "total": upstream,
            "successes": successes,
            "skipped": skipped,
            "failed": failed,
            "removed": removed,
            "upstream_failed": upstream_failed,
            "done": done,
        }
        changed: bool = False
        if flag_upstream_failed:
            if trigger_rule == TR.ALL_SUCCESS:
                if upstream_failed or failed:
                    changed = ti.set_state(State.UPSTREAM_FAILED, session)
                elif skipped:
                    changed = ti.set_state(State.SKIPPED, session)
                elif removed and successes and ti.map_index > -1:
                    if ti.map_index >= successes:
                        changed = ti.set_state(State.REMOVED, session)
            elif trigger_rule == TR.ALL_FAILED:
                if successes or skipped:
                    changed = ti.set_state(State.SKIPPED, session)
            elif trigger_rule == TR.ONE_SUCCESS:
                if upstream_done and done == skipped:
                    # if upstream is done and all are skipped mark as skipped
                    changed = ti.set_state(State.SKIPPED, session)
                elif upstream_done and successes <= 0:
                    # if upstream is done and there are no successes mark as upstream failed
                    changed = ti.set_state(State.UPSTREAM_FAILED, session)
            elif trigger_rule == TR.ONE_FAILED:
                if upstream_done and not (failed or upstream_failed):
                    changed = ti.set_state(State.SKIPPED, session)
            elif trigger_rule == TR.ONE_DONE:
                if upstream_done and not (failed or successes):
                    changed = ti.set_state(State.SKIPPED, session)
            elif trigger_rule == TR.NONE_FAILED:
                if upstream_failed or failed:
                    changed = ti.set_state(State.UPSTREAM_FAILED, session)
            elif trigger_rule == TR.NONE_FAILED_MIN_ONE_SUCCESS:
                if upstream_failed or failed:
                    changed = ti.set_state(State.UPSTREAM_FAILED, session)
                elif skipped == upstream:
                    changed = ti.set_state(State.SKIPPED, session)
            elif trigger_rule == TR.NONE_SKIPPED:
                if skipped:
                    changed = ti.set_state(State.SKIPPED, session)
            elif trigger_rule == TR.ALL_SKIPPED:
                if successes or failed:
                    changed = ti.set_state(State.SKIPPED, session)

        if changed:
            dep_context.have_changed_ti_states = True

        if trigger_rule == TR.ONE_SUCCESS:
            if successes <= 0:
                yield self._failing_status(
                    reason=(
                        f"Task's trigger rule '{trigger_rule}' requires one upstream task success, "
                        f"but none were found. upstream_tasks_state={upstream_tasks_state}, "
                        f"upstream_task_ids={task.upstream_task_ids}"
                    )
                )
        elif trigger_rule == TR.ONE_FAILED:
            if not failed and not upstream_failed:
                yield self._failing_status(
                    reason=(
                        f"Task's trigger rule '{trigger_rule}' requires one upstream task failure, "
                        f"but none were found. upstream_tasks_state={upstream_tasks_state}, "
                        f"upstream_task_ids={task.upstream_task_ids}"
                    )
                )
        elif trigger_rule == TR.ONE_DONE:
            if successes + failed <= 0:
                yield self._failing_status(
                    reason=(
                        f"Task's trigger rule '{trigger_rule}'"
                        "requires at least one upstream task failure or success"
                        f"but none were failed or success. upstream_tasks_state={upstream_tasks_state}, "
                        f"upstream_task_ids={task.upstream_task_ids}"
                    )
                )
        elif trigger_rule == TR.ALL_SUCCESS:
            num_failures = upstream - successes
            if ti.map_index > -1:
                num_failures -= removed
            if num_failures > 0:
                yield self._failing_status(
                    reason=(
                        f"Task's trigger rule '{trigger_rule}' requires all upstream tasks to have "
                        f"succeeded, but found {num_failures} non-success(es). "
                        f"upstream_tasks_state={upstream_tasks_state}, "
                        f"upstream_task_ids={task.upstream_task_ids}"
                    )
                )
        elif trigger_rule == TR.ALL_FAILED:
            num_successes = upstream - failed - upstream_failed
            if ti.map_index > -1:
                num_successes -= removed
            if num_successes > 0:
                yield self._failing_status(
                    reason=(
                        f"Task's trigger rule '{trigger_rule}' requires all upstream tasks to have failed, "
                        f"but found {num_successes} non-failure(s). "
                        f"upstream_tasks_state={upstream_tasks_state}, "
                        f"upstream_task_ids={task.upstream_task_ids}"
                    )
                )
        elif trigger_rule == TR.ALL_DONE:
            if not upstream_done:
                yield self._failing_status(
                    reason=(
                        f"Task's trigger rule '{trigger_rule}' requires all upstream tasks to have "
                        f"completed, but found {upstream_done} task(s) that were not done. "
                        f"upstream_tasks_state={upstream_tasks_state}, "
                        f"upstream_task_ids={task.upstream_task_ids}"
                    )
                )
        elif trigger_rule == TR.NONE_FAILED:
            num_failures = upstream - successes - skipped
            if ti.map_index > -1:
                num_failures -= removed
            if num_failures > 0:
                yield self._failing_status(
                    reason=(
                        f"Task's trigger rule '{trigger_rule}' requires all upstream tasks to have "
                        f"succeeded or been skipped, but found {num_failures} non-success(es). "
                        f"upstream_tasks_state={upstream_tasks_state}, "
                        f"upstream_task_ids={task.upstream_task_ids}"
                    )
                )
        elif trigger_rule == TR.NONE_FAILED_MIN_ONE_SUCCESS:
            num_failures = upstream - successes - skipped
            if ti.map_index > -1:
                num_failures -= removed
            if num_failures > 0:
                yield self._failing_status(
                    reason=(
                        f"Task's trigger rule '{trigger_rule}' requires all upstream tasks to have "
                        f"succeeded or been skipped, but found {num_failures} non-success(es). "
                        f"upstream_tasks_state={upstream_tasks_state}, "
                        f"upstream_task_ids={task.upstream_task_ids}"
                    )
                )
        elif trigger_rule == TR.NONE_SKIPPED:
            if not upstream_done or (skipped > 0):
                yield self._failing_status(
                    reason=(
                        f"Task's trigger rule '{trigger_rule}' requires all upstream tasks to not have been "
                        f"skipped, but found {skipped} task(s) skipped. "
                        f"upstream_tasks_state={upstream_tasks_state}, "
                        f"upstream_task_ids={task.upstream_task_ids}"
                    )
                )
        elif trigger_rule == TR.ALL_SKIPPED:
            num_non_skipped = upstream - skipped
            if num_non_skipped > 0:
                yield self._failing_status(
                    reason=(
                        f"Task's trigger rule '{trigger_rule}' requires all upstream tasks to have been "
                        f"skipped, but found {num_non_skipped} task(s) in non skipped state. "
                        f"upstream_tasks_state={upstream_tasks_state}, "
                        f"upstream_task_ids={task.upstream_task_ids}"
                    )
                )
        else:
            yield self._failing_status(reason=f"No strategy to evaluate trigger rule '{trigger_rule}'.")
