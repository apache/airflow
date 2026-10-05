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

from typing import TYPE_CHECKING

from airflow.models.taskinstance import PAST_DEPENDS_MET
from airflow.models.xcom import XComModel
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.orm import Session

    from airflow.models.taskinstance import TaskInstance
    from airflow.serialization.definitions.taskgroup import SerializedMappedTaskGroup
    from airflow.ti_deps.dep_context import DepContext
    from airflow.ti_deps.deps.base_ti_dep import TIDepStatus

# The following constants are taken from the SkipMixin class in the standard provider
# The key used by SkipMixin to store XCom data.
XCOM_SKIPMIXIN_KEY = "skipmixin_key"

# The dictionary key used to denote task IDs that are skipped
XCOM_SKIPMIXIN_SKIPPED = "skipped"

# The dictionary key used to denote task IDs that are followed
XCOM_SKIPMIXIN_FOLLOWED = "followed"


class NotPreviouslySkippedDep(BaseTIDep):
    """
    Determine if this task should be skipped.

    Based on any of the task's direct upstream relatives have decided this task should
    be skipped. Inside a mapped task group, a ``SkipMixin`` task of the same group that
    lists this task in its skip decision for the same map index skips it too, even when
    it is not a direct upstream.
    """

    NAME = "Not Previously Skipped"
    IGNORABLE = True
    IS_TASK_DEP = True

    def _get_dep_statuses(self, ti, dep_context, *, session):
        finished_tis = dep_context.ensure_finished_tis(ti.get_dagrun(session=session), session=session)

        # An unexpanded placeholder (map index -1) keeps the direct-upstream lookup below.
        mapped_group = ti.task.get_closest_mapped_task_group() if ti.map_index >= 0 else None

        finished_task_ids = {t.task_id for t in finished_tis}

        for parent in ti.task.get_direct_relatives(upstream=True):
            if parent.inherits_from_skipmixin:
                if mapped_group is not None and _shares_map_indexes(parent, mapped_group):
                    # Read below from the per-pass memo, together with the rest of the group.
                    continue

                if parent.task_id not in finished_task_ids:
                    # This can happen if the parent task has not yet run.
                    continue

                # Use the parent's map context to look up the XCom. An unmapped parent
                # (e.g. LatestOnlyOperator) writes XCom with map_index=-1, so we must
                # query with -1 instead of the child's map_index. A parent inside a
                # mapped task group is expanded like a mapped task, so it writes XCom
                # per map index even though ``is_mapped`` is False.
                xcom_map_index = ti.map_index if parent.get_needs_expansion() else -1
                prev_result = ti.xcom_pull(
                    task_ids=parent.task_id,
                    key=XCOM_SKIPMIXIN_KEY,
                    session=session,
                    map_indexes=xcom_map_index,
                )

                if prev_result is None:
                    # This can happen if the parent task has not yet run.
                    continue

                if _should_skip(prev_result, ti.task_id, is_direct_parent=True):
                    yield from self._skip(ti, dep_context, parent.task_id, session=session)
                    return

        if mapped_group is None:
            return

        # A SkipMixin task inside a mapped task group decides once per map index, and the
        # worker leaves skipping those downstream task instances to this dep because they may
        # not be expanded yet. ShortCircuitOperator lists every task it skips, not only its
        # direct downstream, so honour any decision of the same group for this map index.
        decisions = _mapped_group_skip_decisions(ti, mapped_group, finished_tis, dep_context, session)
        upstream_task_ids = ti.task.upstream_task_ids
        for parent_task_id, prev_result in decisions.get(ti.map_index, ()):
            if _should_skip(prev_result, ti.task_id, is_direct_parent=parent_task_id in upstream_task_ids):
                yield from self._skip(ti, dep_context, parent_task_id, session=session)
                return

    def _skip(
        self, ti: TaskInstance, dep_context: DepContext, parent_task_id: str, *, session: Session
    ) -> Iterator[TIDepStatus]:
        # If the parent SkipMixin has run, and the XCom result stored indicates this
        # ti should be skipped, set ti.state to SKIPPED and fail the rule so that the
        # ti does not execute.
        if dep_context.wait_for_past_depends_before_skipping:
            past_depends_met = ti.xcom_pull(
                task_ids=ti.task_id, key=PAST_DEPENDS_MET, session=session, default=False
            )
            if not past_depends_met:
                yield self._failing_status(reason="Task should be skipped but the past depends are not met")
                return
        ti.set_state(TaskInstanceState.SKIPPED, session=session)
        yield self._failing_status(
            reason=f"Skipping because of previous XCom result from parent task {parent_task_id}"
        )


def _should_skip(prev_result: dict, task_id: str, *, is_direct_parent: bool) -> bool:
    # "followed" only lists a branch operator's direct downstream tasks, so it says nothing
    # about a task further down, which follows its own trigger rule instead.
    if is_direct_parent and XCOM_SKIPMIXIN_FOLLOWED in prev_result:
        return task_id not in prev_result[XCOM_SKIPMIXIN_FOLLOWED]
    return XCOM_SKIPMIXIN_SKIPPED in prev_result and task_id in prev_result[XCOM_SKIPMIXIN_SKIPPED]


def _shares_map_indexes(task, mapped_group: SerializedMappedTaskGroup) -> bool:
    # Only tasks whose closest mapped task group is the same one use the same map indexes; a
    # task in a nested mapped task group is expanded once per combination of both groups.
    group = task.get_closest_mapped_task_group()
    return group is not None and group.group_id == mapped_group.group_id


def _mapped_group_skip_decisions(
    ti: TaskInstance,
    mapped_group: SerializedMappedTaskGroup,
    finished_tis: list[TaskInstance],
    dep_context: DepContext,
    session: Session,
) -> dict[int, list[tuple[str, dict]]]:
    """
    Return the skip decisions of the group's successful SkipMixin task instances, by map index.

    Clearing a task instance keeps its XComs until it runs again, so a decision is only
    current when the task instance that wrote it succeeded; one cleared and then skipped
    or upstream-failed without running still carries the decision of its earlier try.
    """
    memo_key = (ti.dag_id, ti.run_id, mapped_group.group_id)
    if (decisions := dep_context.mapped_group_skip_decisions.get(memo_key)) is not None:
        return decisions

    decisions = {}
    skipmixin_task_ids = {
        t.task_id
        for t in mapped_group.iter_tasks()
        if t.inherits_from_skipmixin and _shares_map_indexes(t, mapped_group)
    }
    succeeded_keys = {
        (t.task_id, t.map_index)
        for t in finished_tis
        if t.state == TaskInstanceState.SUCCESS and t.task_id in skipmixin_task_ids
    }
    if succeeded_keys:
        query = XComModel.get_many(
            run_id=ti.run_id, key=XCOM_SKIPMIXIN_KEY, dag_ids=ti.dag_id, task_ids=skipmixin_task_ids
        )
        rows = session.execute(
            query.with_only_columns(XComModel.task_id, XComModel.map_index, XComModel.value).order_by(None)
        )
        for row in rows:
            if (row.task_id, row.map_index) not in succeeded_keys:
                continue
            if (decision := XComModel.deserialize_value(row)) is not None:
                decisions.setdefault(row.map_index, []).append((row.task_id, decision))
    dep_context.mapped_group_skip_decisions[memo_key] = decisions
    return decisions
