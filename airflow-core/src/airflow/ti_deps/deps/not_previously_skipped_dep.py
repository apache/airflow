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

from airflow.models.taskinstance import PAST_DEPENDS_MET
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep

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
    be skipped.
    """

    NAME = "Not Previously Skipped"
    IGNORABLE = True
    IS_TASK_DEP = True

    def _get_dep_statuses(self, ti, session, dep_context):
        from airflow.utils.state import TaskInstanceState

        upstream = ti.task.get_direct_relatives(upstream=True)

        finished_tis = dep_context.ensure_finished_tis(ti.get_dagrun(session), session)

        finished_task_ids = {t.task_id for t in finished_tis}

        for parent in upstream:
            if parent.inherits_from_skipmixin:
                if parent.task_id not in finished_task_ids:
                    # This can happen if the parent task has not yet run.
                    continue

                prev_result = ti.xcom_pull(
                    task_ids=parent.task_id, key=XCOM_SKIPMIXIN_KEY, session=session, map_indexes=ti.map_index
                )

                if prev_result is None:
                    # This can happen if the parent task has not yet run.
                    continue

                should_skip = False
                if (
                    XCOM_SKIPMIXIN_FOLLOWED in prev_result
                    and ti.task_id not in prev_result[XCOM_SKIPMIXIN_FOLLOWED]
                ):
                    # Skip any tasks that are not in "followed"
                    should_skip = True
                elif (
                    XCOM_SKIPMIXIN_SKIPPED in prev_result
                    and ti.task_id in prev_result[XCOM_SKIPMIXIN_SKIPPED]
                ):
                    # Skip any tasks that are in "skipped"
                    should_skip = True

                if should_skip:
                    # If the parent SkipMixin has run, and the XCom result stored indicates this
                    # ti should be skipped, set ti.state to SKIPPED and fail the rule so that the
                    # ti does not execute.
                    if dep_context.wait_for_past_depends_before_skipping:
                        past_depends_met = ti.xcom_pull(
                            task_ids=ti.task_id, key=PAST_DEPENDS_MET, session=session, default=False
                        )
                        if not past_depends_met:
                            yield self._failing_status(
                                reason="Task should be skipped but the past depends are not met"
                            )
                            return
                    ti.set_state(TaskInstanceState.SKIPPED, session)
                    yield self._failing_status(
                        reason=f"Skipping because of previous XCom result from parent task {parent.task_id}"
                    )
                    return
