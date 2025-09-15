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

import logging

from airflow.models.taskinstance import PAST_DEPENDS_MET
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep

logger = logging.getLogger(__name__)

## The following constants are taken from the SkipMixin class in the standard provider
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

                # Pull the SkipMixin XCom from the upstream parent.
                # If the parent is mapped, pull the XCom for the same map index.
                # If the parent is NOT mapped (common for ShortCircuitOperator), pull WITHOUT
                # the map_indexes filter so mapped children (map_index >= 0) can see the parent's
                # XCom written at the parent's TI row.
                is_parent_mapped = getattr(parent, "is_mapped", False)
                map_indexes = ti.map_index if is_parent_mapped else None

                logger.info(
                    "Checking SkipMixin XCom from parent task %s (mapped=%s, map_index=%s, child_map_index=%s)",
                    parent.task_id,
                    is_parent_mapped,
                    map_indexes,
                    ti.map_index,
                )

                prev_result = ti.xcom_pull(
                    task_ids=parent.task_id,
                    key=XCOM_SKIPMIXIN_KEY,
                    session=session,
                    map_indexes=map_indexes,
                )

                if prev_result is None:
                    logger.info(
                        "No SkipMixin XCom found for parent task %s (map_index=%s), continuing",
                        parent.task_id,
                        map_indexes,
                    )
                    continue

                should_skip = False

                # The SkipMixin XCom may have several shapes depending on version/usage:
                # 1) {"skipped": ["task_id1", ...], "followed": [...]}
                # 2) {"skipped": {"task_id1": [0,1], ...}, "followed": {"task_id2": [0]}}
                #
                # Handle both shapes for robust behavior.

                # Handle "followed" first: if the parent says which tasks to follow and the current
                # task is not in that set/list/dict for the current map_index, then it should be skipped.
                if XCOM_SKIPMIXIN_FOLLOWED in prev_result:
                    followed_val = prev_result[XCOM_SKIPMIXIN_FOLLOWED]
                    logger.debug("Found 'followed' in SkipMixin XCom: %s", followed_val)

                    if isinstance(followed_val, dict):
                        # dict: task_id -> list-of-map-indexes
                        idxs = followed_val.get(ti.task_id)
                        if idxs is None:
                            # This task is not in the followed dict -> skip it
                            logger.info("Task %s not in 'followed' dict, marking to skip", ti.task_id)
                            should_skip = True
                        else:
                            # If current TI is mapped, ensure its map_index is in idxs; otherwise skip.
                            if getattr(ti, "map_index", None) is not None and ti.map_index >= 0:
                                if ti.map_index not in idxs:
                                    logger.info(
                                        "Mapped task %s[%s] not in followed map indexes %s, marking to skip",
                                        ti.task_id,
                                        ti.map_index,
                                        idxs,
                                    )
                                    should_skip = True
                                else:
                                    logger.debug(
                                        "Mapped task %s[%s] is in followed map indexes %s, not skipping",
                                        ti.task_id,
                                        ti.map_index,
                                        idxs,
                                    )
                            # If TI is unmapped, presence in dict above is enough to follow
                            pass
                    else:
                        # followed_val assumed to be iterable of task_ids (list/set)
                        if ti.task_id not in followed_val:
                            logger.info(
                                "Task %s not in 'followed' list %s, marking to skip", ti.task_id, followed_val
                            )
                            should_skip = True

                # If not already determined to skip via "followed", check "skipped".
                if not should_skip and XCOM_SKIPMIXIN_SKIPPED in prev_result:
                    skipped_val = prev_result[XCOM_SKIPMIXIN_SKIPPED]
                    logger.debug("Found 'skipped' in SkipMixin XCom: %s", skipped_val)

                    if isinstance(skipped_val, dict):
                        # dict: task_id -> list-of-map-indexes
                        idxs = skipped_val.get(ti.task_id)
                        if idxs is not None:
                            if getattr(ti, "map_index", None) is not None and ti.map_index >= 0:
                                # Mapped TI: skip only if this map_index is in the list
                                if ti.map_index in idxs:
                                    logger.info(
                                        "Mapped task %s[%s] is in 'skipped' map %s, marking to skip",
                                        ti.task_id,
                                        ti.map_index,
                                        skipped_val,
                                    )
                                    should_skip = True
                            else:
                                # Unmapped TI: presence alone means skip.
                                logger.info(
                                    "Unmapped task %s is in 'skipped' map, marking to skip", ti.task_id
                                )
                                should_skip = True
                    else:
                        # skipped_val is a simple list of task_ids
                        if ti.task_id in skipped_val:
                            logger.info(
                                "Task %s is in 'skipped' list %s, marking to skip", ti.task_id, skipped_val
                            )
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
