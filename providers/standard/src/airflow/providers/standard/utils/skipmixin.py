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
from collections.abc import Iterable, Sequence
from types import GeneratorType
from typing import TYPE_CHECKING, Union

from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator, MappedOperator
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as SDKTaskInstanceProtocol

    DAGNode = BaseOperator
    RuntimeTaskInstanceProtocol = SDKTaskInstanceProtocol

    SkipMixinOperator = Union[BaseOperator, MappedOperator]
else:
    SkipMixinOperator = "BaseOperator | MappedOperator"

# The key used by SkipMixin to store XCom data.
XCOM_SKIPMIXIN_KEY = "skipmixin_key"

# The dictionary key used to denote task IDs that are skipped
XCOM_SKIPMIXIN_SKIPPED = "skipped"

# The dictionary key used to denote task IDs that are followed
XCOM_SKIPMIXIN_FOLLOWED = "followed"


def _ensure_tasks(nodes: Iterable[DAGNode]) -> list[SkipMixinOperator]:
    """Return a list of operators from the given nodes."""
    from airflow.models.baseoperator import BaseOperator, MappedOperator

    result: list[SkipMixinOperator] = []
    for node in nodes:
        if isinstance(node, (BaseOperator, MappedOperator)):
            result.append(node)  # type: ignore[arg-type]
    return result


# This class should only be used in Airflow 3.0 and later.
class SkipMixin(LoggingMixin):
    """A Mixin to skip Tasks Instances."""

    @staticmethod
    def _set_state_to_skipped(tasks: Sequence[str | tuple[str, int]], map_index: int | None = None) -> None:
        """
        Set the state of the tasks to skipped.

        :param tasks: List of task IDs or (task_id, map_index) tuples to skip
        :param map_index: Map index of the current task (deprecated, included for backward compatibility)
        """
        processed_tasks: list[str | tuple[str, int]] = []
        for task in tasks:
            if isinstance(task, str) and "[" in task and task.endswith("]"):
                task_id, map_idx_str = task.rsplit("[", 1)
                try:
                    map_idx = int(map_idx_str.rstrip("]"))
                    processed_tasks.append((task_id, map_idx))
                    continue
                except (ValueError, IndexError):
                    processed_tasks.append(task)
            else:
                processed_tasks.append(task)

        from airflow.exceptions import DownstreamTasksSkipped

        raise DownstreamTasksSkipped(tasks=processed_tasks)

    def skip(
        self,
        ti: RuntimeTaskInstanceProtocol,
        tasks: Iterable[DAGNode],
        dag_run=None,
    ) -> None:
        """
        Set tasks instances to skipped from the same dag run.

        If this instance has a `task_id` attribute, store the list of skipped task IDs to XCom
        so that NotPreviouslySkippedDep knows these tasks should be skipped when they
        are cleared.

        :param ti: the task instance for which to set the tasks to skipped
        :param tasks: tasks to skip (not task_ids)
        """
        task_id: str | None = getattr(self, "task_id", None)
        task_list = list(_ensure_tasks(tasks))
        log = self.log
        tasks_list = list(tasks)
        if len(tasks_list) != len(task_list):
            dropped = [
                str(getattr(t, "task_id", str(t)))
                for t in tasks_list
                if hasattr(t, "task_id") and t not in task_list
            ]
            if dropped:
                log.info("Dropped %d non-operator tasks: %s", len(dropped), dropped)

        if not task_list:
            return

        expanded_task_ids: list[str] = []

        dag_run = dag_run or getattr(ti, "dag_run", None)

        for t in task_list:
            task_id = getattr(t, "task_id", "unknown")
            is_mapped = getattr(t, "is_mapped", False)

            if is_mapped and dag_run is not None:
                try:
                    mapped_params = getattr(t, "mapped_params", {})
                    num_instances = 1
                    if hasattr(mapped_params, "get_total_map_length"):
                        num_instances = mapped_params.get_total_map_length()
                    elif hasattr(t, "get_parse_time_mapped_ti_count"):
                        num_instances = t.get_parse_time_mapped_ti_count()

                    for i in range(num_instances):
                        mapped_id = f"{task_id}[{i}]"
                        expanded_task_ids.append(mapped_id)

                except Exception as e:
                    log.warning("Failed to expand mapped task %s: %s", task_id, e, exc_info=True)
                    expanded_task_ids.append(task_id)
            else:
                expanded_task_ids.append(task_id)

        if task_id is not None:
            ti.xcom_push(
                key=XCOM_SKIPMIXIN_KEY,
                value={XCOM_SKIPMIXIN_SKIPPED: expanded_task_ids},
            )

        self._set_state_to_skipped(expanded_task_ids, ti.map_index)

    def skip_all_except(
        self,
        ti: RuntimeTaskInstanceProtocol,
        branch_task_ids: None | str | Iterable[str],
    ):
        """
        Implement the logic for a branching operator.

        Given a single task ID or list of task IDs to follow, this skips all other tasks
        immediately downstream of this operator.

        branch_task_ids is stored to XCom so that NotPreviouslySkippedDep knows skipped tasks or
        newly added tasks should be skipped when they are cleared.
        """
        # Ensure we don't serialize a generator object
        if branch_task_ids and isinstance(branch_task_ids, GeneratorType):
            branch_task_ids = list(branch_task_ids)
        log = self.log  # Note: need to catch logger form instance, static logger breaks pytest
        if isinstance(branch_task_ids, str):
            branch_task_id_set = {branch_task_ids}
        elif isinstance(branch_task_ids, Iterable):
            branch_task_id_set = set(branch_task_ids)
            invalid_task_ids_type = {
                (bti, type(bti).__name__) for bti in branch_task_id_set if not isinstance(bti, str)
            }
            if invalid_task_ids_type:
                raise AirflowException(
                    f"'branch_task_ids' expected all task IDs are strings. "
                    f"Invalid tasks found: {invalid_task_ids_type}."
                )
        elif branch_task_ids is None:
            branch_task_id_set = set()
        else:
            raise AirflowException(
                "'branch_task_ids' must be either None, a task ID, or an Iterable of IDs, "
                f"but got {type(branch_task_ids).__name__!r}."
            )

        log.info("Following branch %s", branch_task_id_set)

        if TYPE_CHECKING:
            assert ti.task

        task = ti.task
        dag = ti.task.dag

        valid_task_ids = set(dag.task_ids)
        invalid_task_ids = branch_task_id_set - valid_task_ids
        if invalid_task_ids:
            raise AirflowException(
                "'branch_task_ids' must contain only valid task_ids. "
                f"Invalid tasks found: {invalid_task_ids}."
            )

        downstream_tasks = _ensure_tasks(task.downstream_list)

        if downstream_tasks:
            # For a branching workflow that looks like this, when "branch" does skip_all_except("task1"),
            # we intuitively expect both "task1" and "join" to execute even though strictly speaking,
            # "join" is also immediately downstream of "branch" and should have been skipped. Therefore,
            # we need a special case here for such empty branches: Check downstream tasks of branch_task_ids.
            # In case the task to skip is also downstream of branch_task_ids, we add it to branch_task_ids and
            # exclude it from skipping.
            #
            # branch  ----->  join
            #   \            ^
            #     v        /
            #       task1
            #
            for branch_task_id in list(branch_task_id_set):
                branch_task_id_set.update(dag.get_task(branch_task_id).get_flat_relative_ids(upstream=False))

            skip_tasks = [
                (t.task_id, ti.map_index) for t in downstream_tasks if t.task_id not in branch_task_id_set
            ]

            follow_task_ids = [t.task_id for t in downstream_tasks if t.task_id in branch_task_id_set]
            log.info("Skipping tasks %s", skip_tasks)
            ti.xcom_push(
                key=XCOM_SKIPMIXIN_KEY,
                value={XCOM_SKIPMIXIN_FOLLOWED: follow_task_ids},
            )
            #  The following could be applied only for non-mapped tasks,
            #  as future mapped tasks have not been expanded yet. Such tasks
            #  have to be handled by NotPreviouslySkippedDep.
            self._set_state_to_skipped(skip_tasks, ti.map_index)  # type: ignore[arg-type]
