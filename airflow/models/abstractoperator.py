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

import datetime
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Callable

from sqlalchemy import select

from airflow.configuration import conf
from airflow.sdk.definitions._internal.abstractoperator import (
    AbstractOperator as TaskSDKAbstractOperator,
    NotMapped as NotMapped,  # Re-export this for compat
)
from airflow.sdk.definitions.context import Context
from airflow.utils.db import exists_query
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.weight_rule import db_safe_priority

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.dag import DAG as SchedulerDAG
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.definitions.baseoperator import BaseOperator
    from airflow.task.priority_strategy import PriorityWeightStrategy
    from airflow.triggers.base import StartTriggerArgs

TaskStateChangeCallback = Callable[[Context], None]

DEFAULT_OWNER: str = conf.get_mandatory_value("operators", "default_owner")
DEFAULT_POOL_SLOTS: int = 1
DEFAULT_PRIORITY_WEIGHT: int = 1
DEFAULT_EXECUTOR: str | None = None
DEFAULT_QUEUE: str = conf.get_mandatory_value("operators", "default_queue")
DEFAULT_IGNORE_FIRST_DEPENDS_ON_PAST: bool = conf.getboolean(
    "scheduler", "ignore_first_depends_on_past_by_default"
)
DEFAULT_WAIT_FOR_PAST_DEPENDS_BEFORE_SKIPPING: bool = False
DEFAULT_RETRIES: int = conf.getint("core", "default_task_retries", fallback=0)
DEFAULT_RETRY_DELAY: datetime.timedelta = datetime.timedelta(
    seconds=conf.getint("core", "default_task_retry_delay", fallback=300)
)
MAX_RETRY_DELAY: int = conf.getint("core", "max_task_retry_delay", fallback=24 * 60 * 60)

DEFAULT_TRIGGER_RULE: TriggerRule = TriggerRule.ALL_SUCCESS
DEFAULT_TASK_EXECUTION_TIMEOUT: datetime.timedelta | None = conf.gettimedelta(
    "core", "default_task_execution_timeout"
)


class AbstractOperator(LoggingMixin, TaskSDKAbstractOperator):
    """
    Common implementation for operators, including unmapped and mapped.

    This base class is more about sharing implementations, not defining a common
    interface. Unfortunately it's difficult to use this as the common base class
    for typing due to BaseOperator carrying too much historical baggage.

    The union type ``from airflow.models.operator import Operator`` is easier
    to use for typing purposes.

    :meta private:
    """

    weight_rule: PriorityWeightStrategy

    def unmap(self, resolve: None | dict[str, Any] | tuple[Context, Session]) -> BaseOperator:
        """
        Get the "normal" operator from current abstract operator.

        MappedOperator uses this to unmap itself based on the map index. A non-
        mapped operator (i.e. BaseOperator subclass) simply returns itself.

        :meta private:
        """
        raise NotImplementedError()

    def expand_start_from_trigger(self, *, context: Context, session: Session) -> bool:
        """
        Get the start_from_trigger value of the current abstract operator.

        MappedOperator uses this to unmap start_from_trigger to decide whether to start the task
        execution directly from triggerer.

        :meta private:
        """
        raise NotImplementedError()

    def expand_start_trigger_args(self, *, context: Context, session: Session) -> StartTriggerArgs | None:
        """
        Get the start_trigger_args value of the current abstract operator.

        MappedOperator uses this to unmap start_trigger_args to decide how to start a task from triggerer.

        :meta private:
        """
        raise NotImplementedError()

    @property
    def priority_weight_total(self) -> int:
        """
        Total priority weight for the task. It might include all upstream or downstream tasks.

        Depending on the weight rule:

        - WeightRule.ABSOLUTE - only own weight
        - WeightRule.DOWNSTREAM - adds priority weight of all downstream tasks
        - WeightRule.UPSTREAM - adds priority weight of all upstream tasks
        """
        # TODO: This should live in the WeightStragies themselves, not in here
        from airflow.task.priority_strategy import (
            _AbsolutePriorityWeightStrategy,
            _DownstreamPriorityWeightStrategy,
            _UpstreamPriorityWeightStrategy,
        )

        if isinstance(self.weight_rule, _AbsolutePriorityWeightStrategy):
            return db_safe_priority(self.priority_weight)
        elif isinstance(self.weight_rule, _DownstreamPriorityWeightStrategy):
            upstream = False
        elif isinstance(self.weight_rule, _UpstreamPriorityWeightStrategy):
            upstream = True
        else:
            upstream = False
        dag = self.get_dag()
        if dag is None:
            return db_safe_priority(self.priority_weight)
        return db_safe_priority(
            self.priority_weight
            + sum(
                dag.task_dict[task_id].priority_weight
                for task_id in self.get_flat_relative_ids(upstream=upstream)
            )
        )

    def expand_mapped_task(self, run_id: str, *, session: Session) -> tuple[Sequence[TaskInstance], int]:
        """
        Create the mapped task instances for mapped task.

        :raise NotMapped: If this task does not need expansion.
        :return: The newly created mapped task instances (if any) in ascending
            order by map index, and the maximum map index value.
        """
        from sqlalchemy import func, or_

        from airflow.models.taskinstance import TaskInstance
        from airflow.sdk.definitions.baseoperator import BaseOperator
        from airflow.sdk.definitions.mappedoperator import MappedOperator
        from airflow.settings import task_instance_mutation_hook

        if not isinstance(self, (BaseOperator, MappedOperator)):
            raise RuntimeError(
                f"cannot expand unrecognized operator type {type(self).__module__}.{type(self).__name__}"
            )

        from airflow.models.baseoperator import BaseOperator as DBBaseOperator
        from airflow.models.expandinput import NotFullyPopulated

        try:
            total_length: int | None = DBBaseOperator.get_mapped_ti_count(self, run_id, session=session)
        except NotFullyPopulated as e:
            # It's possible that the upstream tasks are not yet done, but we
            # don't have upstream of upstreams in partial DAGs (possible in the
            # mini-scheduler), so we ignore this exception.
            if not self.dag or not self.dag.partial:
                self.log.error(
                    "Cannot expand %r for run %s; missing upstream values: %s",
                    self,
                    run_id,
                    sorted(e.missing),
                )
            total_length = None

        state: TaskInstanceState | None = None
        unmapped_ti: TaskInstance | None = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.task_id == self.task_id,
                TaskInstance.run_id == run_id,
                TaskInstance.map_index == -1,
                or_(TaskInstance.state.in_(State.unfinished), TaskInstance.state.is_(None)),
            )
        ).one_or_none()

        all_expanded_tis: list[TaskInstance] = []

        if unmapped_ti:
            if TYPE_CHECKING:
                assert self.dag is None or isinstance(self.dag, SchedulerDAG)

            # The unmapped task instance still exists and is unfinished, i.e. we
            # haven't tried to run it before.
            if total_length is None:
                # If the DAG is partial, it's likely that the upstream tasks
                # are not done yet, so the task can't fail yet.
                if not self.dag or not self.dag.partial:
                    unmapped_ti.state = TaskInstanceState.UPSTREAM_FAILED
            elif total_length < 1:
                # If the upstream maps this to a zero-length value, simply mark
                # the unmapped task instance as SKIPPED (if needed).
                self.log.info(
                    "Marking %s as SKIPPED since the map has %d values to expand",
                    unmapped_ti,
                    total_length,
                )
                unmapped_ti.state = TaskInstanceState.SKIPPED
            else:
                zero_index_ti_exists = exists_query(
                    TaskInstance.dag_id == self.dag_id,
                    TaskInstance.task_id == self.task_id,
                    TaskInstance.run_id == run_id,
                    TaskInstance.map_index == 0,
                    session=session,
                )
                if not zero_index_ti_exists:
                    # Otherwise convert this into the first mapped index, and create
                    # TaskInstance for other indexes.
                    unmapped_ti.map_index = 0
                    self.log.debug("Updated in place to become %s", unmapped_ti)
                    all_expanded_tis.append(unmapped_ti)
                    # execute hook for task instance map index 0
                    task_instance_mutation_hook(unmapped_ti)
                    session.flush()
                else:
                    self.log.debug("Deleting the original task instance: %s", unmapped_ti)
                    session.delete(unmapped_ti)
                state = unmapped_ti.state

        if total_length is None or total_length < 1:
            # Nothing to fixup.
            indexes_to_map: Iterable[int] = ()
        else:
            # Only create "missing" ones.
            current_max_mapping = session.scalar(
                select(func.max(TaskInstance.map_index)).where(
                    TaskInstance.dag_id == self.dag_id,
                    TaskInstance.task_id == self.task_id,
                    TaskInstance.run_id == run_id,
                )
            )
            indexes_to_map = range(current_max_mapping + 1, total_length)

        for index in indexes_to_map:
            # TODO: Make more efficient with bulk_insert_mappings/bulk_save_mappings.
            ti = TaskInstance(self, run_id=run_id, map_index=index, state=state)
            self.log.debug("Expanding TIs upserted %s", ti)
            task_instance_mutation_hook(ti)
            ti = session.merge(ti)
            ti.refresh_from_task(self)  # session.merge() loses task information.
            all_expanded_tis.append(ti)

        # Coerce the None case to 0 -- these two are almost treated identically,
        # except the unmapped ti (if exists) is marked to different states.
        total_expanded_ti_count = total_length or 0

        # Any (old) task instances with inapplicable indexes (>= the total
        # number we need) are set to "REMOVED".
        query = select(TaskInstance).where(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.run_id == run_id,
            TaskInstance.map_index >= total_expanded_ti_count,
        )
        query = with_row_locks(query, of=TaskInstance, session=session, skip_locked=True)
        to_update = session.scalars(query)
        for ti in to_update:
            ti.state = TaskInstanceState.REMOVED
        session.flush()
        return all_expanded_tis, total_expanded_ti_count - 1
