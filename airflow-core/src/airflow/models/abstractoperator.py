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
from typing import TYPE_CHECKING, Any, Callable

from airflow.configuration import conf
from airflow.sdk.definitions._internal.abstractoperator import (
    AbstractOperator as TaskSDKAbstractOperator,
    NotMapped as NotMapped,  # Re-export this for compat
)
from airflow.sdk.definitions.context import Context
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.weight_rule import db_safe_priority

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.sdk.bases.baseoperator import BaseOperator
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
