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

import dataclasses
import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, NoReturn

from airflow.providers.common.compat.sdk import BaseSensorOperator, timezone
from airflow.providers.standard.triggers.temporal import DateTimeTrigger
from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_3_PLUS
from airflow.triggers.base import StartTriggerArgs

if TYPE_CHECKING:
    from airflow.sdk import Context


class DateTimeSensor(BaseSensorOperator):
    """
    Waits until the specified datetime.

    A major advantage of this sensor is idempotence for the ``target_time``.
    It handles some cases for which ``TimeSensor`` and ``TimeDeltaSensor`` are not suited.

    **Example** 1 :
        If a task needs to wait for 11am on each ``logical_date``. Using
        ``TimeSensor`` or ``TimeDeltaSensor``, all backfill tasks started at
        1am have to wait for 10 hours. This is unnecessary, e.g. a backfill
        task with ``{{ ds }} = '1970-01-01'`` does not need to wait because
        ``1970-01-01T11:00:00`` has already passed.

    **Example** 2 :
        If a DAG is scheduled to run at 23:00 daily, but one of the tasks is
        required to run at 01:00 next day, using ``TimeSensor`` will return
        ``True`` immediately because 23:00 > 01:00. Instead, we can do this:

        .. code-block:: python

            DateTimeSensor(
                task_id="wait_for_0100",
                target_time="{{ data_interval_end.tomorrow().replace(hour=1) }}",
            )

    :param target_time: datetime after which the job succeeds. (templated)
    """

    template_fields: Sequence[str] = ("target_time",)

    def __init__(self, *, target_time: str | datetime.datetime, **kwargs) -> None:
        super().__init__(**kwargs)
        self.target_time = target_time

    def poke(self, context: Context) -> bool:
        self.log.info("Checking if the time (%s) has come", self.target_time)
        return timezone.utcnow() > self._moment

    @property
    def _moment(self) -> datetime.datetime:
        target_time: Any = self.target_time
        if isinstance(target_time, datetime.datetime):
            target_time = target_time.isoformat()
        if isinstance(target_time, str):
            return timezone.parse(target_time)
        raise TypeError(f"Expected str or datetime.datetime type for target_time. Got {type(target_time)}")


class DateTimeSensorAsync(DateTimeSensor):
    """
    Wait until the specified datetime occurs.

    Deferring itself to avoid taking up a worker slot while it is waiting.
    It is a drop-in replacement for DateTimeSensor.

    :param target_time: datetime after which the job succeeds. (templated)
    :param start_from_trigger: Start the task directly from the triggerer without going into the worker.
        This requires either a static ``target_time`` (a datetime or ISO-8601 string) or, on
        Airflow >= 3.3, a templated ``target_time`` that the triggerer can render before the trigger
        runs. On earlier Airflow versions a templated ``target_time`` cannot be resolved this way, so
        ``start_from_trigger`` is disabled with a warning and the task defers from the worker instead.
    :param trigger_kwargs: The keyword arguments passed to the trigger when start_from_trigger is set to True
        during dynamic task mapping. This argument is not used in standard usage.
    :param end_from_trigger: End the task directly from the triggerer without going into the worker.
    """

    start_trigger_args = StartTriggerArgs(
        trigger_cls="airflow.providers.standard.triggers.temporal.DateTimeTrigger",
        trigger_kwargs={"moment": "", "end_from_trigger": False},
        next_method="execute_complete",
        next_kwargs=None,
        timeout=None,
    )
    start_from_trigger = False

    def __init__(
        self,
        *,
        start_from_trigger: bool = False,
        end_from_trigger: bool = False,
        trigger_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.end_from_trigger = end_from_trigger

        self.start_from_trigger = start_from_trigger
        if self.start_from_trigger:
            try:
                moment = self._moment
            except ValueError:
                # target_time couldn't be parsed as a static datetime at Dag-parse time. This is
                # the normal, documented case of target_time being a Jinja template, e.g.
                # "{{ data_interval_end.tomorrow().replace(hour=1) }}" -- not necessarily bad input.
                moment = None

            # Replaced rather than mutated: ``start_trigger_args`` is a class attribute, so
            # assigning through it would overwrite the arguments of every other task built
            # from this operator.
            if moment is not None:
                self.start_trigger_args = dataclasses.replace(
                    self.start_trigger_args,
                    trigger_kwargs=dict(
                        moment=moment,
                        end_from_trigger=self.end_from_trigger,
                    ),
                )
            elif AIRFLOW_V_3_3_PLUS:
                # Hand the raw template string to the trigger under the same name as this
                # operator's template field ("target_time"). The triggerer renders any
                # start_trigger_args kwarg whose name matches an operator template field before
                # running the trigger (see BaseTrigger.task_instance / render_template_fields),
                # the same mechanism FileSensor relies on for a templated `filepath`.
                self.start_trigger_args = dataclasses.replace(
                    self.start_trigger_args,
                    trigger_kwargs=dict(
                        target_time=self.target_time,
                        end_from_trigger=self.end_from_trigger,
                    ),
                )
            else:
                # Airflow < 3.3 triggerers can't render template fields on a trigger before it
                # runs, so a templated target_time can never be resolved via start_from_trigger.
                # Falling back to the normal worker-deferred path (execute()) avoids crashing Dag
                # parsing on every parse cycle; execute() runs after the scheduler/worker has
                # already rendered target_time normally.
                self.log.warning(
                    "start_from_trigger=True requires a static target_time on Airflow < 3.3, but "
                    "%r looks like a template for task %r. Disabling start_from_trigger and "
                    "deferring from the worker instead. Upgrade to Airflow >= 3.3 to defer "
                    "directly from the triggerer with a templated target_time.",
                    self.target_time,
                    self.task_id,
                )
                self.start_from_trigger = False

    def execute(self, context: Context) -> NoReturn:
        self.defer(
            method_name="execute_complete",
            trigger=DateTimeTrigger(
                moment=self._moment,
                end_from_trigger=self.end_from_trigger,
            )
            if AIRFLOW_V_3_0_PLUS
            else DateTimeTrigger(moment=self._moment),
        )

    def execute_complete(self, context: Context, event: Any = None) -> None:
        """Handle the event when the trigger fires and return immediately."""
        return None
