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

from collections.abc import Iterable
from typing import TYPE_CHECKING

from airflow.providers.standard.utils.weekday import WeekDay
from airflow.sensors.base import BaseSensorOperator
from airflow.utils import timezone

if TYPE_CHECKING:
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        # TODO: Remove once provider drops support for Airflow 2
        from airflow.utils.context import Context


class DayOfWeekSensor(BaseSensorOperator):
    """
    Waits until the first specified day of the week.

    For example, if the execution day of the task is '2018-12-22' (Saturday)
    and you pass 'FRIDAY', the task will wait until next Friday.

    **Example** (with single day): ::

        weekend_check = DayOfWeekSensor(
            task_id="weekend_check", week_day="Saturday", use_task_logical_date=True, dag=dag
        )

    **Example** (with multiple day using set): ::

        weekend_check = DayOfWeekSensor(
            task_id="weekend_check", week_day={"Saturday", "Sunday"}, use_task_logical_date=True, dag=dag
        )

    **Example** (with :class:`~airflow.utils.weekday.WeekDay` enum): ::

        # import WeekDay Enum
        from airflow.providers.standard.utils.weekday import WeekDay

        weekend_check = DayOfWeekSensor(
            task_id="weekend_check",
            week_day={WeekDay.SATURDAY, WeekDay.SUNDAY},
            use_task_logical_date=True,
            dag=dag,
        )

    :param week_day: Day of the week to check (full name). Optionally, a set
        of days can also be provided using a set.
        Example values:

            * ``"MONDAY"``,
            * ``{"Saturday", "Sunday"}``
            * ``{WeekDay.TUESDAY}``
            * ``{WeekDay.SATURDAY, WeekDay.SUNDAY}``

        To use ``WeekDay`` enum, import it from ``airflow.utils.weekday``

    :param use_task_logical_date: If ``True``, uses task's logical date to compare
        with week_day. Execution Date is Useful for backfilling.
        If ``False``, uses system's day of the week. Useful when you
        don't want to run anything on weekdays on the system.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:DayOfWeekSensor`

    """

    def __init__(
        self,
        *,
        week_day: str | Iterable[str] | WeekDay | Iterable[WeekDay],
        use_task_logical_date: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.week_day = week_day
        self.use_task_logical_date = use_task_logical_date
        self._week_day_num = WeekDay.validate_week_day(week_day)

    def poke(self, context: Context) -> bool:
        self.log.info(
            "Poking until weekday is in %s, Today is %s",
            self.week_day,
            WeekDay(timezone.utcnow().isoweekday()).name,
        )

        if self.use_task_logical_date:
            logical_date = context.get("logical_date")
            dag_run = context.get("dag_run")

            if not (logical_date or (dag_run and dag_run.run_after)):
                raise ValueError(
                    "Either `logical_date` or `run_after` should be provided in the task context when "
                    "`use_task_logical_date` is True"
                )

            determined_weekday_num = (
                logical_date.isoweekday() if logical_date else dag_run.run_after.isoweekday()  # type: ignore[union-attr]
            )

            return determined_weekday_num in self._week_day_num
        return timezone.utcnow().isoweekday() in self._week_day_num
