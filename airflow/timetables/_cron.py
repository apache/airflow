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
from functools import cached_property
from typing import TYPE_CHECKING, Any

from cron_descriptor import CasingTypeEnum, ExpressionDescriptor, FormatException, MissingFieldException
from croniter import CroniterBadCronError, CroniterBadDateError, croniter
from pendulum.tz.timezone import Timezone

from airflow.exceptions import AirflowTimetableInvalid
from airflow.utils.dates import cron_presets
from airflow.utils.timezone import convert_to_utc, is_localized, make_aware, make_naive

if TYPE_CHECKING:
    from pendulum import DateTime


def _is_schedule_fixed(expression: str) -> bool:
    """Figures out if the schedule has a fixed time (e.g. 3 AM every day).

    :return: True if the schedule has a fixed time, False if not.

    Detection is done by "peeking" the next two cron trigger time; if the
    two times have the same minute and hour value, the schedule is fixed,
    and we *don't* need to perform the DST fix.

    This assumes DST happens on whole minute changes (e.g. 12:59 -> 12:00).
    """
    cron = croniter(expression)
    next_a = cron.get_next(datetime.datetime)
    next_b = cron.get_next(datetime.datetime)
    return next_b.minute == next_a.minute and next_b.hour == next_a.hour


class CronMixin:
    """Mixin to provide interface to work with croniter."""

    def __init__(self, crons: str | list[str], timezone: str | Timezone) -> None:
        """
        :param crons: One or more cron expressions
        :param timezone:
        """
        if isinstance(timezone, str):
            timezone = Timezone(timezone)
        self._timezone = timezone

        if isinstance(crons, str):
            crons = [crons]

        self._expressions = {cron_presets.get(cron, cron) for cron in crons}
        cron_descriptions = set()

        for cron_expression in self._expressions:
            descriptor = ExpressionDescriptor(
                expression=cron_expression, casing_type=CasingTypeEnum.Sentence, use_24hour_time_format=True
            )

            try:
                # checking for more than 5 parameters in Cron and avoiding evaluation for now,
                # as Croniter has inconsistent evaluation with other libraries
                if len(croniter(cron_expression).expanded) > 5:
                    raise FormatException()
                interval_description: str = descriptor.get_description()
            except (CroniterBadCronError, FormatException, MissingFieldException):
                interval_description = ""

            cron_descriptions.add(interval_description)

        self.description: str = " and ".join(cron_descriptions)

    def __eq__(self, other: Any) -> bool:
        """Both expression and timezone should match.

        This is only for testing purposes and should not be relied on otherwise.
        """
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._expressions == other._expressions and self._timezone == other._timezone

    @property
    def summary(self) -> str:
        return ", ".join(self._expressions)

    def validate(self) -> None:
        try:
            for cron_expression in self._expressions:
                croniter(cron_expression)
        except (CroniterBadCronError, CroniterBadDateError) as e:
            raise AirflowTimetableInvalid(str(e))

    @cached_property
    def _should_fix_dst(self) -> bool:
        # This is lazy so instantiating a schedule does not immediately raise
        # an exception. Validity is checked with validate() during DAG-bagging.
        return not all(_is_schedule_fixed(c) for c in self._expressions)

    def _get_next(self, current: DateTime) -> DateTime:
        """Get the first schedule after specified time, with DST fixed."""
        naive = make_naive(current, self._timezone)
        crons = {croniter(expression, start_time=naive) for expression in self._expressions}
        earliest_datetime = min(cron.get_next(datetime.datetime, current) for cron in crons)
        if is_localized(earliest_datetime):
            earliest_datetime = make_naive(earliest_datetime)
        if not self._should_fix_dst:
            return convert_to_utc(make_aware(earliest_datetime, self._timezone))
        delta = earliest_datetime - naive
        return convert_to_utc(current.in_timezone(self._timezone) + delta)

    def _get_prev(self, current: DateTime) -> DateTime:
        """Get the first schedule before specified time, with DST fixed."""
        naive = make_naive(current, self._timezone)
        crons = {croniter(expression, start_time=current) for expression in self._expressions}
        latest_datetime = max(cron.get_prev(datetime.datetime) for cron in crons)
        if is_localized(latest_datetime):
            latest_datetime = make_naive(latest_datetime)
        if not self._should_fix_dst:
            return convert_to_utc(make_aware(latest_datetime, self._timezone))
        delta = naive - latest_datetime
        return convert_to_utc(current.in_timezone(self._timezone) - delta)

    def _align_to_next(self, current: DateTime) -> DateTime:
        """Get the next scheduled time.

        This is ``current + interval``, unless ``current`` falls right on the
        interval boundary, when ``current`` is returned.
        """
        next_time = self._get_next(current)
        if self._get_prev(next_time) != current:
            return next_time
        return current

    def _align_to_prev(self, current: DateTime) -> DateTime:
        """Get the prev scheduled time.

        This is ``current - interval``, unless ``current`` falls right on the
        interval boundary, when ``current`` is returned.
        """
        prev_time = self._get_prev(current)
        if self._get_next(prev_time) != current:
            return prev_time
        return current
