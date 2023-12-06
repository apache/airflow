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
from typing import TYPE_CHECKING, Any

from cron_descriptor import CasingTypeEnum, ExpressionDescriptor, FormatException, MissingFieldException
from croniter import CroniterBadCronError, CroniterBadDateError, croniter
from pendulum.tz.timezone import Timezone

from airflow.exceptions import AirflowTimetableInvalid
from airflow.utils.dates import cron_presets
from airflow.utils.timezone import convert_to_utc, make_aware, make_naive

if TYPE_CHECKING:
    from pendulum import DateTime


def _covers_every_hour(cron: croniter) -> bool:
    """Check whether the given cron runs at least once an hour.

    This indicates whether we need to implement a workaround for (what I call)
    the "fold hour problem". Folding happens when a region switches time
    backwards, usually as a part of ending a DST period, causing a block of time
    to occur twice in the wall clock. This is indicated by the ``fold`` flag on
    datetime.

    As an example, Switzerland in 2023 ended DST on 3am (wall clock time, UTC+2)
    by dialing back the clock to 2am (UTC+1). So for (say) ``30 * * * *``, if
    the last run was 2:30am (UTC+2), the next needs to be 2:30am (UTC+1, folded)
    instead of 3:30am.

    While this technically happens for all runs (in such a timezone), we only
    really care about runs that happen at least once an hour, and can
    provide a somewhat reasonable rationale to skip the fold hour for things
    such as ``*/2`` (every two hour). So we try to *minially* peak into croniter
    internals to work around the issue.

    The check is simple since croniter internally normalizes things to ``*``.
    More edge cases can be added later as needed.

    See also: https://github.com/kiorky/croniter/issues/56.
    """
    return cron.expanded[1] == ["*"]


class CronMixin:
    """Mixin to provide interface to work with croniter."""

    def __init__(self, cron: str, timezone: str | Timezone) -> None:
        self._expression = cron_presets.get(cron, cron)

        if isinstance(timezone, str):
            timezone = Timezone(timezone)
        self._timezone = timezone

        try:
            descriptor = ExpressionDescriptor(
                expression=self._expression, casing_type=CasingTypeEnum.Sentence, use_24hour_time_format=True
            )
            # checking for more than 5 parameters in Cron and avoiding evaluation for now,
            # as Croniter has inconsistent evaluation with other libraries
            if len(croniter(self._expression).expanded) > 5:
                raise FormatException()
            interval_description: str = descriptor.get_description()
        except (CroniterBadCronError, FormatException, MissingFieldException):
            interval_description = ""
        self.description: str = interval_description

    def __eq__(self, other: Any) -> bool:
        """Both expression and timezone should match.

        This is only for testing purposes and should not be relied on otherwise.
        """
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._expression == other._expression and self._timezone == other._timezone

    @property
    def summary(self) -> str:
        return self._expression

    def validate(self) -> None:
        try:
            croniter(self._expression)
        except (CroniterBadCronError, CroniterBadDateError) as e:
            raise AirflowTimetableInvalid(str(e))

    def _get_next(self, current: DateTime) -> DateTime:
        """Get the first schedule after specified time, with DST fixed."""
        naive = make_naive(current, self._timezone)
        cron = croniter(self._expression, start_time=naive)
        scheduled = cron.get_next(datetime.datetime)
        if not _covers_every_hour(cron):
            return convert_to_utc(make_aware(scheduled, self._timezone))
        delta = scheduled - naive
        return convert_to_utc(current.in_timezone(self._timezone) + delta)

    def _get_prev(self, current: DateTime) -> DateTime:
        """Get the first schedule before specified time, with DST fixed."""
        naive = make_naive(current, self._timezone)
        cron = croniter(self._expression, start_time=naive)
        scheduled = cron.get_prev(datetime.datetime)
        if not _covers_every_hour(cron):
            return convert_to_utc(make_aware(scheduled, self._timezone))
        delta = naive - scheduled
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
