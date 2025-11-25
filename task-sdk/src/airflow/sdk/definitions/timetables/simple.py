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

from airflow.sdk.definitions.timetables.base import BaseTimetable


class TrivialTimetable(BaseTimetable):
    """Some code reuse for "trivial" timetables that has nothing complex."""

    def __hash__(self) -> int:
        """As simple as reasonably possible, just to satisfy the linter."""
        return 0

    def __eq__(self, other: object) -> bool:
        """
        As long as *other* is of the same type.

        This is only for testing purposes and should not be relied on otherwise.
        """
        if not isinstance(other, type(self)):
            return NotImplemented
        return True


class NullTimetable(TrivialTimetable):
    """
    Timetable that never schedules anything.

    This corresponds to ``schedule=None``.
    """

    can_be_scheduled = False


class OnceTimetable(TrivialTimetable):
    """
    Timetable that schedules the execution once as soon as possible.

    This corresponds to ``schedule="@once"``.
    """


class ContinuousTimetable(TrivialTimetable):
    """
    Timetable that schedules continually, while still respecting start_date and end_date.

    This corresponds to ``schedule="@continuous"``.
    """

    active_runs_limit = 1
