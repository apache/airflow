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

from dateutil.relativedelta import relativedelta

from airflow.sdk.bases.timetable import BaseTimetable
from airflow.sdk.definitions.timetables._cron import CronMixin
from airflow.sdk.definitions.timetables._delta import DeltaMixin

Delta = datetime.timedelta | relativedelta


class CronDataIntervalTimetable(CronMixin, BaseTimetable):
    """
    Timetable that schedules data intervals with a cron expression.

    This corresponds to ``schedule=<cron>``, where ``<cron>`` is either
    a five/six-segment representation, or one of ``cron_presets``.

    The implementation extends on croniter to add timezone awareness. This is
    because croniter works only with naive timestamps, and cannot consider DST
    when determining the next/previous time.

    Using this class is equivalent to supplying a cron expression dire

    Don't pass ``@once`` in here; use ``OnceTimetable`` instead.
    """


class DeltaDataIntervalTimetable(DeltaMixin, BaseTimetable):
    """
    Timetable that schedules data intervals with a time delta.

    This corresponds to ``schedule=<delta>``, where ``<delta>`` is
    either a ``datetime.timedelta`` or ``dateutil.relativedelta.relativedelta``
    instance.
    """
