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

from airflow.sdk.bases.timetable import BaseTimetable


class NullTimetable(BaseTimetable):
    """
    Timetable that never schedules anything.

    This corresponds to ``schedule=None``.
    """

    can_be_scheduled = False


class OnceTimetable(BaseTimetable):
    """
    Timetable that schedules the execution once as soon as possible.

    This corresponds to ``schedule="@once"``.
    """


class ContinuousTimetable(BaseTimetable):
    """
    Timetable that schedules continually, while still respecting start_date and end_date.

    This corresponds to ``schedule="@continuous"``.
    """

    active_runs_limit = 1


class PartitionAtRuntime(BaseTimetable):
    """
    Marker timetable indicating that partition key(s) are determined at runtime.

    Use ``schedule=PartitionAtRuntime()`` to signal that tasks in this Dag will
    set partition keys on their outlet events at execution time, rather than
    deriving them from a cron expression or an upstream asset event.

    Like ``schedule=None``, the Dag is not scheduled automatically — it must be
    triggered externally (manually or via the API). The difference is semantic:
    it tells readers and tooling that the Dag is expected to emit partitioned
    asset events whose keys are discovered at runtime via
    ``outlet_events[asset].partition_keys``.
    """

    can_be_scheduled = False
