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

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.trigger import CronTriggerTimetable


# [START custom_partition_timetable]
class ScheduledRuntimePartitionTimetable(CronTriggerTimetable):
    """
    A schedulable timetable whose partition key is decided at task runtime.

    Runs fire on the given cron cadence, exactly like an ordinary
    :class:`~airflow.timetables.trigger.CronTriggerTimetable`. The partition key,
    however, is not derived from the schedule: it is set while the producing task
    runs — typically after the task checks whether the period's source data has
    arrived — by calling ``outlet_events[self].add_partitions(...)``.

    This uses runtime partitioning on a regular cron schedule: the timetable stays
    schedulable (``can_be_scheduled`` is ``True``) yet sets
    ``partitioned_at_runtime = True`` so the partition key is deferred to task
    runtime. It differs from :class:`~airflow.sdk.PartitionedAtRuntime` (which also
    defers the key to runtime but never schedules a run on its own) and from
    :class:`~airflow.timetables.trigger.CronPartitionTimetable` (which works out the
    partition key from the cadence ahead of the run). ``partitioned`` stays
    ``False``: no partition key is worked out ahead of the run.

    Registering it via the ``AirflowPlugin.timetables`` registry makes it usable
    by Dag authors without modifying core Airflow.
    """

    partitioned_at_runtime = True


class ScheduledRuntimePartitionTimetablePlugin(AirflowPlugin):
    name = "scheduled_runtime_partition_timetable_plugin"
    timetables = [ScheduledRuntimePartitionTimetable]


# [END custom_partition_timetable]
