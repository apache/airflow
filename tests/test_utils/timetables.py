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

from airflow import settings
from airflow.timetables.base import DataInterval, Timetable
from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable


def cron_timetable(expr: str) -> CronDataIntervalTimetable:
    return CronDataIntervalTimetable(expr, settings.TIMEZONE)


def delta_timetable(delta) -> DeltaDataIntervalTimetable:
    return DeltaDataIntervalTimetable(delta)


class CustomSerializationTimetable(Timetable):
    def __init__(self, value: str):
        self.value = value

    @classmethod
    def deserialize(cls, data):
        return cls(data["value"])

    def __eq__(self, other) -> bool:
        """Only for testing purposes."""
        if not isinstance(other, CustomSerializationTimetable):
            return False
        return self.value == other.value

    def serialize(self):
        return {"value": self.value}

    @property
    def summary(self):
        return f"{type(self).__name__}({self.value!r})"

    def infer_manual_data_interval(self, *, run_after):
        raise DataInterval.exact(run_after)

    def next_dagrun_info(self, *, last_automated_data_interval, restriction):
        return None
