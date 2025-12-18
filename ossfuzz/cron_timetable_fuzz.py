#!/usr/bin/python3
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

import datetime
import os
import sys

import atheris
import pendulum

os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow")

_CRON_SEEDS = [
    "@hourly",
    "@daily",
    "@weekly",
    "0 0 * * *",
    "*/5 * * * *",
    "30 21 * * 5",
]
_TZ_SEEDS = [
    "UTC",
    "America/New_York",
    "Europe/Paris",
    "Asia/Tokyo",
]

with atheris.instrument_imports(include=["airflow"], enable_loader_override=False):
    from airflow._shared.timezones.timezone import parse_timezone
    from airflow.timetables.base import TimeRestriction
    from airflow.timetables.interval import CronDataIntervalTimetable
    from airflow.timetables.trigger import CronTriggerTimetable, MultipleCronTriggerTimetable


def _consume_tz(fdp: atheris.FuzzedDataProvider) -> str:
    tz = fdp.PickValueInList(_TZ_SEEDS)
    if fdp.ConsumeBool():
        tz = fdp.ConsumeString(64) or tz
    return tz


def _consume_cron(fdp: atheris.FuzzedDataProvider) -> str:
    cron = fdp.PickValueInList(_CRON_SEEDS)
    if fdp.ConsumeBool():
        cron = fdp.ConsumeString(128) or cron
    return cron


def _consume_restriction(fdp: atheris.FuzzedDataProvider, tz: str) -> TimeRestriction:
    tzinfo = parse_timezone(tz)
    base = pendulum.datetime(2020, 1, 1, tz=tzinfo)
    earliest = base.add(days=fdp.ConsumeIntInRange(-30, 30)) if fdp.ConsumeBool() else None
    latest = None
    if fdp.ConsumeBool():
        latest = base.add(days=fdp.ConsumeIntInRange(-30, 30))
    return TimeRestriction(earliest=earliest, latest=latest, catchup=fdp.ConsumeBool())


def TestInput(input_bytes: bytes):
    if len(input_bytes) > 2048:
        return

    fdp = atheris.FuzzedDataProvider(input_bytes)
    cron = _consume_cron(fdp)
    tz = _consume_tz(fdp)

    try:
        restriction = _consume_restriction(fdp, tz)
    except Exception:
        return

    try:
        data_tt = CronDataIntervalTimetable(cron, timezone=tz)
        _ = data_tt.summary
        _ = data_tt.serialize()
        _ = data_tt.infer_manual_data_interval(run_after=pendulum.now(parse_timezone(tz)))
        _ = data_tt.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
    except Exception:
        pass

    try:
        interval = datetime.timedelta(seconds=fdp.ConsumeIntInRange(0, 86400))
        run_immediately: bool | datetime.timedelta
        if fdp.ConsumeBool():
            run_immediately = fdp.ConsumeBool()
        else:
            run_immediately = datetime.timedelta(seconds=fdp.ConsumeIntInRange(0, 3600))
        trig_tt = CronTriggerTimetable(
            cron,
            timezone=tz,
            interval=interval,
            run_immediately=run_immediately,
        )
        _ = trig_tt.summary
        _ = trig_tt.serialize()
        _ = trig_tt.infer_manual_data_interval(run_after=pendulum.now(parse_timezone(tz)))
        _ = trig_tt.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
    except Exception:
        pass

    try:
        crons = [cron]
        for _ in range(fdp.ConsumeIntInRange(0, 2)):
            crons.append(_consume_cron(fdp))
        multi = MultipleCronTriggerTimetable(*crons, timezone=tz)
        _ = multi.summary
        _ = multi.serialize()
        _ = multi.infer_manual_data_interval(run_after=pendulum.now(parse_timezone(tz)))
        _ = multi.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
    except Exception:
        return


def main():
    atheris.Setup(sys.argv, TestInput, enable_python_coverage=True)
    atheris.Fuzz()


if __name__ == "__main__":
    main()

