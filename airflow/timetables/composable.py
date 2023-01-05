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

from typing import Any

from pendulum import DateTime

from airflow.timetables._cron import CronMixin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.strategy_protocol import IntervalStrategy, ManualStrategy, RunStrategy


def compose_timetable(
    run_strategy_cls: type[RunStrategy],
    interval_strategy_cls: type[IntervalStrategy],
    manual_strategy_cls: type[ManualStrategy],
):
    """
    This method allows you to compose timetables out of three parts: a RunStrategy that determines
    when DAG Runs are scheduled, an IntervalStrategy for determining regularly scheduled intervals,
    and a ManualStrategy for setting the interval when a manual DAG Run is triggered.
    """
    class ComposedTimetable(Timetable):
        """
        A Timetable assembled from a RunStrategy, IntervalStrategy, and ManualStrategy
        """

        def __init__(self, run_args, interval_args, manual_args):
            self.run_strategy = run_strategy_cls(run_args)
            self.interval_strategy = interval_strategy_cls(interval_args)
            self.manual_strategy = manual_strategy_cls(manual_args)

            self.run_args = run_args
            self.interval_strategy = interval_args
            self.manual_strategy = manual_args

        @classmethod
        def deserialize(cls, data: dict[str, Any]) -> Timetable:
            return cls(**data)

        def serialize(self) -> dict[str, Any]:
            return {
                "run_args": self.run_strategy.serialize(),
                "interval_args": self.interval_strategy.serialize(),
                "manual_args": self.manual_strategy.serialize(),
            }

        def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
            # It's possible that the setting of manual run data interval may depend on the regularly scheduled
            # Dag Runs, so we make the run strategy and interval strategy arguments
            return self.manual_strategy.calculate_manual_run(
                run_after, self.run_strategy, self.interval_strategy
            )

        def next_dagrun_info(
            self,
            *,
            last_automated_data_interval: DataInterval | None,
            restriction: TimeRestriction,
        ) -> DagRunInfo | None:
            run_after: DateTime = self.run_strategy.calculate_run_after(
                last_automated_data_interval, restriction
            )
            interval = self.interval_strategy.calculate_interval(
                last_automated_data_interval, restriction, run_after
            )

            return DagRunInfo(run_after=run_after, data_interval=interval)

    return ComposedTimetable


class CronRunStrategy(CronMixin, RunStrategy):
    def calculate_run_after(self, last_automated_data_interval, restriction):
        if restriction.catchup:
            if last_automated_data_interval is None:
                if restriction.earliest is None:
                    return None
                next_start_time = self._align_to_next(restriction.earliest)
            else:
                next_start_time = self._get_next(last_automated_data_interval.end)
        else:
            current_time = DateTime.utcnow()
            if restriction.earliest is not None and current_time < restriction.earliest:
                next_start_time = self._align_to_next(restriction.earliest)
            else:
                next_start_time = self._align_to_next(current_time)
        if restriction.latest is not None and restriction.latest < next_start_time:
            return None


class LastToNowIntervalStrategy(IntervalStrategy):
    def calculate_interval(self, last_automated_data_interval, restriction, run_after):
        last_scheduled_run = last_automated_data_interval["end"] | run_after
        return DataInterval(last_scheduled_run, run_after)


class ExactIntervalStrategy(IntervalStrategy):
    def calculate_interval(self, last_automated_data_interval, restriction, run_after):
        return DataInterval.exact(run_after)


class ExactManualStrategy(ManualStrategy):
    def calculate_manual_run(self, run_after, run_strategy, interval_strategy):
        return DataInterval.exact(run_after)


CronExactTimetable = compose_timetable(CronRunStrategy, ExactIntervalStrategy, ExactManualStrategy)
CronLastToNowTimetable = compose_timetable(CronRunStrategy, LastToNowIntervalStrategy, ExactManualStrategy)
