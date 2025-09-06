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

import collections
import datetime
from collections.abc import Iterator
from typing import Literal, cast

import sqlalchemy as sa
import structlog
from croniter.croniter import croniter
from pendulum import DateTime
from sqlalchemy.engine import Row
from sqlalchemy.orm import Session

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.parameters import RangeFilter
from airflow.api_fastapi.core_api.datamodels.ui.calendar import (
    CalendarTimeRangeCollectionResponse,
    CalendarTimeRangeResponse,
)
from airflow.models.dagrun import DagRun
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.timetables._cron import CronMixin
from airflow.timetables.base import DataInterval, TimeRestriction
from airflow.timetables.simple import ContinuousTimetable

log = structlog.get_logger(logger_name=__name__)


class CalendarService:
    """Service class for calendar-related operations."""

    MAX_PLANNED_RUNS: int = 2000

    def get_calendar_data(
        self,
        dag_id: str,
        session: Session,
        dag: SerializedDAG,
        logical_date: RangeFilter,
        granularity: Literal["hourly", "daily"] = "daily",
    ) -> CalendarTimeRangeCollectionResponse:
        """
        Get calendar data for a DAG including historical and planned runs.

        Args:
            dag_id: The DAG ID
            session: Database session
            dag: The DAG object
            logical_date: Date range filter
            granularity: Time granularity ("hourly" or "daily")

        Returns:
            List of calendar time range results
        """
        historical_data, raw_dag_states = self._get_historical_dag_runs(
            dag_id,
            session,
            logical_date,
            granularity,
        )

        planned_data = self._get_planned_dag_runs(dag, raw_dag_states, logical_date, granularity)

        all_data = historical_data + planned_data
        return CalendarTimeRangeCollectionResponse(
            total_entries=len(all_data),
            dag_runs=all_data,
        )

    def _get_historical_dag_runs(
        self,
        dag_id: str,
        session: Session,
        logical_date: RangeFilter,
        granularity: Literal["hourly", "daily"],
    ) -> tuple[list[CalendarTimeRangeResponse], list[Row]]:
        """Get historical DAG runs from the database."""
        dialect = session.bind.dialect.name

        time_expression = self._get_time_truncation_expression(DagRun.logical_date, granularity, dialect)

        select_stmt = (
            sa.select(
                time_expression.label("datetime"),
                DagRun.state,
                sa.func.max(DagRun.data_interval_start).label("data_interval_start"),
                sa.func.max(DagRun.data_interval_end).label("data_interval_end"),
                sa.func.count("*").label("count"),
            )
            .where(DagRun.dag_id == dag_id)
            .group_by(time_expression, DagRun.state)
            .order_by(time_expression.asc())
        )

        select_stmt = logical_date.to_orm(select_stmt)
        dag_states = session.execute(select_stmt).all()

        calendar_results = [
            CalendarTimeRangeResponse(
                # ds.datetime in sqlite and mysql is a string, in postgresql it is a datetime
                date=ds.datetime,
                state=ds.state,
                count=ds.count,
            )
            for ds in dag_states
        ]

        return calendar_results, dag_states

    def _get_planned_dag_runs(
        self,
        dag: SerializedDAG,
        raw_dag_states: list[Row],
        logical_date: RangeFilter,
        granularity: Literal["hourly", "daily"],
    ) -> list[CalendarTimeRangeResponse]:
        """Get planned DAG runs based on the DAG's timetable."""
        if not self._should_calculate_planned_runs(dag, raw_dag_states):
            return []

        last_data_interval = self._get_last_data_interval(raw_dag_states)
        if not last_data_interval:
            return []

        year = last_data_interval.end.year
        restriction = TimeRestriction(
            timezone.coerce_datetime(dag.start_date) if dag.start_date else None,
            timezone.coerce_datetime(dag.end_date) if dag.end_date else None,
            False,
        )

        if isinstance(dag.timetable, CronMixin):
            return self._calculate_cron_planned_runs(dag, last_data_interval, year, logical_date, granularity)
        return self._calculate_timetable_planned_runs(
            dag, last_data_interval, year, restriction, logical_date, granularity
        )

    def _should_calculate_planned_runs(self, dag: SerializedDAG, raw_dag_states: list[Row]) -> bool:
        """Check if we should calculate planned runs."""
        return (
            bool(raw_dag_states)
            and bool(raw_dag_states[-1].data_interval_start)
            and bool(raw_dag_states[-1].data_interval_end)
            and not isinstance(dag.timetable, ContinuousTimetable)
        )

    def _get_last_data_interval(self, raw_dag_states: list[Row]) -> DataInterval | None:
        """Extract the last data interval from raw database results."""
        if not raw_dag_states:
            return None

        last_state = raw_dag_states[-1]
        if not (last_state.data_interval_start and last_state.data_interval_end):
            return None

        return DataInterval(
            timezone.coerce_datetime(last_state.data_interval_start),
            timezone.coerce_datetime(last_state.data_interval_end),
        )

    def _calculate_cron_planned_runs(
        self,
        dag: SerializedDAG,
        last_data_interval: DataInterval,
        year: int,
        logical_date: RangeFilter,
        granularity: Literal["hourly", "daily"],
    ) -> list[CalendarTimeRangeResponse]:
        """Calculate planned runs for cron-based timetables."""
        dates: dict[datetime.datetime, int] = collections.Counter()

        dates_iter: Iterator[datetime.datetime | None] = croniter(
            cast("CronMixin", dag.timetable)._expression,
            start_time=last_data_interval.end,
            ret_type=datetime.datetime,
        )

        for dt in dates_iter:
            if dt is None or dt.year != year:
                break
            if dag.end_date and dt > dag.end_date:
                break
            if not self._is_date_in_range(dt, logical_date):
                continue

            dates[self._truncate_datetime_for_granularity(dt, granularity)] += 1

        return [
            CalendarTimeRangeResponse(date=dt, state="planned", count=count) for dt, count in dates.items()
        ]

    def _calculate_timetable_planned_runs(
        self,
        dag: SerializedDAG,
        last_data_interval: DataInterval,
        year: int,
        restriction: TimeRestriction,
        logical_date: RangeFilter,
        granularity: Literal["hourly", "daily"],
    ) -> list[CalendarTimeRangeResponse]:
        """Calculate planned runs for generic timetables."""
        dates: dict[datetime.datetime, int] = collections.Counter()
        prev_logical_date = DateTime.min
        total_planned = 0

        while total_planned < self.MAX_PLANNED_RUNS:
            curr_info = dag.timetable.next_dagrun_info(
                last_automated_data_interval=last_data_interval,
                restriction=restriction,
            )

            if curr_info is None:  # No more DAG runs to schedule
                break
            if curr_info.logical_date <= prev_logical_date:  # Timetable not progressing, stopping
                break
            if curr_info.logical_date.year != year:  # Crossed year boundary
                break

            if not self._is_date_in_range(curr_info.logical_date, logical_date):
                last_data_interval = curr_info.data_interval
                prev_logical_date = curr_info.logical_date
                total_planned += 1
                continue

            last_data_interval = curr_info.data_interval
            dt = self._truncate_datetime_for_granularity(curr_info.logical_date, granularity)
            dates[dt] += 1
            prev_logical_date = curr_info.logical_date
            total_planned += 1

        return [
            CalendarTimeRangeResponse(date=dt, state="planned", count=count) for dt, count in dates.items()
        ]

    def _get_time_truncation_expression(
        self,
        column: sa.Column,
        granularity: Literal["hourly", "daily"],
        dialect: str,
    ) -> sa.Column:
        """
        Get database-specific time truncation expression for SQLAlchemy.

        We want to return always timestamp for both hourly and daily truncation.
        Unfortunately different databases have different functions for truncating datetime, so we need to handle
        them separately.

        Args:
            column: The datetime column to truncate
            granularity: Either "hourly" or "daily"
            dialect: Database dialect ("postgresql", "mysql", "sqlite")

        Returns:
            SQLAlchemy expression for time truncation

        Raises:
            ValueError: If the dialect is not supported
        """
        if granularity == "hourly":
            if dialect == "postgresql":
                expression = sa.func.date_trunc("hour", column)
            elif dialect == "mysql":
                expression = sa.func.date_format(column, "%Y-%m-%dT%H:00:00Z")
            elif dialect == "sqlite":
                expression = sa.func.strftime("%Y-%m-%dT%H:00:00Z", column)
            else:
                raise ValueError(f"Unsupported dialect: {dialect}")
        else:
            if dialect == "postgresql":
                expression = sa.func.timezone("UTC", sa.func.cast(sa.func.cast(column, sa.Date), sa.DateTime))
            elif dialect == "mysql":
                expression = sa.func.date_format(column, "%Y-%m-%dT%00:00:00Z")
            elif dialect == "sqlite":
                expression = sa.func.strftime("%Y-%m-%dT00:00:00Z", column)
            else:
                raise ValueError(f"Unsupported dialect: {dialect}")
        return expression

    def _truncate_datetime_for_granularity(
        self,
        dt: datetime.datetime,
        granularity: Literal["hourly", "daily"],
    ) -> datetime.datetime:
        """
        Truncate datetime based on granularity for planned tasks grouping.

        Args:
            dt: The datetime to truncate
            granularity: Either "hourly" or "daily"

        Returns:
            Truncated datetime
        """
        if granularity == "hourly":
            return dt.replace(minute=0, second=0, microsecond=0)
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    def _is_date_in_range(self, dt: datetime.datetime, logical_date: RangeFilter) -> bool:
        """Check if a date is within the specified range filter."""
        if not logical_date.value:
            return True

        if logical_date.value.lower_bound_gte and dt < logical_date.value.lower_bound_gte:
            return False
        if logical_date.value.lower_bound_gt and dt <= logical_date.value.lower_bound_gt:
            return False
        if logical_date.value.upper_bound_lte and dt > logical_date.value.upper_bound_lte:
            return False
        if logical_date.value.upper_bound_lt and dt >= logical_date.value.upper_bound_lt:
            return False

        return True
