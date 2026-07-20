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
from collections.abc import Iterator, Sequence
from datetime import datetime
from typing import Literal, cast

import sqlalchemy as sa
import structlog
from croniter.croniter import croniter
from sqlalchemy.engine import Row
from sqlalchemy.orm import InstrumentedAttribute, Session

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.parameters import RangeFilter
from airflow.api_fastapi.core_api.datamodels.ui.calendar import (
    CalendarTimeRangeCollectionResponse,
    CalendarTimeRangeResponse,
)
from airflow.models.dagrun import DagRun
from airflow.serialization.definitions.dag import SerializedDAG
from airflow.timetables._cron import CronMixin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction
from airflow.utils.sqlalchemy import get_dialect_name

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
        partition_date: RangeFilter,
        granularity: Literal["hourly", "daily"] = "daily",
    ) -> CalendarTimeRangeCollectionResponse:
        """
        Get calendar data for a Dag including historical and planned runs.

        Args:
            dag_id: The Dag ID
            session: Database session
            dag: The Dag object
            logical_date: Date range filter for logical_date
            partition_date: Date range filter for partition_date
            granularity: Time granularity ("hourly" or "daily")

        Returns:
            List of calendar time range results
        """
        date_filter = partition_date if partition_date.is_active() else logical_date
        historical_data, raw_dag_states = self._get_historical_dag_runs(
            dag_id,
            session,
            date_filter,
            granularity,
        )

        planned_data = self._get_planned_dag_runs(dag, raw_dag_states, date_filter, granularity)

        all_data = historical_data + planned_data
        return CalendarTimeRangeCollectionResponse(
            total_entries=len(all_data),
            dag_runs=all_data,
        )

    def _get_historical_dag_runs(
        self,
        dag_id: str,
        session: Session,
        date_filter: RangeFilter,
        granularity: Literal["hourly", "daily"],
    ) -> tuple[list[CalendarTimeRangeResponse], Sequence[Row]]:
        """Get historical Dag runs from the database."""
        dialect = get_dialect_name(session)

        effective_date = sa.func.coalesce(DagRun.partition_date, DagRun.logical_date)
        time_expression = self._get_time_truncation_expression(effective_date, granularity, dialect)

        select_stmt = (
            sa.select(
                time_expression.label("datetime"),
                DagRun.state,
                sa.func.max(DagRun.data_interval_start).label("data_interval_start"),
                sa.func.max(DagRun.data_interval_end).label("data_interval_end"),
                sa.func.max(DagRun.run_after).label("run_after"),
                sa.func.max(DagRun.partition_date).label("partition_date"),
                sa.func.count("*").label("count"),
            )
            .where(DagRun.dag_id == dag_id)
            .group_by(time_expression, DagRun.state)
            .order_by(time_expression.asc())
        )

        select_stmt = date_filter.to_orm(select_stmt)
        dag_states = session.execute(select_stmt).all()

        calendar_results = [
            CalendarTimeRangeResponse(
                # ds.datetime in sqlite and mysql is a string, in postgresql it is a datetime
                date=ds.datetime,
                state=ds.state,
                count=int(ds._mapping["count"]),
            )
            for ds in dag_states
        ]

        return calendar_results, dag_states

    def _get_planned_dag_runs(
        self,
        dag: SerializedDAG,
        raw_dag_states: Sequence[Row],
        date_filter: RangeFilter,
        granularity: Literal["hourly", "daily"],
    ) -> list[CalendarTimeRangeResponse]:
        """Get planned Dag runs based on the Dag's timetable."""
        if not self._should_calculate_planned_runs(dag, raw_dag_states):
            return []

        last_state = raw_dag_states[-1]

        if dag.timetable.partitioned:
            last_run_after = timezone.coerce_datetime(last_state.run_after)
            last_partition_date = timezone.coerce_datetime(last_state.partition_date)
            if not last_run_after or not last_partition_date:
                return []
            year = last_partition_date.year
            last_info = DagRunInfo(
                run_after=last_run_after,
                partition_date=last_partition_date,
                partition_key=None,
                data_interval=None,
            )
        else:
            last_data_interval = self._get_last_data_interval(raw_dag_states)
            if not last_data_interval:
                return []
            year = last_data_interval.end.year
            if isinstance(dag.timetable, CronMixin):
                return self._calculate_cron_planned_runs(
                    dag, last_data_interval, year, date_filter, granularity
                )
            last_info = DagRunInfo(
                run_after=last_data_interval.end,
                data_interval=last_data_interval,
                partition_date=None,
                partition_key=None,
            )

        restriction = TimeRestriction(
            timezone.coerce_datetime(dag.start_date) if dag.start_date else None,
            timezone.coerce_datetime(dag.end_date) if dag.end_date else None,
            False,
        )

        return self._calculate_timetable_planned_runs(
            dag, last_info, year, restriction, date_filter, granularity
        )

    def _should_calculate_planned_runs(self, dag: SerializedDAG, raw_dag_states: Sequence[Row]) -> bool:
        """Check if we should calculate planned runs."""
        if not raw_dag_states or not dag.timetable.periodic:
            return False
        last = raw_dag_states[-1]
        return bool(last.data_interval_start and last.data_interval_end) or bool(last.partition_date)

    def _get_last_data_interval(self, raw_dag_states: Sequence[Row]) -> DataInterval | None:
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
        date_filter: RangeFilter,
        granularity: Literal["hourly", "daily"],
    ) -> list[CalendarTimeRangeResponse]:
        """Calculate planned runs for cron-based timetables."""
        dates: dict[datetime, int] = collections.Counter()

        dates_iter: Iterator[datetime | None] = croniter(
            cast("CronMixin", dag.timetable)._expression,
            start_time=last_data_interval.end,
            ret_type=datetime,
        )

        for dt in dates_iter:
            if dt is None or dt.year != year:
                break
            if dag.end_date and dt > dag.end_date:
                break
            if not self._is_date_in_range(dt, date_filter):
                continue

            dates[self._truncate_datetime_for_granularity(dt, granularity)] += 1

        return [
            CalendarTimeRangeResponse(date=dt, state="planned", count=count) for dt, count in dates.items()
        ]

    def _calculate_timetable_planned_runs(
        self,
        dag: SerializedDAG,
        last_info: DagRunInfo,
        year: int,
        restriction: TimeRestriction,
        date_filter: RangeFilter,
        granularity: Literal["hourly", "daily"],
    ) -> list[CalendarTimeRangeResponse]:
        """Calculate planned runs for generic timetables."""
        dates: dict[datetime, int] = collections.Counter()
        prev_run_after = last_info.run_after
        total_planned = 0

        while total_planned < self.MAX_PLANNED_RUNS:
            curr_info = dag.timetable.next_dagrun_info_v2(
                last_dagrun_info=last_info,
                restriction=restriction,
            )

            if curr_info is None:
                break
            if curr_info.run_after <= prev_run_after:
                break

            effective_date = curr_info.partition_date or curr_info.logical_date
            if not effective_date:
                break
            if effective_date.year != year:
                break

            if not self._is_date_in_range(effective_date, date_filter):
                last_info = curr_info
                prev_run_after = curr_info.run_after
                total_planned += 1
                continue

            dt = self._truncate_datetime_for_granularity(effective_date, granularity)
            dates[dt] += 1
            last_info = curr_info
            prev_run_after = curr_info.run_after
            total_planned += 1

        return [
            CalendarTimeRangeResponse(date=dt, state="planned", count=count) for dt, count in dates.items()
        ]

    def _get_time_truncation_expression(
        self,
        column: InstrumentedAttribute[datetime | None] | sa.sql.elements.ColumnElement,
        granularity: Literal["hourly", "daily"],
        dialect: str | None,
    ) -> sa.sql.elements.ColumnElement:
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
        dt: datetime,
        granularity: Literal["hourly", "daily"],
    ) -> datetime:
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

    def _is_date_in_range(self, dt: datetime, date_filter: RangeFilter) -> bool:
        """Check if a date is within the specified range filter."""
        if not date_filter.value:
            return True

        if date_filter.value.lower_bound_gte and dt < date_filter.value.lower_bound_gte:
            return False
        if date_filter.value.lower_bound_gt and dt <= date_filter.value.lower_bound_gt:
            return False
        if date_filter.value.upper_bound_lte and dt > date_filter.value.upper_bound_lte:
            return False
        if date_filter.value.upper_bound_lt and dt >= date_filter.value.upper_bound_lt:
            return False

        return True
