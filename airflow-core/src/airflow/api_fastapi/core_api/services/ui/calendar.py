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
from datetime import datetime, timedelta
from typing import Literal, cast

import sqlalchemy as sa
import structlog
from sqlalchemy.engine import Row
from sqlalchemy.orm import InstrumentedAttribute, Session

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.parameters import Range, RangeFilter
from airflow.api_fastapi.core_api.datamodels.ui.calendar import (
    CalendarDeadlineCollectionResponse,
    CalendarDeadlineResponse,
    CalendarTimeRangeCollectionResponse,
    CalendarTimeRangeResponse,
)
from airflow.models.dagrun import DagRun
from airflow.models.deadline import Deadline
from airflow.serialization.definitions.dag import SerializedDAG
from airflow.timetables._cron import CronMixin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction
from airflow.utils.sqlalchemy import get_dialect_name
from airflow.utils.types import DagRunType

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
        historical_dag_runs = self._get_historical_dag_runs(dag_id, session, date_filter, granularity)

        # Planned runs extend from the latest run of the Dag, regardless of the displayed date range,
        # so that ranges without any historical runs (e.g. a future year) still show planned runs.
        latest_run = self._get_latest_dag_run(dag_id, session)
        planned_dag_runs = self._get_planned_dag_runs(dag, latest_run, date_filter, granularity)

        all_dag_runs = historical_dag_runs + planned_dag_runs
        return CalendarTimeRangeCollectionResponse(
            total_entries=len(all_dag_runs),
            dag_runs=all_dag_runs,
        )

    def _get_historical_dag_runs(
        self,
        dag_id: str,
        session: Session,
        date_filter: RangeFilter,
        granularity: Literal["hourly", "daily"],
    ) -> list[CalendarTimeRangeResponse]:
        """
        Fetch historical Dag runs.

        :param dag_id: The Dag ID
        :param session: Database session
        :param date_filter: Filter for specific dates
        :param granularity: hourly or daily
        :return:
        """
        if date_filter.value:
            lower_bounds = [
                bound
                for bound in (date_filter.value.lower_bound_gte, date_filter.value.lower_bound_gt)
                if bound is not None
            ]
            if lower_bounds and max(lower_bounds) > timezone.utcnow():
                # The lower boundary is in the future, therefore skip requesting historical dag runs.
                return []

        dialect = get_dialect_name(session)

        effective_date = sa.func.coalesce(DagRun.partition_date, DagRun.logical_date)
        time_expression = self._get_time_truncation_expression(effective_date, granularity, dialect)

        is_backfill_expression = sa.case(
            (DagRun.run_type == DagRunType.BACKFILL_JOB, sa.literal(True)),
            else_=sa.literal(False),
        )

        select_stmt = (
            sa.select(
                time_expression.label("datetime"),
                DagRun.state,
                is_backfill_expression.label("is_backfill"),
                sa.func.max(DagRun.data_interval_start).label("data_interval_start"),
                sa.func.max(DagRun.data_interval_end).label("data_interval_end"),
                sa.func.max(DagRun.run_after).label("run_after"),
                sa.func.max(DagRun.partition_date).label("partition_date"),
                sa.func.count("*").label("count"),
            )
            .where(DagRun.dag_id == dag_id)
            .group_by(time_expression, DagRun.state, is_backfill_expression)
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
                is_backfill=bool(ds._mapping["is_backfill"]),
            )
            for ds in dag_states
        ]

        return calendar_results

    def _get_latest_dag_run(self, dag_id: str, session: Session) -> Row | None:
        """
        Get the latest Dag run of the Dag (by partition date or logical date), ignoring any date filter.

        :param dag_id: The Dag ID
        :param session: Database session
        :return: The latest run, or None if the Dag has no runs
        """
        return session.execute(
            sa.select(
                DagRun.data_interval_start,
                DagRun.data_interval_end,
                DagRun.run_after,
                DagRun.partition_date,
            )
            .where(DagRun.dag_id == dag_id)
            .order_by(sa.func.coalesce(DagRun.partition_date, DagRun.logical_date).desc())
            .limit(1)
        ).first()

    def _get_planned_dag_runs(
        self,
        dag: SerializedDAG,
        last_state: Row | None,
        date_filter: RangeFilter,
        granularity: Literal["hourly", "daily"],
    ) -> list[CalendarTimeRangeResponse]:
        """Get planned Dag runs based on the Dag's timetable."""
        if last_state is None or not dag.timetable.periodic:
            return []

        if (
            not (last_state.data_interval_start and last_state.data_interval_end)
            and not last_state.partition_date
        ):
            return []

        if dag.timetable.partitioned:
            last_run_after = timezone.coerce_datetime(last_state.run_after)
            last_partition_date = timezone.coerce_datetime(last_state.partition_date)
            if not last_run_after or not last_partition_date:
                return []
            date_filter = self._get_filter_with_default_upper_bound(date_filter, last_partition_date.year)
            last_info = DagRunInfo(
                run_after=last_run_after,
                partition_date=last_partition_date,
                partition_key=None,
                data_interval=None,
            )
        else:
            last_data_interval = self._get_last_data_interval(last_state)
            if not last_data_interval:
                return []
            date_filter = self._get_filter_with_default_upper_bound(date_filter, last_data_interval.end.year)
            if isinstance(dag.timetable, CronMixin):
                return self._calculate_cron_planned_runs(dag, last_data_interval, date_filter, granularity)
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

        return self._calculate_timetable_planned_runs(dag, last_info, restriction, date_filter, granularity)

    def _get_filter_with_default_upper_bound(
        self, date_filter: RangeFilter, default_upper_bound_year: int
    ) -> RangeFilter:
        """
        Get the range filter with an upper bound, so planned runs do not extend indefinitely.

        Necessary when no upper bound is provided.

        :param date_filter: Date range filter of the displayed range
        :param default_upper_bound_year: Year of the last run
        :return: The given filter if it has an upper bound, otherwise a copy with one
        """
        value = date_filter.value or Range(
            lower_bound_gte=None, lower_bound_gt=None, upper_bound_lte=None, upper_bound_lt=None
        )
        if value.upper_bound_lte is not None or value.upper_bound_lt is not None:
            return date_filter
        return RangeFilter(
            value.model_copy(
                update={"upper_bound_lt": datetime(default_upper_bound_year + 1, 1, 1, tzinfo=timezone.utc)}
            ),
            date_filter.attribute,
        )

    def _get_last_data_interval(self, last_state: Row) -> DataInterval | None:
        """Extract the last data interval from the latest run."""
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
        date_filter: RangeFilter,
        granularity: Literal["hourly", "daily"],
    ) -> list[CalendarTimeRangeResponse]:
        """Calculate planned runs for cron-based timetables."""
        dates: dict[datetime, int] = collections.Counter()

        cron_timetable = cast("CronMixin", dag.timetable)

        # Skip straight to the displayed range instead of iterating over every tick in between.
        # _get_next returns ticks strictly after its input, so step back for an inclusive lower bound.
        start_times = [last_data_interval.end]
        if date_filter.value:
            if date_filter.value.lower_bound_gte:
                start_times.append(
                    timezone.coerce_datetime(date_filter.value.lower_bound_gte) - timedelta(seconds=1)
                )
            if date_filter.value.lower_bound_gt:
                start_times.append(timezone.coerce_datetime(date_filter.value.lower_bound_gt))
        dt = max(start_times)

        # Step with CronMixin._get_next so planned instants match the scheduler exactly,
        # including its DST gap/fold handling. Cap the iteration like
        # _calculate_timetable_planned_runs does; a high-frequency expression (e.g.
        # "* * * * *", or a seconds-resolution cron) would otherwise take hundreds of
        # thousands of steps before hitting the end of the range.
        for _ in range(self.MAX_PLANNED_RUNS):
            dt = cron_timetable._get_next(dt)
            if self._is_after_range(dt, date_filter):
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
            if self._is_after_range(effective_date, date_filter):
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

    def _is_after_range(self, dt: datetime, date_filter: RangeFilter) -> bool:
        """Check if a date is past the upper bound of the range filter."""
        if not date_filter.value:
            return False

        if date_filter.value.upper_bound_lte and dt > date_filter.value.upper_bound_lte:
            return True
        return bool(date_filter.value.upper_bound_lt and dt >= date_filter.value.upper_bound_lt)

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

    def get_deadline_calendar_data(
        self,
        dag_id: str,
        deadline_time: RangeFilter,
        granularity: Literal["hourly", "daily"] = "daily",
        *,
        session: Session,
    ) -> CalendarDeadlineCollectionResponse:
        """
        Get deadline calendar data for a Dag, aggregated by time bucket and missed status.

        Args:
            dag_id: The Dag ID
            session: Database session
            deadline_time: Date range filter for deadline_time
            granularity: Time granularity ("hourly" or "daily")

        Returns:
            Aggregated deadline counts per time bucket, split by missed/pending status
        """
        dialect = get_dialect_name(session)
        time_expression = self._get_time_truncation_expression(Deadline.deadline_time, granularity, dialect)

        select_stmt = (
            sa.select(
                time_expression.label("datetime"),
                Deadline.missed,
                sa.func.count("*").label("count"),
            )
            .join(Deadline.dagrun)
            .where(DagRun.dag_id == dag_id)
            .group_by(time_expression, Deadline.missed)
            .order_by(time_expression.asc(), Deadline.missed.asc())
        )

        select_stmt = deadline_time.to_orm(select_stmt)
        rows = session.execute(select_stmt).all()

        results = [
            CalendarDeadlineResponse(
                date=row.datetime,
                missed=row.missed,
                count=int(row._mapping["count"]),
            )
            for row in rows
        ]

        return CalendarDeadlineCollectionResponse(
            total_entries=len(results),
            deadlines=results,
        )
