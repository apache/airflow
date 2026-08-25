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

from collections.abc import Generator
from datetime import timedelta
from itertools import groupby
from typing import Annotated, Literal

import pendulum
from fastapi import Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy import false, func, select
from sqlalchemy.sql import Select

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import apply_filters_to_select
from airflow.api_fastapi.common.parameters import (
    FilterOptionEnum,
    FilterParam,
    QueryDagIdPatternSearch,
    QueryDagRunRunTypesFilter,
    QueryDagRunStateFilter,
    QueryTagsFilter,
    QueryTeamsFilter,
    RangeFilter,
    datetime_range_filter_factory,
    filter_param_factory,
    float_range_filter_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.time_schedule import TimeScheduleBatch, TimeScheduleItem
from airflow.api_fastapi.core_api.security import ReadableDagsFilterDep, requires_access_dag
from airflow.api_fastapi.core_api.services.ui.time_schedule import aggregate_time_schedule_items
from airflow.models import DagModel, DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState

time_schedule_router = AirflowRouter(prefix="/time-schedule", tags=["Time Schedule"])

_DAG_BATCH_SIZE = 25
_SUPPORTED_TIME_SCALES = (1, 5, 10, 15, 20, 30, 40, 50, 60)


def _build_run_item(dag_run: DagRun, *, label: str, is_time_scheduled: bool) -> TimeScheduleItem:
    start_date = dag_run.start_date or dag_run.run_after
    duration_ms = (dag_run.duration or 0) * 1000
    end_date = dag_run.end_date or start_date + timedelta(milliseconds=duration_ms)
    return TimeScheduleItem(
        dag_id=dag_run.dag_id,
        dag_run_id=dag_run.run_id,
        duration_ms=duration_ms,
        end_date=end_date,
        is_placeholder=False,
        is_planned=False,
        is_time_scheduled=is_time_scheduled,
        label=label,
        run_count=1,
        start_date=start_date,
        state=dag_run.state or DagRunState.QUEUED,
    )


def _build_selected_dags_query(
    *,
    dag_id_pattern: QueryDagIdPatternSearch,
    readable_dags_filter: ReadableDagsFilterDep,
    show_scheduled_only: bool,
    tags: QueryTagsFilter,
    teams: QueryTeamsFilter,
    timetable_type: FilterParam[list[str] | None],
) -> Select[tuple[DagModel]]:
    query = select(DagModel).where(DagModel.is_stale == false())
    query = apply_filters_to_select(
        statement=query,
        filters=[dag_id_pattern, tags, teams, timetable_type, readable_dags_filter],
    )
    if show_scheduled_only:
        query = query.where(DagModel.timetable_periodic.is_(True))
    return query


def _build_selected_runs_query(
    *,
    dag_ids_query: Select[tuple[DagModel]],
    duration_range: RangeFilter,
    limit: int,
    run_after: RangeFilter,
    run_type: QueryDagRunRunTypesFilter,
    start_date_range: RangeFilter,
    state: QueryDagRunStateFilter,
) -> Select[tuple[int, str]]:
    query = select(DagRun.id, DagRun.dag_id).where(
        DagRun.dag_id.in_(dag_ids_query.with_only_columns(DagModel.dag_id))
    )
    query = apply_filters_to_select(
        statement=query,
        filters=[run_after, start_date_range, duration_range, run_type, state],
    )
    return query.order_by(func.coalesce(DagRun.start_date, DagRun.run_after).desc()).limit(limit)


@time_schedule_router.get(
    "",
    response_class=StreamingResponse,
    response_model=TimeScheduleBatch,
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN))],
    responses={
        200: {
            "content": {"application/x-ndjson": {"schema": {"type": "string"}}},
            "description": "NDJSON stream of aggregated Time Schedule batches",
        }
    },
)
def get_time_schedule_stream(
    tags: QueryTagsFilter,
    teams: QueryTeamsFilter,
    dag_id_pattern: QueryDagIdPatternSearch,
    run_type: QueryDagRunRunTypesFilter,
    state: QueryDagRunStateFilter,
    readable_dags_filter: ReadableDagsFilterDep,
    run_after: Annotated[RangeFilter, Depends(datetime_range_filter_factory("run_after", DagRun))],
    start_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("start_date", DagRun))],
    duration_range: Annotated[RangeFilter, Depends(float_range_filter_factory("duration", DagRun))],
    timetable_type: Annotated[
        FilterParam[list[str] | None],
        Depends(filter_param_factory(DagModel.timetable_type, list[str], FilterOptionEnum.IN)),
    ],
    aggregation_mode: Literal["max", "mean", "min"] = "mean",
    limit: Annotated[int, Query(ge=1, le=5000)] = 200,
    show_scheduled_only: bool = True,
    time_scale: Annotated[
        int,
        Query(
            ge=min(_SUPPORTED_TIME_SCALES),
            le=max(_SUPPORTED_TIME_SCALES),
            json_schema_extra={"enum": _SUPPORTED_TIME_SCALES},
        ),
    ] = 60,
    timezone: str = "UTC",
    view_mode: Literal["day", "week"] = "day",
) -> StreamingResponse:
    """
    Stream server-filtered and aggregated Time Schedule data in Dag batches.

    This private UI endpoint returns newline-delimited ``TimeScheduleBatch`` JSON objects rather than raw
    Dag run rows. Each batch contains up to 25 Dags and is emitted after its database session has closed,
    allowing the UI to render progressively without a slow client retaining a database connection.

    Dag metadata filters and Dag run filters are applied before the bounded, newest-first Dag run selection.
    Periodic Dags without a selected run can produce planned items from their next run time and
    ``dagrun_timeout``. The endpoint is coupled to the Time Schedule UI and is not a stable public API.
    """
    if time_scale not in _SUPPORTED_TIME_SCALES:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            f"Unsupported time scale: {time_scale}",
        )
    try:
        pendulum.timezone(timezone)
    except (ValueError, TypeError) as error:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, f"Unknown timezone: {timezone}") from error

    dag_ids_query = _build_selected_dags_query(
        dag_id_pattern=dag_id_pattern,
        readable_dags_filter=readable_dags_filter,
        show_scheduled_only=show_scheduled_only,
        tags=tags,
        teams=teams,
        timetable_type=timetable_type,
    )
    selected_runs_query = _build_selected_runs_query(
        dag_ids_query=dag_ids_query,
        duration_range=duration_range,
        limit=limit,
        run_after=run_after,
        run_type=run_type,
        start_date_range=start_date_range,
        state=state,
    )

    def _generate() -> Generator[str, None, None]:
        # Release each database connection before yielding so a slow client does not
        # hold one for the lifetime of the stream.
        with create_session(scoped=False) as session:
            selected_runs = session.execute(selected_runs_query).all()
        run_ids_by_dag: dict[str, list[int]] = {}
        for run_id, dag_id in selected_runs:
            run_ids_by_dag.setdefault(dag_id, []).append(run_id)
        dag_ids_with_runs = sorted(run_ids_by_dag)

        for batch_start in range(0, len(dag_ids_with_runs), _DAG_BATCH_SIZE):
            dag_id_batch = dag_ids_with_runs[batch_start : batch_start + _DAG_BATCH_SIZE]
            run_id_batch = [run_id for dag_id in dag_id_batch for run_id in run_ids_by_dag[dag_id]]
            with create_session(scoped=False) as session:
                rows = session.execute(
                    select(DagRun, DagModel)
                    .join(DagModel, DagModel.dag_id == DagRun.dag_id)
                    .where(DagRun.id.in_(run_id_batch))
                    .order_by(DagRun.dag_id, func.coalesce(DagRun.start_date, DagRun.run_after).desc())
                ).all()

            batch_items: list[TimeScheduleItem] = []
            for _, dag_rows_iterator in groupby(rows, key=lambda row: row[0].dag_id):
                dag_rows = list(dag_rows_iterator)
                run_items = [
                    _build_run_item(
                        dag_run,
                        label=dag.dag_display_name,
                        is_time_scheduled=dag.timetable_periodic,
                    )
                    for dag_run, dag in dag_rows
                ]
                batch_items.extend(
                    aggregate_time_schedule_items(
                        aggregation_mode=aggregation_mode,
                        items=run_items,
                        time_scale=time_scale,
                        timezone=timezone,
                        view_mode=view_mode,
                    )
                )
            yield TimeScheduleBatch(dag_run_count=len(rows), items=batch_items).model_dump_json() + "\n"

        last_dag_id: str | None = None
        while True:
            with create_session(scoped=False) as session:
                dags_without_runs_query = dag_ids_query
                if dag_ids_with_runs:
                    dags_without_runs_query = dags_without_runs_query.where(
                        DagModel.dag_id.not_in(dag_ids_with_runs)
                    )
                if last_dag_id is not None:
                    dags_without_runs_query = dags_without_runs_query.where(DagModel.dag_id > last_dag_id)
                dags_without_runs = list(
                    session.scalars(dags_without_runs_query.order_by(DagModel.dag_id).limit(_DAG_BATCH_SIZE))
                )
                planned_dags = [
                    dag
                    for dag in dags_without_runs
                    if dag.timetable_periodic
                    and dag.timetable_summary is not None
                    and dag.next_dagrun_create_after is not None
                ]
                serialized_dags = {
                    serialized_dag.dag_id: serialized_dag.dag
                    for serialized_dag in SerializedDagModel.get_latest_serialized_dags(
                        dag_ids=[dag.dag_id for dag in planned_dags], session=session
                    )
                }

            if not dags_without_runs:
                break

            remaining_items: list[TimeScheduleItem] = []
            for dag in planned_dags:
                serialized_dag = serialized_dags.get(dag.dag_id)
                duration = serialized_dag.dagrun_timeout if serialized_dag is not None else None
                duration_ms = duration.total_seconds() * 1000 if duration is not None else 0
                start_date = dag.next_dagrun_create_after
                if start_date is None:
                    continue
                remaining_items.append(
                    TimeScheduleItem(
                        dag_id=dag.dag_id,
                        dag_run_id=f"{dag.dag_id}-planned",
                        duration_ms=duration_ms,
                        end_date=start_date + timedelta(milliseconds=duration_ms),
                        is_placeholder=False,
                        is_planned=True,
                        is_time_scheduled=True,
                        label=dag.dag_display_name,
                        run_count=0,
                        start_date=start_date,
                        state="planned",
                    )
                )

            if view_mode == "day" and not show_scheduled_only:
                placeholder_start = pendulum.now(timezone).start_of("day")
                planned_dag_ids = {dag.dag_id for dag in planned_dags}
                remaining_items.extend(
                    TimeScheduleItem(
                        dag_id=dag.dag_id,
                        dag_run_id=f"{dag.dag_id}-placeholder",
                        duration_ms=0,
                        end_date=None,
                        is_placeholder=True,
                        is_planned=False,
                        is_time_scheduled=False,
                        label=dag.dag_display_name,
                        run_count=0,
                        start_date=placeholder_start,
                        state="placeholder",
                    )
                    for dag in dags_without_runs
                    if dag.dag_id not in planned_dag_ids
                )

            aggregated_remaining_items = aggregate_time_schedule_items(
                aggregation_mode=aggregation_mode,
                items=[item for item in remaining_items if not item.is_placeholder],
                time_scale=time_scale,
                timezone=timezone,
                view_mode=view_mode,
            )
            placeholders = [item for item in remaining_items if item.is_placeholder]
            if aggregated_remaining_items or placeholders:
                yield (
                    TimeScheduleBatch(
                        dag_run_count=0, items=[*aggregated_remaining_items, *placeholders]
                    ).model_dump_json()
                    + "\n"
                )

            last_dag_id = dags_without_runs[-1].dag_id

    return StreamingResponse(content=_generate(), media_type="application/x-ndjson")
