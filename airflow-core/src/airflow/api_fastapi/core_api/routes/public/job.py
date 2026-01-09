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

from typing import Annotated

from fastapi import Depends, status
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from airflow.api_fastapi.common.db.common import (
    SessionDep,
    paginated_select,
)
from airflow.api_fastapi.common.parameters import (
    FilterParam,
    QueryLimit,
    QueryOffset,
    RangeFilter,
    SortParam,
    datetime_range_filter_factory,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.job import (
    JobCollectionResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import AccessView, requires_access_view
from airflow.jobs.job import Job, JobState

job_router = AirflowRouter(tags=["Job"], prefix="/jobs")


@job_router.get(
    "",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST]),
    dependencies=[Depends(requires_access_view(AccessView.JOBS))],
)
def get_jobs(
    start_date_range: Annotated[
        RangeFilter,
        Depends(datetime_range_filter_factory("start_date", Job)),
    ],
    end_date_range: Annotated[
        RangeFilter,
        Depends(datetime_range_filter_factory("end_date", Job)),
    ],
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                [
                    "id",
                    "dag_id",
                    "state",
                    "job_type",
                    "start_date",
                    "end_date",
                    "latest_heartbeat",
                    "executor_class",
                    "hostname",
                    "unixname",
                ],
                Job,
            ).dynamic_depends(default="id")
        ),
    ],
    session: SessionDep,
    state: Annotated[
        FilterParam[str | None], Depends(filter_param_factory(Job.state, str | None, filter_name="job_state"))
    ],
    job_type: Annotated[
        FilterParam[str | None],
        Depends(filter_param_factory(Job.job_type, str | None, filter_name="job_type")),
    ],
    hostname: Annotated[
        FilterParam[str | None],
        Depends(filter_param_factory(Job.hostname, str | None, filter_name="hostname")),
    ],
    executor_class: Annotated[
        FilterParam[str | None],
        Depends(filter_param_factory(Job.executor_class, str | None, filter_name="executor_class")),
    ],
    is_alive: bool | None = None,
) -> JobCollectionResponse:
    """Get all jobs."""
    base_select = (
        select(Job)
        .where(Job.state == JobState.RUNNING)
        .order_by(Job.latest_heartbeat.desc())
        .options(joinedload(Job.dag_model))
    )

    jobs_select, total_entries = paginated_select(
        statement=base_select,
        filters=[
            start_date_range,
            end_date_range,
            state,
            job_type,
            hostname,
            executor_class,
        ],
        order_by=order_by,
        limit=limit,
        offset=offset,
        session=session,
        return_total_entries=True,
    )
    jobs = session.scalars(jobs_select).all()

    if is_alive is not None:
        jobs = [job for job in jobs if job.is_alive()]

    return JobCollectionResponse(
        jobs=jobs,
        total_entries=total_entries,
    )
