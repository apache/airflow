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

import pendulum
from fastapi import Depends, HTTPException, Query, Request, status
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from airflow.api.common.mark_tasks import (
    set_dag_run_state_to_failed,
    set_dag_run_state_to_queued,
    set_dag_run_state_to_success,
)
from airflow.api_fastapi.common.db.common import get_session
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.assets import AssetEventCollectionResponse, AssetEventResponse
from airflow.api_fastapi.core_api.datamodels.dag_run import (
    DAGRunClearBody,
    DAGRunPatchBody,
    DAGRunPatchStates,
    DAGRunResponse,
    TriggerDAGRunPostBody,
)
from airflow.api_fastapi.core_api.datamodels.task_instances import (
    TaskInstanceCollectionResponse,
    TaskInstanceResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.models import DAG, DagModel, DagRun
from airflow.models.dag_version import DagVersion
from airflow.timetables.base import DataInterval
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

dag_run_router = AirflowRouter(tags=["DagRun"], prefix="/dags/{dag_id}/dagRuns")


@dag_run_router.get(
    "/{dag_run_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def get_dag_run(
    dag_id: str, dag_run_id: str, session: Annotated[Session, Depends(get_session)]
) -> DAGRunResponse:
    dag_run = session.scalar(select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id))
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )

    return DAGRunResponse.model_validate(dag_run, from_attributes=True)


@dag_run_router.delete(
    "/{dag_run_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def delete_dag_run(dag_id: str, dag_run_id: str, session: Annotated[Session, Depends(get_session)]):
    """Delete a DAG Run entry."""
    dag_run = session.scalar(select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id))

    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )

    session.delete(dag_run)


@dag_run_router.patch(
    "/{dag_run_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def patch_dag_run(
    dag_id: str,
    dag_run_id: str,
    patch_body: DAGRunPatchBody,
    session: Annotated[Session, Depends(get_session)],
    request: Request,
    update_mask: list[str] | None = Query(None),
) -> DAGRunResponse:
    """Modify a DAG Run."""
    dag_run = session.scalar(select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id))
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )

    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)

    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")

    if update_mask:
        data = patch_body.model_dump(include=set(update_mask))
    else:
        data = patch_body.model_dump()

    for attr_name, attr_value in data.items():
        if attr_name == "state":
            attr_value = getattr(patch_body, "state")
            if attr_value == DAGRunPatchStates.SUCCESS:
                set_dag_run_state_to_success(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
            elif attr_value == DAGRunPatchStates.QUEUED:
                set_dag_run_state_to_queued(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
            elif attr_value == DAGRunPatchStates.FAILED:
                set_dag_run_state_to_failed(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
        elif attr_name == "note":
            # Once Authentication is implemented in this FastAPI app,
            # user id will be added when updating dag run note
            # Refer to https://github.com/apache/airflow/issues/43534
            dag_run = session.get(DagRun, dag_run.id)
            if dag_run.dag_run_note is None:
                dag_run.note = (attr_value, None)
            else:
                dag_run.dag_run_note.content = attr_value

    dag_run = session.get(DagRun, dag_run.id)

    return DAGRunResponse.model_validate(dag_run, from_attributes=True)


@dag_run_router.get(
    "/{dag_run_id}/upstreamAssetEvents",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def get_upstream_asset_events(
    dag_id: str, dag_run_id: str, session: Annotated[Session, Depends(get_session)]
) -> AssetEventCollectionResponse:
    """If dag run is asset-triggered, return the asset events that triggered it."""
    dag_run: DagRun | None = session.scalar(
        select(DagRun).where(
            DagRun.dag_id == dag_id,
            DagRun.run_id == dag_run_id,
        )
    )
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )
    events = dag_run.consumed_asset_events
    return AssetEventCollectionResponse(
        asset_events=[
            AssetEventResponse.model_validate(asset_event, from_attributes=True) for asset_event in events
        ],
        total_entries=len(events),
    )


@dag_run_router.post(
    "/{dag_run_id}/clear", responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND])
)
def clear_dag_run(
    dag_id: str,
    dag_run_id: str,
    body: DAGRunClearBody,
    request: Request,
    session: Annotated[Session, Depends(get_session)],
) -> TaskInstanceCollectionResponse | DAGRunResponse:
    dag_run = session.scalar(select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id))
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )

    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)
    start_date = dag_run.logical_date
    end_date = dag_run.logical_date

    if body.dry_run:
        task_instances = dag.clear(
            start_date=start_date,
            end_date=end_date,
            task_ids=None,
            only_failed=False,
            dry_run=True,
            session=session,
        )

        return TaskInstanceCollectionResponse(
            task_instances=[
                TaskInstanceResponse.model_validate(ti, from_attributes=True) for ti in task_instances
            ],
            total_entries=len(task_instances),
        )
    else:
        dag.clear(
            start_date=dag_run.start_date,
            end_date=dag_run.end_date,
            task_ids=None,
            only_failed=False,
            session=session,
        )
        dag_run_cleared = session.scalar(select(DagRun).where(DagRun.id == dag_run.id))
        return DAGRunResponse.model_validate(dag_run_cleared, from_attributes=True)


@dag_run_router.post(
    "",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
)
def trigger_dag_run(
    dag_id, body: TriggerDAGRunPostBody, request: Request, session: Annotated[Session, Depends(get_session)]
):
    """Trigger a DAG."""
    dm = session.scalar(select(DagModel).where(DagModel.is_active, DagModel.dag_id == dag_id).limit(1))
    if not dm:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"DAG with dag_id: '{dag_id}' not found")

    if dm.has_import_errors:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"DAG with dag_id: '{dag_id}' has import errors and cannot be triggered",
        )

    logical_date = pendulum.instance(body.logical_date)
    run_id = body.dag_run_id
    dagrun_instance = session.scalar(
        select(DagRun)
        .where(
            DagRun.dag_id == dag_id,
            or_(DagRun.run_id == run_id, DagRun.logical_date == logical_date),
        )
        .limit(1)
    )
    if not dagrun_instance:
        dag: DAG = request.app.state.dag_bag.get_dag(dag_id)

        if body.data_interval_start and body.data_interval_end:
            data_interval = DataInterval(
                start=pendulum.instance(body.data_interval_start),
                end=pendulum.instance(body.data_interval_end),
            )
        else:
            data_interval = dag.timetable.infer_manual_data_interval(run_after=logical_date)
        dag_version = DagVersion.get_latest_version(dag.dag_id)
        dag_run = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            run_id=run_id,
            logical_date=logical_date,
            data_interval=data_interval,
            state=DagRunState.QUEUED,
            conf=body.conf,
            external_trigger=True,
            dag_version=dag_version,
            session=session,
            triggered_by=DagRunTriggeredByType.REST_API,
        )
        dag_run_note = body.note
        if dag_run_note:
            current_user_id = None  # refer to https://github.com/apache/airflow/issues/43534
            dag_run.note = (dag_run_note, current_user_id)
        return DAGRunResponse.model_validate(dag_run, from_attributes=True)

    if dagrun_instance.logical_date == logical_date:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"DAGRun with DAG ID: '{dag_id}' and "
            f"DAGRun logical date: '{logical_date.isoformat(sep=' ')}' already exists",
        )

    raise HTTPException(
        status.HTTP_409_CONFLICT,
        f"DAGRun with DAG ID: '{dag_id}' and DAGRun ID: '{body.dag_run_id}' already exists",
    )
