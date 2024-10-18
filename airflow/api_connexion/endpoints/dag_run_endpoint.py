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

from http import HTTPStatus
from typing import TYPE_CHECKING, Collection

import pendulum
from connexion import NoContent
from flask import g
from marshmallow import ValidationError
from sqlalchemy import delete, or_, select

from airflow.api.common.mark_tasks import (
    set_dag_run_state_to_failed,
    set_dag_run_state_to_queued,
    set_dag_run_state_to_success,
)
from airflow.api_connexion import security
from airflow.api_connexion.endpoints.request_dict import get_json_request_dict
from airflow.api_connexion.exceptions import AlreadyExists, BadRequest, NotFound
from airflow.api_connexion.parameters import (
    apply_sorting,
    check_limit,
    format_datetime,
    format_parameters,
)
from airflow.api_connexion.schemas.asset_schema import (
    AssetEventCollection,
    asset_event_collection_schema,
)
from airflow.api_connexion.schemas.dag_run_schema import (
    DAGRunCollection,
    DAGRunCollectionSchema,
    DAGRunSchema,
    clear_dagrun_form_schema,
    dagrun_collection_schema,
    dagrun_schema,
    dagruns_batch_form_schema,
    set_dagrun_note_form_schema,
    set_dagrun_state_form_schema,
)
from airflow.api_connexion.schemas.task_instance_schema import (
    TaskInstanceReferenceCollection,
    task_instance_reference_collection_schema,
)
from airflow.auth.managers.models.resource_details import DagAccessEntity
from airflow.exceptions import ParamValidationError
from airflow.models import DagModel, DagRun
from airflow.timetables.base import DataInterval
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.utils.api_migration import mark_fastapi_migration_done
from airflow.utils.db import get_query_count
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType
from airflow.www.decorators import action_logging
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

    from airflow.api_connexion.types import APIResponse


@mark_fastapi_migration_done
@security.requires_access_dag("DELETE", DagAccessEntity.RUN)
@provide_session
@action_logging
def delete_dag_run(*, dag_id: str, dag_run_id: str, session: Session = NEW_SESSION) -> APIResponse:
    """Delete a DAG Run."""
    deleted_count = session.execute(
        delete(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id)
    ).rowcount
    if deleted_count == 0:
        raise NotFound(detail=f"DAGRun with DAG ID: '{dag_id}' and DagRun ID: '{dag_run_id}' not found")
    return NoContent, HTTPStatus.NO_CONTENT


@mark_fastapi_migration_done
@security.requires_access_dag("GET", DagAccessEntity.RUN)
@provide_session
def get_dag_run(
    *, dag_id: str, dag_run_id: str, fields: Collection[str] | None = None, session: Session = NEW_SESSION
) -> APIResponse:
    """Get a DAG Run."""
    dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id))
    if dag_run is None:
        raise NotFound(
            "DAGRun not found",
            detail=f"DAGRun with DAG ID: '{dag_id}' and DagRun ID: '{dag_run_id}' not found",
        )
    try:
        # parse fields to Schema @post_dump
        dagrun_schema = DAGRunSchema(context={"fields": fields}) if fields else DAGRunSchema()
        return dagrun_schema.dump(dag_run)
    except ValueError as e:
        # Invalid fields
        raise BadRequest("DAGRunSchema error", detail=str(e))


@security.requires_access_dag("GET", DagAccessEntity.RUN)
@security.requires_access_asset("GET")
@provide_session
def get_upstream_asset_events(*, dag_id: str, dag_run_id: str, session: Session = NEW_SESSION) -> APIResponse:
    """If dag run is asset-triggered, return the asset events that triggered it."""
    dag_run: DagRun | None = session.scalar(
        select(DagRun).where(
            DagRun.dag_id == dag_id,
            DagRun.run_id == dag_run_id,
        )
    )
    if dag_run is None:
        raise NotFound(
            "DAGRun not found",
            detail=f"DAGRun with DAG ID: '{dag_id}' and DagRun ID: '{dag_run_id}' not found",
        )
    events = dag_run.consumed_dataset_events
    return asset_event_collection_schema.dump(
        AssetEventCollection(asset_events=events, total_entries=len(events))
    )


def _fetch_dag_runs(
    query: Select,
    *,
    end_date_gte: str | None,
    end_date_lte: str | None,
    execution_date_gte: str | None,
    execution_date_lte: str | None,
    start_date_gte: str | None,
    start_date_lte: str | None,
    updated_at_gte: str | None = None,
    updated_at_lte: str | None = None,
    limit: int | None,
    offset: int | None,
    order_by: str,
    session: Session,
) -> tuple[list[DagRun], int]:
    if start_date_gte:
        query = query.where(DagRun.start_date >= start_date_gte)
    if start_date_lte:
        query = query.where(DagRun.start_date <= start_date_lte)
    # filter execution date
    if execution_date_gte:
        query = query.where(DagRun.execution_date >= execution_date_gte)
    if execution_date_lte:
        query = query.where(DagRun.execution_date <= execution_date_lte)
    # filter end date
    if end_date_gte:
        query = query.where(DagRun.end_date >= end_date_gte)
    if end_date_lte:
        query = query.where(DagRun.end_date <= end_date_lte)
    # filter updated at
    if updated_at_gte:
        query = query.where(DagRun.updated_at >= updated_at_gte)
    if updated_at_lte:
        query = query.where(DagRun.updated_at <= updated_at_lte)

    total_entries = get_query_count(query, session=session)
    to_replace = {"dag_run_id": "run_id", "execution_date": "logical_date"}
    allowed_sort_attrs = [
        "id",
        "state",
        "dag_id",
        "execution_date",
        "dag_run_id",
        "start_date",
        "end_date",
        "updated_at",
        "external_trigger",
        "conf",
    ]
    query = apply_sorting(query, order_by, to_replace, allowed_sort_attrs)
    return session.scalars(query.offset(offset).limit(limit)).all(), total_entries


@security.requires_access_dag("GET", DagAccessEntity.RUN)
@format_parameters(
    {
        "start_date_gte": format_datetime,
        "start_date_lte": format_datetime,
        "execution_date_gte": format_datetime,
        "execution_date_lte": format_datetime,
        "end_date_gte": format_datetime,
        "end_date_lte": format_datetime,
        "updated_at_gte": format_datetime,
        "updated_at_lte": format_datetime,
        "limit": check_limit,
    }
)
@provide_session
def get_dag_runs(
    *,
    dag_id: str,
    start_date_gte: str | None = None,
    start_date_lte: str | None = None,
    execution_date_gte: str | None = None,
    execution_date_lte: str | None = None,
    end_date_gte: str | None = None,
    end_date_lte: str | None = None,
    updated_at_gte: str | None = None,
    updated_at_lte: str | None = None,
    state: list[str] | None = None,
    offset: int | None = None,
    limit: int | None = None,
    order_by: str = "id",
    fields: Collection[str] | None = None,
    session: Session = NEW_SESSION,
):
    """Get all DAG Runs."""
    query = select(DagRun)

    #  This endpoint allows specifying ~ as the dag_id to retrieve DAG Runs for all DAGs.
    if dag_id == "~":
        query = query.where(
            DagRun.dag_id.in_(get_auth_manager().get_permitted_dag_ids(methods=["GET"], user=g.user))
        )
    else:
        query = query.where(DagRun.dag_id == dag_id)

    if state:
        query = query.where(DagRun.state.in_(state))

    dag_run, total_entries = _fetch_dag_runs(
        query,
        end_date_gte=end_date_gte,
        end_date_lte=end_date_lte,
        execution_date_gte=execution_date_gte,
        execution_date_lte=execution_date_lte,
        start_date_gte=start_date_gte,
        start_date_lte=start_date_lte,
        updated_at_gte=updated_at_gte,
        updated_at_lte=updated_at_lte,
        limit=limit,
        offset=offset,
        order_by=order_by,
        session=session,
    )
    try:
        dagrun_collection_schema = (
            DAGRunCollectionSchema(context={"fields": fields}) if fields else DAGRunCollectionSchema()
        )
        return dagrun_collection_schema.dump(DAGRunCollection(dag_runs=dag_run, total_entries=total_entries))
    except ValueError as e:
        raise BadRequest("DAGRunCollectionSchema error", detail=str(e))


@security.requires_access_dag("GET", DagAccessEntity.RUN)
@provide_session
def get_dag_runs_batch(*, session: Session = NEW_SESSION) -> APIResponse:
    """Get list of DAG Runs."""
    body = get_json_request_dict()
    try:
        data = dagruns_batch_form_schema.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))

    readable_dag_ids = get_auth_manager().get_permitted_dag_ids(methods=["GET"], user=g.user)
    query = select(DagRun)
    if data.get("dag_ids"):
        dag_ids = set(data["dag_ids"]) & set(readable_dag_ids)
        query = query.where(DagRun.dag_id.in_(dag_ids))
    else:
        query = query.where(DagRun.dag_id.in_(readable_dag_ids))

    states = data.get("states")
    if states:
        query = query.where(DagRun.state.in_(states))

    dag_runs, total_entries = _fetch_dag_runs(
        query,
        end_date_gte=data["end_date_gte"],
        end_date_lte=data["end_date_lte"],
        execution_date_gte=data["execution_date_gte"],
        execution_date_lte=data["execution_date_lte"],
        start_date_gte=data["start_date_gte"],
        start_date_lte=data["start_date_lte"],
        limit=data["page_limit"],
        offset=data["page_offset"],
        order_by=data.get("order_by", "id"),
        session=session,
    )

    return dagrun_collection_schema.dump(DAGRunCollection(dag_runs=dag_runs, total_entries=total_entries))


@security.requires_access_dag("POST", DagAccessEntity.RUN)
@action_logging
@provide_session
def post_dag_run(*, dag_id: str, session: Session = NEW_SESSION) -> APIResponse:
    """Trigger a DAG."""
    dm = session.scalar(select(DagModel).where(DagModel.is_active, DagModel.dag_id == dag_id).limit(1))
    if not dm:
        raise NotFound(title="DAG not found", detail=f"DAG with dag_id: '{dag_id}' not found")
    if dm.has_import_errors:
        raise BadRequest(
            title="DAG cannot be triggered",
            detail=f"DAG with dag_id: '{dag_id}' has import errors",
        )
    try:
        post_body = dagrun_schema.load(get_json_request_dict(), session=session)
    except ValidationError as err:
        raise BadRequest(detail=str(err))

    logical_date = pendulum.instance(post_body["execution_date"])
    run_id = post_body["run_id"]
    dagrun_instance = session.scalar(
        select(DagRun)
        .where(
            DagRun.dag_id == dag_id,
            or_(DagRun.run_id == run_id, DagRun.execution_date == logical_date),
        )
        .limit(1)
    )
    if not dagrun_instance:
        try:
            dag = get_airflow_app().dag_bag.get_dag(dag_id)

            data_interval_start = post_body.get("data_interval_start")
            data_interval_end = post_body.get("data_interval_end")
            if data_interval_start and data_interval_end:
                data_interval = DataInterval(
                    start=pendulum.instance(data_interval_start),
                    end=pendulum.instance(data_interval_end),
                )
            else:
                data_interval = dag.timetable.infer_manual_data_interval(run_after=logical_date)

            dag_run = dag.create_dagrun(
                run_type=DagRunType.MANUAL,
                run_id=run_id,
                execution_date=logical_date,
                data_interval=data_interval,
                state=DagRunState.QUEUED,
                conf=post_body.get("conf"),
                external_trigger=True,
                dag_hash=get_airflow_app().dag_bag.dags_hash.get(dag_id),
                session=session,
                triggered_by=DagRunTriggeredByType.REST_API,
            )
            dag_run_note = post_body.get("note")
            if dag_run_note:
                current_user_id = get_auth_manager().get_user_id()
                dag_run.note = (dag_run_note, current_user_id)
            return dagrun_schema.dump(dag_run)
        except (ValueError, ParamValidationError) as ve:
            raise BadRequest(detail=str(ve))

    if dagrun_instance.execution_date == logical_date:
        raise AlreadyExists(
            detail=(
                f"DAGRun with DAG ID: '{dag_id}' and "
                f"DAGRun logical date: '{logical_date.isoformat(sep=' ')}' already exists"
            ),
        )

    raise AlreadyExists(detail=f"DAGRun with DAG ID: '{dag_id}' and DAGRun ID: '{run_id}' already exists")


@mark_fastapi_migration_done
@security.requires_access_dag("PUT", DagAccessEntity.RUN)
@provide_session
@action_logging
def update_dag_run_state(*, dag_id: str, dag_run_id: str, session: Session = NEW_SESSION) -> APIResponse:
    """Set a state of a dag run."""
    dag_run: DagRun | None = session.scalar(
        select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id)
    )
    if dag_run is None:
        error_message = f"Dag Run id {dag_run_id} not found in dag {dag_id}"
        raise NotFound(error_message)
    try:
        post_body = set_dagrun_state_form_schema.load(get_json_request_dict())
    except ValidationError as err:
        raise BadRequest(detail=str(err))

    state = post_body["state"]
    dag = get_airflow_app().dag_bag.get_dag(dag_id)
    if state == DagRunState.SUCCESS:
        set_dag_run_state_to_success(dag=dag, run_id=dag_run.run_id, commit=True)
    elif state == DagRunState.QUEUED:
        set_dag_run_state_to_queued(dag=dag, run_id=dag_run.run_id, commit=True)
    else:
        set_dag_run_state_to_failed(dag=dag, run_id=dag_run.run_id, commit=True)
    dag_run = session.get(DagRun, dag_run.id)
    return dagrun_schema.dump(dag_run)


@security.requires_access_dag("PUT", DagAccessEntity.RUN)
@action_logging
@provide_session
def clear_dag_run(*, dag_id: str, dag_run_id: str, session: Session = NEW_SESSION) -> APIResponse:
    """Clear a dag run."""
    dag_run: DagRun | None = session.scalar(
        select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id)
    )
    if dag_run is None:
        error_message = f"Dag Run id {dag_run_id} not found in dag   {dag_id}"
        raise NotFound(error_message)
    try:
        post_body = clear_dagrun_form_schema.load(get_json_request_dict())
    except ValidationError as err:
        raise BadRequest(detail=str(err))

    dry_run = post_body.get("dry_run", False)
    dag = get_airflow_app().dag_bag.get_dag(dag_id)
    start_date = dag_run.logical_date
    end_date = dag_run.logical_date

    if dry_run:
        task_instances = dag.clear(
            start_date=start_date,
            end_date=end_date,
            task_ids=None,
            only_failed=False,
            dry_run=True,
        )
        return task_instance_reference_collection_schema.dump(
            TaskInstanceReferenceCollection(task_instances=task_instances)
        )
    else:
        dag.clear(
            start_date=start_date,
            end_date=end_date,
            task_ids=None,
            only_failed=False,
        )
        dag_run = session.execute(select(DagRun).where(DagRun.id == dag_run.id)).scalar_one()
        return dagrun_schema.dump(dag_run)


@security.requires_access_dag("PUT", DagAccessEntity.RUN)
@action_logging
@provide_session
def set_dag_run_note(*, dag_id: str, dag_run_id: str, session: Session = NEW_SESSION) -> APIResponse:
    """Set the note for a dag run."""
    dag_run: DagRun | None = session.scalar(
        select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id)
    )
    if dag_run is None:
        error_message = f"Dag Run id {dag_run_id} not found in dag {dag_id}"
        raise NotFound(error_message)
    try:
        post_body = set_dagrun_note_form_schema.load(get_json_request_dict())
        new_note = post_body["note"]
    except ValidationError as err:
        raise BadRequest(detail=str(err))

    current_user_id = get_auth_manager().get_user_id()
    if dag_run.dag_run_note is None:
        dag_run.note = (new_note, current_user_id)
    else:
        dag_run.dag_run_note.content = new_note
        dag_run.dag_run_note.user_id = current_user_id
    session.commit()
    return dagrun_schema.dump(dag_run)
