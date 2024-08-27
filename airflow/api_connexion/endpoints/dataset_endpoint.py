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
from typing import TYPE_CHECKING

from connexion import NoContent
from marshmallow import ValidationError
from sqlalchemy import delete, func, select
from sqlalchemy.orm import joinedload, subqueryload

from airflow.api_connexion import security
from airflow.api_connexion.endpoints.request_dict import get_json_request_dict
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_datetime, format_parameters
from airflow.api_connexion.schemas.asset_schema import (
    AssetCollection,
    AssetEventCollection,
    DagScheduleAssetReference,
    QueuedEvent,
    QueuedEventCollection,
    TaskOutletAssetReference,
    asset_collection_schema,
    asset_event_collection_schema,
    asset_event_schema,
    asset_schema,
    create_asset_event_schema,
    queued_event_collection_schema,
    queued_event_schema,
)
from airflow.assets import Asset
from airflow.assets.manager import asset_manager
from airflow.models.asset import AssetDagRunQueue, AssetEvent, AssetModel
from airflow.utils import timezone
from airflow.utils.db import get_query_count
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.www.decorators import action_logging
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_connexion.types import APIResponse

RESOURCE_EVENT_PREFIX = "dataset"


@security.requires_access_dataset("GET")
@provide_session
def get_dataset(*, uri: str, session: Session = NEW_SESSION) -> APIResponse:
    """Get an asset ."""
    asset = session.scalar(
        select(AssetModel)
        .where(AssetModel.uri == uri)
        .options(joinedload(AssetModel.consuming_dags), joinedload(AssetModel.producing_tasks))
    )
    if not asset:
        raise NotFound(
            "Asset not found",
            detail=f"The Asset with uri: `{uri}` was not found",
        )
    return asset_schema.dump(asset)


@security.requires_access_dataset("GET")
@format_parameters({"limit": check_limit})
@provide_session
def get_datasets(
    *,
    limit: int,
    offset: int = 0,
    uri_pattern: str | None = None,
    dag_ids: str | None = None,
    order_by: str = "id",
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get assets."""
    allowed_attrs = ["id", "uri", "created_at", "updated_at"]

    total_entries = session.scalars(select(func.count(AssetModel.id))).one()
    query = select(AssetModel)

    if dag_ids:
        dags_list = dag_ids.split(",")
        query = query.filter(
            (AssetModel.consuming_dags.any(DagScheduleAssetReference.dag_id.in_(dags_list)))
            | (AssetModel.producing_tasks.any(TaskOutletAssetReference.dag_id.in_(dags_list)))
        )
    if uri_pattern:
        query = query.where(AssetModel.uri.ilike(f"%{uri_pattern}%"))
    query = apply_sorting(query, order_by, {}, allowed_attrs)
    assets = session.scalars(
        query.options(subqueryload(AssetModel.consuming_dags), subqueryload(AssetModel.producing_tasks))
        .offset(offset)
        .limit(limit)
    ).all()
    return asset_collection_schema.dump(AssetCollection(datasets=assets, total_entries=total_entries))


@security.requires_access_dataset("GET")
@provide_session
@format_parameters({"limit": check_limit})
def get_dataset_events(
    *,
    limit: int,
    offset: int = 0,
    order_by: str = "timestamp",
    dataset_id: int | None = None,
    source_dag_id: str | None = None,
    source_task_id: str | None = None,
    source_run_id: str | None = None,
    source_map_index: int | None = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get asset events."""
    allowed_attrs = ["source_dag_id", "source_task_id", "source_run_id", "source_map_index", "timestamp"]

    query = select(AssetEvent)

    if dataset_id:
        query = query.where(AssetEvent.dataset_id == dataset_id)
    if source_dag_id:
        query = query.where(AssetEvent.source_dag_id == source_dag_id)
    if source_task_id:
        query = query.where(AssetEvent.source_task_id == source_task_id)
    if source_run_id:
        query = query.where(AssetEvent.source_run_id == source_run_id)
    if source_map_index:
        query = query.where(AssetEvent.source_map_index == source_map_index)

    query = query.options(subqueryload(AssetEvent.created_dagruns))

    total_entries = get_query_count(query, session=session)
    query = apply_sorting(query, order_by, {}, allowed_attrs)
    events = session.scalars(query.offset(offset).limit(limit)).all()
    return asset_event_collection_schema.dump(
        AssetEventCollection(dataset_events=events, total_entries=total_entries)
    )


def _generate_queued_event_where_clause(
    *,
    dag_id: str | None = None,
    dataset_id: int | None = None,
    uri: str | None = None,
    before: str | None = None,
    permitted_dag_ids: set[str] | None = None,
) -> list:
    """Get AssetDagRunQueue where clause."""
    where_clause = []
    if dag_id is not None:
        where_clause.append(AssetDagRunQueue.target_dag_id == dag_id)
    if dataset_id is not None:
        where_clause.append(AssetDagRunQueue.dataset_id == dataset_id)
    if uri is not None:
        where_clause.append(
            AssetDagRunQueue.dataset_id.in_(
                select(AssetModel.id).where(AssetModel.uri == uri),
            ),
        )
    if before is not None:
        where_clause.append(AssetDagRunQueue.created_at < format_datetime(before))
    if permitted_dag_ids is not None:
        where_clause.append(AssetDagRunQueue.target_dag_id.in_(permitted_dag_ids))
    return where_clause


@security.requires_access_dataset("GET")
@security.requires_access_dag("GET")
@provide_session
def get_dag_dataset_queued_event(
    *, dag_id: str, uri: str, before: str | None = None, session: Session = NEW_SESSION
) -> APIResponse:
    """Get a queued asset event for a DAG."""
    where_clause = _generate_queued_event_where_clause(dag_id=dag_id, uri=uri, before=before)
    adrq = session.scalar(
        select(AssetDagRunQueue)
        .join(AssetModel, AssetDagRunQueue.dataset_id == AssetModel.id)
        .where(*where_clause)
    )
    if adrq is None:
        raise NotFound(
            "Queue event not found",
            detail=f"Queue event with dag_id: `{dag_id}` and asset uri: `{uri}` was not found",
        )
    queued_event = {"created_at": adrq.created_at, "dag_id": dag_id, "uri": uri}
    return queued_event_schema.dump(queued_event)


@security.requires_access_dataset("DELETE")
@security.requires_access_dag("GET")
@provide_session
@action_logging
def delete_dag_dataset_queued_event(
    *, dag_id: str, uri: str, before: str | None = None, session: Session = NEW_SESSION
) -> APIResponse:
    """Delete a queued asset event for a DAG."""
    where_clause = _generate_queued_event_where_clause(dag_id=dag_id, uri=uri, before=before)
    delete_stmt = delete(AssetDagRunQueue).where(*where_clause).execution_options(synchronize_session="fetch")
    result = session.execute(delete_stmt)
    if result.rowcount > 0:
        return NoContent, HTTPStatus.NO_CONTENT
    raise NotFound(
        "Queue event not found",
        detail=f"Queue event with dag_id: `{dag_id}` and asset uri: `{uri}` was not found",
    )


@security.requires_access_dataset("GET")
@security.requires_access_dag("GET")
@provide_session
def get_dag_dataset_queued_events(
    *, dag_id: str, before: str | None = None, session: Session = NEW_SESSION
) -> APIResponse:
    """Get queued asset events for a DAG."""
    where_clause = _generate_queued_event_where_clause(dag_id=dag_id, before=before)
    query = (
        select(AssetDagRunQueue, AssetModel.uri)
        .join(AssetModel, AssetDagRunQueue.dataset_id == AssetModel.id)
        .where(*where_clause)
    )
    result = session.execute(query).all()
    total_entries = get_query_count(query, session=session)
    if not result:
        raise NotFound(
            "Queue event not found",
            detail=f"Queue event with dag_id: `{dag_id}` was not found",
        )
    queued_events = [
        QueuedEvent(created_at=adrq.created_at, dag_id=adrq.target_dag_id, uri=uri) for adrq, uri in result
    ]
    return queued_event_collection_schema.dump(
        QueuedEventCollection(queued_events=queued_events, total_entries=total_entries)
    )


@security.requires_access_dataset("DELETE")
@security.requires_access_dag("GET")
@action_logging
@provide_session
def delete_dag_dataset_queued_events(
    *, dag_id: str, before: str | None = None, session: Session = NEW_SESSION
) -> APIResponse:
    """Delete queued asset events for a DAG."""
    where_clause = _generate_queued_event_where_clause(dag_id=dag_id, before=before)
    delete_stmt = delete(AssetDagRunQueue).where(*where_clause)
    result = session.execute(delete_stmt)
    if result.rowcount > 0:
        return NoContent, HTTPStatus.NO_CONTENT

    raise NotFound(
        "Queue event not found",
        detail=f"Queue event with dag_id: `{dag_id}` was not found",
    )


@security.requires_access_dataset("GET")
@provide_session
def get_dataset_queued_events(
    *, uri: str, before: str | None = None, session: Session = NEW_SESSION
) -> APIResponse:
    """Get queued asset events for an asset."""
    permitted_dag_ids = get_auth_manager().get_permitted_dag_ids(methods=["GET"])
    where_clause = _generate_queued_event_where_clause(
        uri=uri, before=before, permitted_dag_ids=permitted_dag_ids
    )
    query = (
        select(AssetDagRunQueue, AssetModel.uri)
        .join(AssetModel, AssetDagRunQueue.dataset_id == AssetModel.id)
        .where(*where_clause)
    )
    total_entries = get_query_count(query, session=session)
    result = session.execute(query).all()
    if total_entries > 0:
        queued_events = [
            QueuedEvent(created_at=adrq.created_at, dag_id=adrq.target_dag_id, uri=uri)
            for adrq, uri in result
        ]
        return queued_event_collection_schema.dump(
            QueuedEventCollection(queued_events=queued_events, total_entries=total_entries)
        )
    raise NotFound(
        "Queue event not found",
        detail=f"Queue event with asset uri: `{uri}` was not found",
    )


@security.requires_access_dataset("DELETE")
@action_logging
@provide_session
def delete_dataset_queued_events(
    *, uri: str, before: str | None = None, session: Session = NEW_SESSION
) -> APIResponse:
    """Delete queued asset events for an asset."""
    permitted_dag_ids = get_auth_manager().get_permitted_dag_ids(methods=["GET"])
    where_clause = _generate_queued_event_where_clause(
        uri=uri, before=before, permitted_dag_ids=permitted_dag_ids
    )
    delete_stmt = delete(AssetDagRunQueue).where(*where_clause).execution_options(synchronize_session="fetch")

    result = session.execute(delete_stmt)
    if result.rowcount > 0:
        return NoContent, HTTPStatus.NO_CONTENT
    raise NotFound(
        "Queue event not found",
        detail=f"Queue event with asset uri: `{uri}` was not found",
    )


@security.requires_access_dataset("POST")
@provide_session
@action_logging
def create_dataset_event(session: Session = NEW_SESSION) -> APIResponse:
    """Create asset event."""
    body = get_json_request_dict()
    try:
        json_body = create_asset_event_schema.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err))

    uri = json_body["dataset_uri"]
    asset = session.scalar(select(AssetModel).where(AssetModel.uri == uri).limit(1))
    if not asset:
        raise NotFound(title="Asset not found", detail=f"Asset with uri: '{uri}' not found")
    timestamp = timezone.utcnow()
    extra = json_body.get("extra", {})
    extra["from_rest_api"] = True
    asset_event = asset_manager.register_asset_change(
        asset=Asset(uri),
        timestamp=timestamp,
        extra=extra,
        session=session,
    )
    if not asset_event:
        raise NotFound(title="Asset not found", detail=f"Asset with uri: '{uri}' not found")
    session.flush()  # So we can dump the timestamp.
    event = asset_event_schema.dump(asset_event)
    return event
