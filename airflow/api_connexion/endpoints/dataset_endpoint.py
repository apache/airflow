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
from sqlalchemy import delete, func, select
from sqlalchemy.orm import joinedload, subqueryload

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_datetime, format_parameters
from airflow.api_connexion.schemas.dataset_schema import (
    DatasetCollection,
    DatasetDagRunQueueCollection,
    DatasetEventCollection,
    dataset_collection_schema,
    dataset_dag_run_queue_collection_schema,
    dataset_dag_run_queue_schema,
    dataset_event_collection_schema,
    dataset_schema,
)
from airflow.models.dataset import DatasetDagRunQueue, DatasetEvent, DatasetModel
from airflow.utils.db import get_query_count
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_connexion.types import APIResponse


@security.requires_access_dataset("GET")
@provide_session
def get_dataset(*, uri: str, session: Session = NEW_SESSION) -> APIResponse:
    """Get a Dataset."""
    dataset = session.scalar(
        select(DatasetModel)
        .where(DatasetModel.uri == uri)
        .options(joinedload(DatasetModel.consuming_dags), joinedload(DatasetModel.producing_tasks))
    )
    if not dataset:
        raise NotFound(
            "Dataset not found",
            detail=f"The Dataset with uri: `{uri}` was not found",
        )
    return dataset_schema.dump(dataset)


@security.requires_access_dataset("GET")
@format_parameters({"limit": check_limit})
@provide_session
def get_datasets(
    *,
    limit: int,
    offset: int = 0,
    uri_pattern: str | None = None,
    order_by: str = "id",
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get datasets."""
    allowed_attrs = ["id", "uri", "created_at", "updated_at"]

    total_entries = session.scalars(select(func.count(DatasetModel.id))).one()
    query = select(DatasetModel)
    if uri_pattern:
        query = query.where(DatasetModel.uri.ilike(f"%{uri_pattern}%"))
    query = apply_sorting(query, order_by, {}, allowed_attrs)
    datasets = session.scalars(
        query.options(subqueryload(DatasetModel.consuming_dags), subqueryload(DatasetModel.producing_tasks))
        .offset(offset)
        .limit(limit)
    ).all()
    return dataset_collection_schema.dump(DatasetCollection(datasets=datasets, total_entries=total_entries))


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
    """Get dataset events."""
    allowed_attrs = ["source_dag_id", "source_task_id", "source_run_id", "source_map_index", "timestamp"]

    query = select(DatasetEvent)

    if dataset_id:
        query = query.where(DatasetEvent.dataset_id == dataset_id)
    if source_dag_id:
        query = query.where(DatasetEvent.source_dag_id == source_dag_id)
    if source_task_id:
        query = query.where(DatasetEvent.source_task_id == source_task_id)
    if source_run_id:
        query = query.where(DatasetEvent.source_run_id == source_run_id)
    if source_map_index:
        query = query.where(DatasetEvent.source_map_index == source_map_index)

    query = query.options(subqueryload(DatasetEvent.created_dagruns))

    total_entries = get_query_count(query, session=session)
    query = apply_sorting(query, order_by, {}, allowed_attrs)
    events = session.scalars(query.offset(offset).limit(limit)).all()
    return dataset_event_collection_schema.dump(
        DatasetEventCollection(dataset_events=events, total_entries=total_entries)
    )


def _get_ddrq(dag_id: str, uri: str, session: Session, before: str | None = None) -> DatasetDagRunQueue:
    """Get DatasetDagRunQueue."""
    dataset = session.scalar(select(DatasetModel).where(DatasetModel.uri == uri))
    if not dataset:
        return None
    dataset_id = dataset.id

    where_clause_conditions = [
        DatasetDagRunQueue.target_dag_id == dag_id,
        DatasetDagRunQueue.dataset_id == dataset_id,
    ]
    if before is not None:
        where_clause_conditions.append(DatasetDagRunQueue.created_at < format_datetime(before))

    return session.scalar(select(DatasetDagRunQueue).where(*where_clause_conditions))


@security.requires_access_dataset("GET")
@provide_session
def get_dag_dataset_queue_event(
    *, dag_id: str, uri: str, before: str | None = None, session: Session = NEW_SESSION
) -> APIResponse:
    """Get a queued Dataset event for a DAG."""
    ddrq = _get_ddrq(dag_id=dag_id, uri=uri, session=session, before=before)
    if ddrq is None:
        raise NotFound(
            "Queue event not found",
            detail=f"Queue event with dag_id: `{dag_id}` and dataset uri: `{uri}` was not found",
        )
    return dataset_dag_run_queue_schema.dump(ddrq)


@security.requires_access_dataset("DELETE")
@provide_session
def delete_dag_dataset_queue_event(
    *, dag_id: str, uri: str, before: str | None = None, session: Session = NEW_SESSION
) -> APIResponse:
    """Delete a queued Dataset event for a DAG."""
    ddrq = _get_ddrq(dag_id=dag_id, uri=uri, session=session, before=before)
    if ddrq is None:
        raise NotFound(
            "Queue event not found",
            detail=f"Queue event with dag_id: `{dag_id}` and dataset uri: `{uri}` was not found",
        )
    session.delete(ddrq)
    return NoContent, HTTPStatus.NO_CONTENT


def _build_get_ddrqs_where_clause(dag_id: str, before: str | None = None):
    where_clauses = [DatasetDagRunQueue.target_dag_id == dag_id]
    if before is not None:
        where_clauses.append(DatasetDagRunQueue.created_at < format_datetime(before))
    return where_clauses


@security.requires_access_dataset("GET")
@provide_session
def get_dag_dataset_queue_events(
    *, dag_id: str, before: str | None = None, session: Session = NEW_SESSION
) -> APIResponse:
    """Get queued Dataset events for a DAG."""
    where_clauses = _build_get_ddrqs_where_clause(dag_id=dag_id, before=before)
    query = select(DatasetDagRunQueue).where(*where_clauses)
    total_entries = get_query_count(query, session=session)
    ddrqs = session.scalars(query).all()
    if not ddrqs:
        raise NotFound(
            "Queue event not found",
            detail=f"Queue event with dag_id: `{dag_id}` was not found",
        )
    return dataset_dag_run_queue_collection_schema.dump(
        DatasetDagRunQueueCollection(dataset_dag_run_queues=ddrqs, total_entries=total_entries)
    )


@security.requires_access_dataset("DELETE")
@provide_session
def delete_dag_dataset_queue_events(
    *, dag_id: str, before: str | None = None, session: Session = NEW_SESSION
) -> APIResponse:
    """Delete queued Dataset events for a DAG."""
    where_clauses = _build_get_ddrqs_where_clause(dag_id=dag_id, before=before)
    if where_clauses:
        delete_stmt = delete(DatasetDagRunQueue).where(*where_clauses)
        s = session.execute(delete_stmt)
        if s.rowcount:
            return NoContent, HTTPStatus.NO_CONTENT
    raise NotFound(
        "Queue event not found",
        detail=f"Queue event with dag_id: `{dag_id}` was not found",
    )


def _build_get_dataset_ddrqs_where_clause(uri: str, session: Session, before: str | None = None):
    dataset = session.scalar(select(DatasetModel).where(DatasetModel.uri == uri))
    if not dataset:
        return None
    dataset_id = dataset.id

    where_clauses = [DatasetDagRunQueue.dataset_id == dataset_id]
    if before is not None:
        where_clauses.append(DatasetDagRunQueue.created_at < format_datetime(before))
    return where_clauses


@security.requires_access_dataset("GET")
@provide_session
def get_dataset_queue_events(
    *, uri: str, before: str | None = None, session: Session = NEW_SESSION
) -> APIResponse:
    """Get queued Dataset events for a Dataset"""
    where_clauses = _build_get_dataset_ddrqs_where_clause(uri=uri, session=session, before=before)
    ddrqs = None
    if where_clauses:
        query = select(DatasetDagRunQueue).where(*where_clauses)
        total_entries = get_query_count(query, session=session)
        ddrqs = session.scalars(query).all()
    if not where_clauses or not ddrqs:
        raise NotFound(
            "Queue event not found",
            detail=f"Queue event with dataset uri: `{uri}` was not found",
        )
    return dataset_dag_run_queue_collection_schema.dump(
        DatasetDagRunQueueCollection(dataset_dag_run_queues=ddrqs, total_entries=total_entries)
    )


@security.requires_access_dataset("DELETE")
@provide_session
def delete_dataset_queue_events(
    *, uri: str, before: str | None = None, session: Session = NEW_SESSION
) -> APIResponse:
    """Delete queued Dataset events for a Dataset"""
    where_clauses = _build_get_dataset_ddrqs_where_clause(uri=uri, session=session, before=before)
    if where_clauses:
        delete_stmt = delete(DatasetDagRunQueue).where(*where_clauses)
        s = session.execute(delete_stmt)
        if s.rowcount:
            return NoContent, HTTPStatus.NO_CONTENT
    raise NotFound(
        "Queue event not found",
        detail=f"Queue event with dataset uri: `{uri}` was not found",
    )
