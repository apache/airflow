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

from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload, subqueryload

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.dataset_schema import (
    DatasetCollection,
    DatasetEventCollection,
    dataset_collection_schema,
    dataset_event_collection_schema,
    dataset_schema,
)
from airflow.api_connexion.types import APIResponse
from airflow.models.dataset import Dataset, DatasetEvent
from airflow.security import permissions
from airflow.utils.session import NEW_SESSION, provide_session


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DATASET)])
@provide_session
def get_dataset(id: int, session: Session = NEW_SESSION) -> APIResponse:
    """Get a Dataset"""
    dataset = (
        session.query(Dataset)
        .options(joinedload(Dataset.downstream_dag_references), joinedload(Dataset.upstream_task_references))
        .get(id)
    )
    if not dataset:
        raise NotFound(
            "Dataset not found",
            detail=f"The Dataset with id: `{id}` was not found",
        )
    return dataset_schema.dump(dataset)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DATASET)])
@format_parameters({'limit': check_limit})
@provide_session
def get_datasets(
    *,
    limit: int,
    offset: int = 0,
    uri_pattern: Optional[str] = None,
    order_by: str = "id",
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get datasets"""
    allowed_attrs = ['id', 'uri', 'created_at', 'updated_at']

    total_entries = session.query(func.count(Dataset.id)).scalar()
    query = session.query(Dataset)
    if uri_pattern:
        query = query.filter(Dataset.uri.ilike(f"%{uri_pattern}%"))
    query = apply_sorting(query, order_by, {}, allowed_attrs)
    datasets = (
        query.options(
            subqueryload(Dataset.downstream_dag_references), subqueryload(Dataset.upstream_task_references)
        )
        .offset(offset)
        .limit(limit)
        .all()
    )
    return dataset_collection_schema.dump(DatasetCollection(datasets=datasets, total_entries=total_entries))


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DATASET)])
@provide_session
@format_parameters({'limit': check_limit})
def get_dataset_events(
    *,
    limit: int,
    offset: int = 0,
    order_by: str = "timestamp",
    dataset_id: Optional[int] = None,
    source_dag_id: Optional[str] = None,
    source_task_id: Optional[str] = None,
    source_run_id: Optional[str] = None,
    source_map_index: Optional[int] = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get dataset events"""
    allowed_attrs = ['source_dag_id', 'source_task_id', 'source_run_id', 'source_map_index', 'timestamp']

    query = session.query(DatasetEvent)

    if dataset_id:
        query = query.filter(DatasetEvent.dataset_id == dataset_id)
    if source_dag_id:
        query = query.filter(DatasetEvent.source_dag_id == source_dag_id)
    if source_task_id:
        query = query.filter(DatasetEvent.source_task_id == source_task_id)
    if source_run_id:
        query = query.filter(DatasetEvent.source_run_id == source_run_id)
    if source_map_index:
        query = query.filter(DatasetEvent.source_map_index == source_map_index)

    total_entries = query.count()
    query = apply_sorting(query, order_by, {}, allowed_attrs)
    events = query.offset(offset).limit(limit).all()
    return dataset_event_collection_schema.dump(
        DatasetEventCollection(dataset_events=events, total_entries=total_entries)
    )
