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

from connexion import NoContent
from flask import g, request
from marshmallow import ValidationError
from sqlalchemy import select, update
from sqlalchemy.sql.expression import or_

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import AlreadyExists, BadRequest, NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.dag_schema import (
    DAGCollection,
    DAGCollectionSchema,
    DAGDetailSchema,
    DAGSchema,
    dag_schema,
    dags_collection_schema,
)
from airflow.exceptions import AirflowException, DagNotFound
from airflow.models.dag import DagModel, DagTag
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.utils.api_migration import mark_fastapi_migration_done
from airflow.utils.db import get_query_count
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.www.decorators import action_logging
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow import DAG
    from airflow.api_connexion.types import APIResponse, UpdateMask


@security.requires_access_dag("GET")
@provide_session
def get_dag(
    *, dag_id: str, fields: Collection[str] | None = None, session: Session = NEW_SESSION
) -> APIResponse:
    """Get basic information about a DAG."""
    dag = session.scalar(select(DagModel).where(DagModel.dag_id == dag_id))
    if dag is None:
        raise NotFound("DAG not found", detail=f"The DAG with dag_id: {dag_id} was not found")
    try:
        dag_schema = DAGSchema(only=fields) if fields else DAGSchema()
    except ValueError as e:
        raise BadRequest("DAGSchema init error", detail=str(e))
    return dag_schema.dump(
        dag,
    )


@security.requires_access_dag("GET")
@provide_session
def get_dag_details(
    *, dag_id: str, fields: Collection[str] | None = None, session: Session = NEW_SESSION
) -> APIResponse:
    """Get details of DAG."""
    dag: DAG = get_airflow_app().dag_bag.get_dag(dag_id)
    if not dag:
        raise NotFound("DAG not found", detail=f"The DAG with dag_id: {dag_id} was not found")
    dag_model: DagModel = session.get(DagModel, dag_id)
    for key, value in dag.__dict__.items():
        if not key.startswith("_") and not hasattr(dag_model, key):
            setattr(dag_model, key, value)
    try:
        dag_detail_schema = DAGDetailSchema(only=fields) if fields else DAGDetailSchema()
    except ValueError as e:
        raise BadRequest("DAGDetailSchema init error", detail=str(e))
    return dag_detail_schema.dump(dag_model)


@mark_fastapi_migration_done
@security.requires_access_dag("GET")
@format_parameters({"limit": check_limit})
@provide_session
def get_dags(
    *,
    limit: int,
    offset: int = 0,
    tags: Collection[str] | None = None,
    dag_id_pattern: str | None = None,
    only_active: bool = True,
    paused: bool | None = None,
    order_by: str = "dag_id",
    fields: Collection[str] | None = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get all DAGs."""
    allowed_attrs = ["dag_id"]
    dags_query = select(DagModel)
    if only_active:
        dags_query = dags_query.where(DagModel.is_active)
    if paused is not None:
        if paused:
            dags_query = dags_query.where(DagModel.is_paused)
        else:
            dags_query = dags_query.where(~DagModel.is_paused)
    if dag_id_pattern:
        dags_query = dags_query.where(DagModel.dag_id.ilike(f"%{dag_id_pattern}%"))

    readable_dags = get_auth_manager().get_permitted_dag_ids(user=g.user)

    dags_query = dags_query.where(DagModel.dag_id.in_(readable_dags))
    if tags:
        cond = [DagModel.tags.any(DagTag.name == tag) for tag in tags]
        dags_query = dags_query.where(or_(*cond))

    total_entries = get_query_count(dags_query, session=session)
    dags_query = apply_sorting(dags_query, order_by, {}, allowed_attrs)
    dags = session.scalars(dags_query.offset(offset).limit(limit)).all()

    try:
        dags_collection_schema = (
            DAGCollectionSchema(only=[f"dags.{field}" for field in fields])
            if fields
            else DAGCollectionSchema()
        )
        return dags_collection_schema.dump(DAGCollection(dags=dags, total_entries=total_entries))
    except ValueError as e:
        raise BadRequest("DAGCollectionSchema error", detail=str(e))


@security.requires_access_dag("PUT")
@action_logging
@provide_session
def patch_dag(*, dag_id: str, update_mask: UpdateMask = None, session: Session = NEW_SESSION) -> APIResponse:
    """Update the specific DAG."""
    try:
        patch_body = dag_schema.load(request.json, session=session)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    if update_mask:
        patch_body_ = {}
        if update_mask != ["is_paused"]:
            raise BadRequest(detail="Only `is_paused` field can be updated through the REST API")
        patch_body_[update_mask[0]] = patch_body[update_mask[0]]
        patch_body = patch_body_
    dag = session.scalar(select(DagModel).where(DagModel.dag_id == dag_id))
    if not dag:
        raise NotFound(f"Dag with id: '{dag_id}' not found")
    dag.is_paused = patch_body["is_paused"]
    session.flush()
    return dag_schema.dump(dag)


@security.requires_access_dag("PUT")
@format_parameters({"limit": check_limit})
@action_logging
@provide_session
def patch_dags(limit, session, offset=0, only_active=True, tags=None, dag_id_pattern=None, update_mask=None):
    """Patch multiple DAGs."""
    try:
        patch_body = dag_schema.load(request.json, session=session)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    if update_mask:
        patch_body_ = {}
        if update_mask != ["is_paused"]:
            raise BadRequest(detail="Only `is_paused` field can be updated through the REST API")
        update_mask = update_mask[0]
        patch_body_[update_mask] = patch_body[update_mask]
        patch_body = patch_body_
    dags_query = select(DagModel)
    if only_active:
        dags_query = dags_query.where(DagModel.is_active)

    if dag_id_pattern == "~":
        dag_id_pattern = "%"
    dags_query = dags_query.where(DagModel.dag_id.ilike(f"%{dag_id_pattern}%"))
    editable_dags = get_auth_manager().get_permitted_dag_ids(methods=["PUT"], user=g.user)

    dags_query = dags_query.where(DagModel.dag_id.in_(editable_dags))
    if tags:
        cond = [DagModel.tags.any(DagTag.name == tag) for tag in tags]
        dags_query = dags_query.where(or_(*cond))

    total_entries = get_query_count(dags_query, session=session)

    dags = session.scalars(dags_query.order_by(DagModel.dag_id).offset(offset).limit(limit)).all()

    dags_to_update = {dag.dag_id for dag in dags}
    session.execute(
        update(DagModel)
        .where(DagModel.dag_id.in_(dags_to_update))
        .values(is_paused=patch_body["is_paused"])
        .execution_options(synchronize_session="fetch")
    )

    session.flush()

    return dags_collection_schema.dump(DAGCollection(dags=dags, total_entries=total_entries))


@security.requires_access_dag("DELETE")
@action_logging
@provide_session
def delete_dag(dag_id: str, session: Session = NEW_SESSION) -> APIResponse:
    """Delete the specific DAG."""
    from airflow.api.common import delete_dag as delete_dag_module

    try:
        delete_dag_module.delete_dag(dag_id, session=session)
    except DagNotFound:
        raise NotFound(f"Dag with id: '{dag_id}' not found")
    except AirflowException:
        raise AlreadyExists(detail=f"Task instances of dag with id: '{dag_id}' are still running")

    return NoContent, HTTPStatus.NO_CONTENT
