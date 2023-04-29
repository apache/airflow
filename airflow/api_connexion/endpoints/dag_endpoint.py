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
from typing import Collection

from connexion import NoContent
from flask import g, request
from marshmallow import ValidationError
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import or_

from airflow import DAG
from airflow.api_connexion import security
from airflow.api_connexion.exceptions import AlreadyExists, BadRequest, NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.dag_schema import (
    DAGCollection,
    dag_detail_schema,
    dag_schema,
    dags_collection_schema,
)
from airflow.api_connexion.types import APIResponse, UpdateMask
from airflow.exceptions import AirflowException, DagNotFound
from airflow.models.dag import DagModel, DagTag
from airflow.security import permissions
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.utils.session import NEW_SESSION, provide_session


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
@provide_session
def get_dag(*, dag_id: str, session: Session = NEW_SESSION) -> APIResponse:
    """Get basic information about a DAG."""
    dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).one_or_none()

    if dag is None:
        raise NotFound("DAG not found", detail=f"The DAG with dag_id: {dag_id} was not found")

    return dag_schema.dump(dag)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
def get_dag_details(*, dag_id: str) -> APIResponse:
    """Get details of DAG."""
    dag: DAG = get_airflow_app().dag_bag.get_dag(dag_id)
    if not dag:
        raise NotFound("DAG not found", detail=f"The DAG with dag_id: {dag_id} was not found")
    return dag_detail_schema.dump(dag)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)])
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
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get all DAGs."""
    allowed_attrs = ["dag_id"]
    dags_query = session.query(DagModel).filter(~DagModel.is_subdag)
    if only_active:
        dags_query = dags_query.filter(DagModel.is_active)
    if paused is not None:
        if paused:
            dags_query = dags_query.filter(DagModel.is_paused)
        else:
            dags_query = dags_query.filter(~DagModel.is_paused)
    if dag_id_pattern:
        dags_query = dags_query.filter(DagModel.dag_id.ilike(f"%{dag_id_pattern}%"))

    readable_dags = get_airflow_app().appbuilder.sm.get_accessible_dag_ids(g.user)

    dags_query = dags_query.filter(DagModel.dag_id.in_(readable_dags))
    if tags:
        cond = [DagModel.tags.any(DagTag.name == tag) for tag in tags]
        dags_query = dags_query.filter(or_(*cond))

    total_entries = dags_query.count()
    dags_query = apply_sorting(dags_query, order_by, {}, allowed_attrs)
    dags = dags_query.offset(offset).limit(limit).all()

    return dags_collection_schema.dump(DAGCollection(dags=dags, total_entries=total_entries))


@security.requires_access([(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG)])
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
    dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).one_or_none()
    if not dag:
        raise NotFound(f"Dag with id: '{dag_id}' not found")
    dag.is_paused = patch_body["is_paused"]
    session.flush()
    return dag_schema.dump(dag)


@security.requires_access([(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG)])
@format_parameters({"limit": check_limit})
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
    if only_active:
        dags_query = session.query(DagModel).filter(~DagModel.is_subdag, DagModel.is_active)
    else:
        dags_query = session.query(DagModel).filter(~DagModel.is_subdag)

    if dag_id_pattern == "~":
        dag_id_pattern = "%"
    dags_query = dags_query.filter(DagModel.dag_id.ilike(f"%{dag_id_pattern}%"))
    editable_dags = get_airflow_app().appbuilder.sm.get_editable_dag_ids(g.user)

    dags_query = dags_query.filter(DagModel.dag_id.in_(editable_dags))
    if tags:
        cond = [DagModel.tags.any(DagTag.name == tag) for tag in tags]
        dags_query = dags_query.filter(or_(*cond))

    total_entries = dags_query.count()

    dags = dags_query.order_by(DagModel.dag_id).offset(offset).limit(limit).all()

    dags_to_update = {dag.dag_id for dag in dags}
    session.query(DagModel).filter(DagModel.dag_id.in_(dags_to_update)).update(
        {DagModel.is_paused: patch_body["is_paused"]}, synchronize_session="fetch"
    )

    session.flush()

    return dags_collection_schema.dump(DAGCollection(dags=dags, total_entries=total_entries))


@security.requires_access([(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG)])
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
