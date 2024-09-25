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

import logging
from typing import TYPE_CHECKING

import pendulum
from flask import g
from sqlalchemy import select

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import Conflict, NotFound, PermissionDenied
from airflow.api_connexion.schemas.backfill_schema import (
    BackfillCollection,
    backfill_collection_schema,
    backfill_schema,
)
from airflow.models.backfill import Backfill
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils import timezone
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.www.decorators import action_logging
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_connexion.types import APIResponse

log = logging.getLogger(__name__)

RESOURCE_EVENT_PREFIX = "dag"


@provide_session
def _create_backfill(
    *,
    dag_id: str,
    from_date: str,
    to_date: str,
    max_active_runs: int,
    reverse: bool,
    dag_run_conf: dict | None,
    session: Session = NEW_SESSION,
):
    serdag = session.get(SerializedDagModel, dag_id)
    if not serdag:
        raise NotFound(f"Could not find dag {dag_id}")

    from_date = pendulum.parse(from_date)
    to_date = pendulum.parse(to_date)
    br = Backfill(
        dag_id=dag_id,
        from_date=from_date,
        to_date=to_date,
        max_active_runs=max_active_runs,
        dag_run_conf=dag_run_conf,
    )
    session.add(br)
    session.commit()
    return br


def _has_access(dag_ids, methods):
    if get_auth_manager().filter_permitted_dag_ids(dag_ids=dag_ids, methods=methods, user=g.user):
        return True
    return False


@security.requires_access_dag("GET")
@action_logging
@provide_session
def list_backfills(dag_id, session):
    backfills = session.scalars(select(Backfill).where(Backfill.dag_id == dag_id)).all()
    obj = BackfillCollection(
        backfills=backfills,
        total_entries=len(backfills),
    )
    return backfill_collection_schema.dump(obj)


@security.requires_access_dag("GET")
@action_logging
@provide_session
def pause_backfill(backfill_id, session):
    br = session.get(Backfill, backfill_id)
    if not _has_access(dag_ids=[br.dag_id], methods="PUT"):
        raise PermissionDenied()
    if br.completed_at:
        raise Conflict("Backfill is already completed.")
    if br.is_paused is False:
        br.is_paused = True
    session.commit()
    return backfill_schema.dump(br)


@security.requires_access_dag("GET")
@action_logging
@provide_session
def unpause_backfill(backfill_id, session):
    br = session.get(Backfill, backfill_id)
    if not _has_access(dag_ids=[br.dag_id], methods="PUT"):
        raise PermissionDenied()
    if br.completed_at:
        raise Conflict("Backfill is already completed.")
    if br.is_paused:
        br.is_paused = False
    session.commit()
    return backfill_schema.dump(br)


@security.requires_access_dag("GET")
@action_logging
@provide_session
def cancel_backfill(backfill_id, session):
    br: Backfill = session.get(Backfill, backfill_id)
    if not _has_access(dag_ids=[br.dag_id], methods="PUT"):
        raise PermissionDenied()

    if br.completed_at is not None:
        raise Conflict("Backfill is already completed.")

    br.completed_at = timezone.utcnow()

    # first, pause
    if not br.is_paused:
        br.is_paused = True
    session.commit()
    return backfill_schema.dump(br)


@security.requires_access_dag("GET")
@action_logging
@provide_session
def get_backfill(backfill_id: int, session: Session = NEW_SESSION):
    backfill = session.get(Backfill, backfill_id)
    if backfill:
        if not _has_access([backfill.dag_id], methods=["GET"]):
            raise PermissionDenied()
        return backfill_schema.dump(backfill)
    raise NotFound("Backfill not found")


@security.requires_access_dag("PUT")
@action_logging
def create_backfill(
    dag_id: str,
    from_date: str,
    to_date: str,
    max_active_runs: int = 10,
    reverse: bool = False,
    dag_run_conf: dict | None = None,
) -> APIResponse:
    backfill_obj = _create_backfill(
        dag_id=dag_id,
        from_date=from_date,
        to_date=to_date,
        max_active_runs=max_active_runs,
        reverse=reverse,
        dag_run_conf=dag_run_conf,
    )
    if not backfill_obj:
        raise Conflict(f"Already an active backfill for dag {dag_id}")
    return backfill_schema.dump(backfill_obj)
