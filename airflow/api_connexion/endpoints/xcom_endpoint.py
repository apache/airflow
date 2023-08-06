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

import copy

from flask import g
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.parameters import check_limit, format_parameters
from airflow.api_connexion.schemas.xcom_schema import XComCollection, xcom_collection_schema, xcom_schema
from airflow.api_connexion.types import APIResponse
from airflow.models import DagRun as DR, XCom
from airflow.security import permissions
from airflow.settings import conf
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.utils.db import get_query_count
from airflow.utils.session import NEW_SESSION, provide_session


@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
    ],
)
@format_parameters({"limit": check_limit})
@provide_session
def get_xcom_entries(
    *,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int | None = None,
    xcom_key: str | None = None,
    limit: int | None,
    offset: int | None = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get all XCom values."""
    query = select(XCom)
    if dag_id == "~":
        appbuilder = get_airflow_app().appbuilder
        readable_dag_ids = appbuilder.sm.get_readable_dag_ids(g.user)
        query = query.where(XCom.dag_id.in_(readable_dag_ids))
        query = query.join(DR, and_(XCom.dag_id == DR.dag_id, XCom.run_id == DR.run_id))
    else:
        query = query.where(XCom.dag_id == dag_id)
        query = query.join(DR, and_(XCom.dag_id == DR.dag_id, XCom.run_id == DR.run_id))

    if task_id != "~":
        query = query.where(XCom.task_id == task_id)
    if dag_run_id != "~":
        query = query.where(DR.run_id == dag_run_id)
    if map_index is not None:
        query = query.where(XCom.map_index == map_index)
    if xcom_key is not None:
        query = query.where(XCom.key == xcom_key)
    query = query.order_by(DR.execution_date, XCom.task_id, XCom.dag_id, XCom.key)
    total_entries = get_query_count(query, session=session)
    query = session.scalars(query.offset(offset).limit(limit))
    return xcom_collection_schema.dump(XComCollection(xcom_entries=query, total_entries=total_entries))


@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
    ],
)
@provide_session
def get_xcom_entry(
    *,
    dag_id: str,
    task_id: str,
    dag_run_id: str,
    xcom_key: str,
    map_index: int = -1,
    deserialize: bool = False,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get an XCom entry."""
    if deserialize:
        if not conf.getboolean("api", "enable_xcom_deserialize_support", fallback=False):
            raise BadRequest(detail="XCom deserialization is disabled in configuration.")
        query = select(XCom, XCom.value)
    else:
        query = select(XCom)

    query = query.where(
        XCom.dag_id == dag_id, XCom.task_id == task_id, XCom.key == xcom_key, XCom.map_index == map_index
    )
    query = query.join(DR, and_(XCom.dag_id == DR.dag_id, XCom.run_id == DR.run_id))
    query = query.where(DR.run_id == dag_run_id)

    if deserialize:
        item = session.execute(query).one_or_none()
    else:
        item = session.scalars(query).one_or_none()

    if item is None:
        raise NotFound("XCom entry not found")

    if deserialize:
        xcom, value = item
        stub = copy.copy(xcom)
        stub.value = value
        stub.value = XCom.deserialize_value(stub)
        item = stub

    return xcom_schema.dump(item)
