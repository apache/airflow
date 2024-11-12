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

from fastapi import Depends, HTTPException
from sqlalchemy import and_, select
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import get_session
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.xcom import (
    XComResponseNative,
    XComResponseString,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.models import DagRun as DR, XCom
from airflow.settings import conf

xcom_router = AirflowRouter(
    tags=["XCom"], prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries"
)


@xcom_router.get(
    "/{xcom_key}",
    responses=create_openapi_http_exception_doc([400, 401, 403, 404]),
)
def get_xcom_entry(
    dag_id: str,
    task_id: str,
    dag_run_id: str,
    xcom_key: str,
    session: Annotated[Session, Depends(get_session)],
    map_index: int = -1,
    deserialize: bool = False,
    stringify: bool = True,
) -> XComResponseNative | XComResponseString:
    """Get an XCom entry."""
    if deserialize:
        if not conf.getboolean("api", "enable_xcom_deserialize_support", fallback=False):
            raise HTTPException(400, "XCom deserialization is disabled in configuration.")
        query = select(XCom, XCom.value)
    else:
        query = select(XCom)
        print()

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
        raise HTTPException(404, f"XCom entry with key: `{xcom_key}` not found")

    if deserialize:
        xcom, value = item
        xcom_stub = copy.copy(xcom)
        xcom_stub.value = value
        xcom_stub.value = XCom.deserialize_value(xcom_stub)
        item = xcom_stub

    if stringify or conf.getboolean("core", "enable_xcom_pickling"):
        return XComResponseString.model_validate(item, from_attributes=True)

    return XComResponseNative.model_validate(item, from_attributes=True)
