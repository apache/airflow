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

import json
import logging
from typing import Annotated

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from airflow.api_fastapi.common.db.common import get_session
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.execution_api import datamodels, deps
from airflow.models.xcom import BaseXCom

# TODO: Add dependency on JWT token
router = AirflowRouter(
    responses={status.HTTP_404_NOT_FOUND: {"description": "XCom not found"}},
)

log = logging.getLogger(__name__)


@router.get(
    "/{dag_id}/{run_id}/{task_id}/{key}",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
        status.HTTP_403_FORBIDDEN: {"description": "Task does not have access to the XCom"},
    },
)
def get_xcom(
    dag_id: str,
    run_id: str,
    task_id: str,
    key: str,
    token: deps.TokenDep,
    session: Annotated[Session, Depends(get_session)],
    map_index: Annotated[int, Query()] = -1,
) -> datamodels.XComResponse:
    """Get an Airflow XCom from database - not other XCom Backends."""
    if not has_xcom_access(key, token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "access_denied",
                "message": f"Task does not have access to XCom key '{key}'",
            },
        )

    # We use `BaseXCom.get_many` to fetch XComs directly from the database, bypassing the XCom Backend.
    # This avoids deserialization via the backend (e.g., from a remote storage like S3) and instead
    # retrieves the raw serialized value from the database. By not relying on `XCom.get_many` or `XCom.get_one`
    # (which automatically deserializes using the backend), we avoid potential
    # performance hits from retrieving large data files into the API server.
    query = BaseXCom.get_many(
        run_id=run_id,
        key=key,
        task_ids=task_id,
        dag_ids=dag_id,
        map_indexes=map_index,
        limit=1,
        session=session,
    )

    result = query.with_entities(BaseXCom.value).first()

    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"XCom with key '{key}' not found for task '{task_id}' in DAG '{dag_id}'",
            },
        )

    try:
        xcom_value = BaseXCom.deserialize_value(result)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "reason": "invalid_format",
                "message": "XCom value is not a valid JSON",
            },
        )

    return datamodels.XComResponse(key=key, value=xcom_value)


def has_xcom_access(xcom_key: str, token: datamodels.TIToken) -> bool:
    """Check if the task has access to the XCom."""
    # TODO: Placeholder for actual implementation

    ti_key = token.ti_key
    log.debug(
        "Checking access for task instance with key '%s' to XCom '%s'",
        ti_key,
        xcom_key,
    )
    return True
