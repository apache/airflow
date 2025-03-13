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
from typing import Annotated

from fastapi import Body, Depends, HTTPException, Query, Response, status
from pydantic import JsonValue
from sqlalchemy.sql.selectable import Select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.execution_api import deps
from airflow.api_fastapi.execution_api.datamodels.token import TIToken
from airflow.api_fastapi.execution_api.datamodels.xcom import XComResponse
from airflow.models.taskmap import TaskMap
from airflow.models.xcom import BaseXCom
from airflow.utils.db import get_query_count

# TODO: Add dependency on JWT token
router = AirflowRouter(
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
        status.HTTP_403_FORBIDDEN: {"description": "Task does not have access to the XCom"},
    },
)

log = logging.getLogger(__name__)


async def xcom_query(
    dag_id: str,
    run_id: str,
    task_id: str,
    key: str,
    session: SessionDep,
    token: deps.TokenDep,
    map_index: Annotated[int | None, Query()] = None,
) -> Select:
    if not has_xcom_access(dag_id, run_id, task_id, key, token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "access_denied",
                "message": f"Task does not have access to XCom key '{key}'",
            },
        )

    query = BaseXCom.get_many(
        run_id=run_id,
        key=key,
        task_ids=task_id,
        dag_ids=dag_id,
        map_indexes=map_index,
        session=session,
    )
    return query


@router.head(
    "/{dag_id}/{run_id}/{task_id}/{key}",
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "XCom not found"},
        status.HTTP_200_OK: {
            "description": "Metadata about the number of matching XCom values",
            "headers": {
                "Content-Range": {
                    "pattern": r"^map_indexes \d+$",
                    "description": "The number of (mapped) XCom values found for this task.",
                },
            },
        },
    },
    description="Return the count of the number of XCom values found via the Content-Range response header",
)
def head_xcom(
    response: Response,
    token: deps.TokenDep,
    session: SessionDep,
    xcom_query: Annotated[Select, Depends(xcom_query)],
    map_index: Annotated[int | None, Query()] = None,
) -> None:
    """Get the count of XComs from database - not other XCom Backends."""
    if map_index is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"reason": "invalid_request", "message": "Cannot specify map_index in a HEAD request"},
        )

    count = get_query_count(xcom_query, session=session)
    # Tell the caller how many items in this query. We define a custom range unit (HTTP spec only defines
    # "bytes" but we can add our own)
    response.headers["Content-Range"] = f"map_indexes {count}"


@router.get(
    "/{dag_id}/{run_id}/{task_id}/{key}",
    responses={status.HTTP_404_NOT_FOUND: {"description": "XCom not found"}},
    description="Get a single XCom Value",
)
def get_xcom(
    session: SessionDep,
    dag_id: str,
    run_id: str,
    task_id: str,
    key: str,
    xcom_query: Annotated[Select, Depends(xcom_query)],
    map_index: Annotated[int, Query()] = -1,
) -> XComResponse:
    """Get an Airflow XCom from database - not other XCom Backends."""
    # The xcom_query allows no map_index to be passed. This endpoint should always return just a single item,
    # so we override that query value
    xcom_query = xcom_query.filter(BaseXCom.map_index == map_index)
    # We use `BaseXCom.get_many` to fetch XComs directly from the database, bypassing the XCom Backend.
    # This avoids deserialization via the backend (e.g., from a remote storage like S3) and instead
    # retrieves the raw serialized value from the database. By not relying on `XCom.get_many` or `XCom.get_one`
    # (which automatically deserializes using the backend), we avoid potential
    # performance hits from retrieving large data files into the API server.
    result = xcom_query.limit(1).first()
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"XCom with {key=} {map_index=} not found for task {task_id!r} in DAG run {run_id!r} of {dag_id!r}",
            },
        )

    return XComResponse(key=key, value=result.value)


# TODO: once we have JWT tokens, then remove dag_id/run_id/task_id from the URL and just use the info in
# the token
@router.post(
    "/{dag_id}/{run_id}/{task_id}/{key}",
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid request body"},
    },
)
def set_xcom(
    dag_id: str,
    run_id: str,
    task_id: str,
    key: str,
    value: Annotated[
        JsonValue,
        Body(
            description="A JSON-formatted string representing the value to set for the XCom.",
            openapi_examples={
                "simple_value": {
                    "summary": "Simple value",
                    "value": '"value1"',
                },
                "dict_value": {
                    "summary": "Dictionary value",
                    "value": '{"key2": "value2"}',
                },
                "list_value": {
                    "summary": "List value",
                    "value": '["value1"]',
                },
            },
        ),
    ],
    token: deps.TokenDep,
    session: SessionDep,
    map_index: Annotated[int, Query()] = -1,
    mapped_length: Annotated[
        int | None, Query(description="Number of mapped tasks this value expands into")
    ] = None,
):
    """Set an Airflow XCom."""
    from airflow.configuration import conf

    if not has_xcom_access(dag_id, run_id, task_id, key, token, write=True):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "access_denied",
                "message": f"Task does not have access to set XCom key '{key}'",
            },
        )

    if mapped_length is not None:
        task_map = TaskMap(
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            map_index=map_index,
            length=mapped_length,
            keys=None,
        )
        max_map_length = conf.getint("core", "max_map_length", fallback=1024)
        if task_map.length > max_map_length:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "reason": "unmappable_return_value_length",
                    "message": "pushed value is too large to map as a downstream's dependency",
                },
            )
        session.add(task_map)

    # else:
    # TODO: Can/should we check if a client _hasn't_ provided this for an upstream of a mapped task? That
    # means loading the serialized dag and that seems like a relatively costly operation for minimal benefit
    # (the mapped task would fail in a moment as it can't be expanded anyway.)

    # We use `BaseXCom.set` to set XComs directly to the database, bypassing the XCom Backend.
    try:
        BaseXCom.set(
            key=key,
            value=value,
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            session=session,
            map_index=map_index,
        )
    except TypeError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "reason": "invalid_format",
                "message": f"XCom value is not a valid JSON: {e}",
            },
        )

    return {"message": "XCom successfully set"}


def has_xcom_access(
    dag_id: str, run_id: str, task_id: str, xcom_key: str, token: TIToken, write: bool = False
) -> bool:
    """Check if the task has access to the XCom."""
    # TODO: Placeholder for actual implementation

    ti_key = token.ti_key
    log.debug(
        "Checking %s XCom access for xcom from TaskInstance with key '%s' to XCom '%s'",
        "write" if write else "read",
        ti_key,
        xcom_key,
    )
    return True
