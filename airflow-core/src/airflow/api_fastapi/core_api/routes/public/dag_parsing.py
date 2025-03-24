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

from collections.abc import Sequence
from typing import TYPE_CHECKING

from fastapi import Depends, HTTPException, Request, status
from itsdangerous import BadSignature, URLSafeSerializer
from sqlalchemy import select

from airflow.api_fastapi.auth.managers.models.resource_details import DagDetails
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dags import (
    DagReserializePostBody,
    ReserializeResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagPriorityParsingRequest

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.models.batch_apis import IsAuthorizedDagRequest

dag_parsing_router = AirflowRouter(tags=["DAG Parsing"], prefix="/parseDagFile")


@dag_parsing_router.put(
    "/{file_token}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(requires_access_dag(method="PUT")), Depends(action_logging())],
)
def reparse_dag_file(
    file_token: str,
    session: SessionDep,
    request: Request,
) -> None:
    """Request re-parsing a DAG file."""
    secret_key = request.app.state.secret_key
    auth_s = URLSafeSerializer(secret_key)
    try:
        path = auth_s.loads(file_token)
    except BadSignature:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "File not found")

    requests: Sequence[IsAuthorizedDagRequest] = [
        {"method": "PUT", "details": DagDetails(id=dag_id)}
        for dag_id in session.scalars(select(DagModel.dag_id).where(DagModel.fileloc == path))
    ]
    if not requests:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "File not found")

    parsing_request = DagPriorityParsingRequest(fileloc=path)
    session.add(parsing_request)


@dag_parsing_router.post(
    "/manage/reserialize",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
        ]
    ),
    dependencies=[Depends(action_logging())],
)
def reserialize_dags(
    request: DagReserializePostBody,
    session: SessionDep,  # Add your session dependency
):
    """
    Reserialize DAG bundles in Airflow.

    - **bundle_names**: List of specific bundles to reserialize (all if empty)
    """
    try:
        manager = DagBundlesManager()

        # Getting all bundle names which was retrieved in validation function
        manager.sync_bundles_to_db()
        all_bundle_names = set(manager.get_all_bundle_names())

        # Validate bundle names if specified
        if request.bundle_names:
            bundles_to_process = set(request.bundle_names)
            if len(bundles_to_process - all_bundle_names) > 0:
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST,
                    f"Invalid bundle name: {bundles_to_process - all_bundle_names}",
                )
        else:
            bundles_to_process = all_bundle_names

        file_locations = session.scalars(
            select(DagModel.fileloc).where(DagModel.bundle_name.in_(list(bundles_to_process)))
        )
        # Process each bundle
        parsing_requests = [DagPriorityParsingRequest(fileloc=fileloc) for fileloc in file_locations]

        session.add_all(parsing_requests)
        return ReserializeResponse(
            message="DAG bundles reserialized successfully", processed_bundles=list(bundles_to_process)
        )
    except HTTPException as e:
        raise e

    except Exception as e:
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to reserialize DAG bundles: {str(e)}",
        )
