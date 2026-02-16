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

from fastapi import Depends, HTTPException, status

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.models.dagbundle import DagBundleModel, DagBundleRefreshRequest

dag_bundles_router = AirflowRouter(tags=["DAG Bundle"], prefix="/dagBundles")


@dag_bundles_router.post(
    "/{bundle_name}/refresh",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]
    ),
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(action_logging())],
)
def refresh_dag_bundle(
    bundle_name: str,
    session: SessionDep,
) -> None:
    """Request a DAG bundle to be refreshed."""
    bundle_model = session.get(DagBundleModel, bundle_name)
    if bundle_model is None or not bundle_model.active:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Bundle {bundle_name!r} not found or inactive",
        )

    existing = session.get(DagBundleRefreshRequest, bundle_name)
    if existing is not None:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"A refresh request for bundle {bundle_name!r} is already pending",
        )

    refresh_request = DagBundleRefreshRequest(
        bundle_name=bundle_name,
        created_at=timezone.utcnow(),
    )
    session.add(refresh_request)
