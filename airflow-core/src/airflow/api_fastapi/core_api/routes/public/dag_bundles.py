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
from sqlalchemy import select, update

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dag_bundles import DagBundleRefreshResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag_bundle
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.models.dagbundle import DagBundleModel

dag_bundles_router = AirflowRouter(tags=["Dag Bundle"], prefix="/dagBundles")


@dag_bundles_router.post(
    "/{bundle_name}/refresh",
    status_code=status.HTTP_202_ACCEPTED,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag_bundle(method="PUT")), Depends(action_logging())],
)
def refresh_dag_bundle(bundle_name: str, session: SessionDep) -> DagBundleRefreshResponse:
    """Request that every Dag processor refresh a bundle."""
    session.execute(
        update(DagBundleModel)
        .where(
            DagBundleModel.name == bundle_name,
            DagBundleModel.active.is_(True),
        )
        .values(refresh_generation=DagBundleModel.refresh_generation + 1)
        .execution_options(synchronize_session=False)
    )
    refresh_generation = session.scalar(
        select(DagBundleModel.refresh_generation).where(
            DagBundleModel.name == bundle_name,
            DagBundleModel.active.is_(True),
        )
    )
    if refresh_generation is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Dag bundle not found")
    return DagBundleRefreshResponse(
        bundle_name=bundle_name,
        refresh_generation=refresh_generation,
    )
