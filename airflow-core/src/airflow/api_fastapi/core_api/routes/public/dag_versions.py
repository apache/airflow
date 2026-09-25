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

from typing import Annotated

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity, DagDetails
from airflow.api_fastapi.common.dagbag import DagBagDep, get_latest_version_of_dag
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    FilterParam,
    QueryLimit,
    QueryOffset,
    SortParam,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dag_versions import (
    DAGVersionCollectionResponse,
    DagVersionDiffResponse,
    DagVersionResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    GetUserDep,
    ReadableDagVersionsFilterDep,
    requires_access_dag,
)
from airflow.exceptions import DagVersionNotFound
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.serialization.dag_version_diff import DEFAULT_MAX_CHANGES, MAX_ALLOWED_CHANGES

dag_versions_router = AirflowRouter(tags=["DagVersion"], prefix="/dags/{dag_id}/dagVersions")


@dag_versions_router.get(
    # Registered before `/{version_number}`: that route would otherwise match `diff` as a
    # version number and reject the request before it ever reaches this handler.
    "/diff",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.VERSION))],
    tags=["experimental"],
    summary="Experimental: Compare what two stored versions of a Dag currently hold.",
    # An explicit description wins over the docstring, so the disclosure rules have to be
    # restated here or they vanish from the published spec.
    description=(
        "🚧 This is an experimental endpoint and may change or be removed without notice.\n\n"
        "Reports observed state — what the two stored versions currently hold — not why a version "
        "was created. Version access returns the changed structure with identifying path components "
        "masked; the raw values behind those changes, and the paths that name them, are disclosed "
        "only to a caller who may also read the Dag's code."
    ),
    # Serializing unset fields as null would erase the absent-vs-null distinction the value
    # fields document.
    response_model_exclude_unset=True,
)
def get_dag_version_diff(
    dag_id: str,
    session: SessionDep,
    user: GetUserDep,
    base_version_number: Annotated[int, Query(description="Version to compare from.", gt=0)],
    target_version_number: Annotated[int, Query(description="Version to compare to.", gt=0)],
    max_changes: Annotated[
        int,
        Query(
            description=(
                "Largest number of records `changes` may hold. A repeat of a path already recorded "
                "does not count towards it, and `truncated` says whether the bound dropped anything."
            ),
            gt=0,
            le=MAX_ALLOWED_CHANGES,
        ),
    ] = DEFAULT_MAX_CHANGES,
) -> DagVersionDiffResponse:
    """
    Compare what two stored versions of a Dag currently hold.

    This reports observed state, not why a version was created. Version access returns the changed
    structure with identifying path components masked; the raw values behind those changes, and the
    paths that name them, are disclosed only to a caller who may also read the Dag's code.
    """
    values_authorized = get_auth_manager().is_authorized_dag(
        method="GET",
        access_entity=DagAccessEntity.CODE,
        details=DagDetails(id=dag_id, team_name=DagModel.get_team_name(dag_id, session=session)),
        user=user,
    )
    try:
        result = DagVersion.get_diff(
            dag_id,
            base_version_number,
            target_version_number,
            values_status="available" if values_authorized else "unavailable",
            max_changes=max_changes,
            session=session,
        )
    except DagVersionNotFound as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(error))
    except ValueError as error:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(error))

    changes = result["changes"]
    return DagVersionDiffResponse(
        diff_schema_version=result["diff_schema_version"],
        base_version_number=base_version_number,
        target_version_number=target_version_number,
        # Renamed on the way out: the engine key reads as a sibling of `diff_schema_version`,
        # which versions this payload rather than the serialized Dags it describes.
        serializer_versions=result["serialized_dag_schema_versions"],
        mode=result["mode"],
        unavailable_reason=result.get("unavailable_reason"),
        values_status=result["values"]["status"],
        truncated=result["truncated"],
        total_changes=sum(change["occurrence_count"] for change in changes),
        changes=changes,
    )


@dag_versions_router.get(
    "/{version_number}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.VERSION))],
)
def get_dag_version(
    dag_id: str,
    version_number: int,
    session: SessionDep,
) -> DagVersionResponse:
    """Get one Dag Version."""
    dag_version = session.scalar(
        select(DagVersion)
        .filter_by(dag_id=dag_id, version_number=version_number)
        .options(joinedload(DagVersion.dag_model))
    )

    if dag_version is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagVersion with dag_id: `{dag_id}` and version_number: `{version_number}` was not found",
        )

    return dag_version


@dag_versions_router.get(
    "",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ],
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.VERSION))],
)
def get_dag_versions(
    dag_id: str,
    session: SessionDep,
    limit: QueryLimit,
    offset: QueryOffset,
    version_number: Annotated[
        FilterParam[int], Depends(filter_param_factory(DagVersion.version_number, int))
    ],
    bundle_name: Annotated[FilterParam[str], Depends(filter_param_factory(DagVersion.bundle_name, str))],
    bundle_version: Annotated[
        FilterParam[str | None], Depends(filter_param_factory(DagVersion.bundle_version, str | None))
    ],
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(["id", "version_number", "bundle_name", "bundle_version"], DagVersion).dynamic_depends()
        ),
    ],
    dag_bag: DagBagDep,
    readable_dag_versions_filter: ReadableDagVersionsFilterDep,
) -> DAGVersionCollectionResponse:
    """
    Get all Dag Versions.

    This endpoint allows specifying `~` as the dag_id to retrieve Dag Versions for all Dags.
    """
    query = select(DagVersion).options(joinedload(DagVersion.dag_model), joinedload(DagVersion.bundle))

    if dag_id != "~":
        get_latest_version_of_dag(dag_bag, dag_id, session)
        query = query.filter(DagVersion.dag_id == dag_id)

    dag_versions_select, total_entries = paginated_select(
        statement=query,
        filters=[version_number, bundle_name, bundle_version, readable_dag_versions_filter],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    dag_versions = session.scalars(dag_versions_select)

    return DAGVersionCollectionResponse(
        dag_versions=dag_versions,
        total_entries=total_entries,
    )
