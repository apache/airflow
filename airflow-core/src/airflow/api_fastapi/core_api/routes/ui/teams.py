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

from fastapi import Depends, HTTPException, status
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    QueryLimit,
    QueryOffset,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.teams import TeamCollectionResponse, TeamResponse
from airflow.api_fastapi.core_api.security import (
    ReadableTeamsFilterDep,
    requires_authenticated,
)
from airflow.configuration import conf
from airflow.models.team import Team

teams_router = AirflowRouter(tags=["Teams"], prefix="/teams")


@teams_router.get(
    path="",
    dependencies=[Depends(requires_authenticated())],
)
def list_teams(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["name"], Team).dynamic_depends()),
    ],
    readable_teams_filter: ReadableTeamsFilterDep,
    session: SessionDep,
) -> TeamCollectionResponse:
    if not conf.getboolean("core", "multi_team"):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "Multi-team mode is not configured in the Airflow environment"
        )

    select_stmt, total_entries = paginated_select(
        statement=select(Team),
        filters=[readable_teams_filter],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    teams = [
        TeamResponse(**row._mapping) if not isinstance(row, Team) else row
        for row in session.scalars(select_stmt)
    ]
    return TeamCollectionResponse(
        teams=teams,
        total_entries=total_entries,
    )
