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

from fastapi import HTTPException, status
from sqlalchemy import func, select

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import RoleCollectionResponse
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.teams import (
    TeamBody,
    TeamCollectionResponse,
    TeamResponse,
)
from airflow.providers.fab.auth_manager.api_fastapi.sorting import build_ordering
from airflow.providers.fab.auth_manager.models import Role, Team
from airflow.providers.fab.www.utils import get_fab_auth_manager


class FABAuthManagerTeams:
    """Service layer for FAB Auth Manager team operations."""

    @classmethod
    def create_team(cls, body: TeamBody) -> TeamResponse:
        security_manager = get_fab_auth_manager().security_manager

        existing = security_manager.find_team(name=body.name)
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Team with name {body.name!r} already exists",
            )

        security_manager.add_team(body.name)

        created = security_manager.find_team(name=body.name)
        if not created:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Team was not created due to an unexpected error.",
            )

        return TeamResponse.model_validate(created)

    @classmethod
    def get_teams(cls, *, order_by: str, limit: int, offset: int) -> TeamCollectionResponse:
        security_manager = get_fab_auth_manager().security_manager
        session = security_manager.session

        total_entries = session.scalars(select(func.count(Team.id))).one()

        ordering = build_ordering(order_by, allowed={"name": Team.name, "role_id": Team.id})

        stmt = select(Team).order_by(ordering).offset(offset).limit(limit)
        teams = session.scalars(stmt).unique().all()

        return TeamCollectionResponse(
            teams=[TeamResponse.model_validate(t) for t in teams],
            total_entries=total_entries,
        )

    @classmethod
    def get_team_roles(cls, *, team: str, order_by: str, limit: int, offset: int) -> RoleCollectionResponse:
        security_manager = get_fab_auth_manager().security_manager
        session = security_manager.session

        existing = security_manager.find_team(name=team)
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Team with name {team!r} does not exist."
            )
        total_entries = session.scalars(select(func.count(Role.id)).where(Role.name.startswith(team))).one()

        ordering = build_ordering(order_by, allowed={"name": Role.name, "role_id": Role.id})

        stmt = select(Role).where(Role.name.startswith(team)).order_by(ordering).offset(offset).limit(limit)
        roles = session.scalars(stmt).unique().all()

        return RoleCollectionResponse(
            roles=[RoleCollectionResponse.model_validate(r) for r in roles], total_entries=total_entries
        )

    @classmethod
    def delete_team(cls, name: str) -> None:
        security_manager = get_fab_auth_manager().security_manager

        existing = security_manager.find_team(name=name)
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Team with name {name!r} does not exist.",
            )
        security_manager.delete_team(existing.name)

    @classmethod
    def get_team(cls, name: str) -> TeamResponse:
        security_manager = get_fab_auth_manager().security_manager

        existing = security_manager.find_team(name=name)
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Team with name {name!r} does not exist.",
            )
        return TeamResponse.model_validate(existing)
