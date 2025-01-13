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
"""
Database helpers for Airflow REST API.

:meta private:
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import TYPE_CHECKING, Annotated, Literal, overload

import pendulum
from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from airflow.api_fastapi.core_api.security import get_user_with_exception_handling
from airflow.auth.managers.models.base_user import BaseUser
from airflow.models import Log
from airflow.utils.db import get_query_count, get_query_count_async
from airflow.utils.log import secrets_masker
from airflow.utils.session import NEW_SESSION, create_session, create_session_async, provide_session

if TYPE_CHECKING:
    from sqlalchemy.sql import Select

    from airflow.api_fastapi.common.parameters import BaseParam


def _get_session() -> Session:
    with create_session(scoped=False) as session:
        yield session


SessionDep = Annotated[Session, Depends(_get_session)]


def apply_filters_to_select(
    *, statement: Select, filters: Sequence[BaseParam | None] | None = None
) -> Select:
    if filters is None:
        return statement
    for f in filters:
        if f is None:
            continue
        statement = f.to_orm(statement)

    return statement


async def _get_async_session() -> AsyncSession:
    async with create_session_async() as session:
        yield session


AsyncSessionDep = Annotated[AsyncSession, Depends(_get_async_session)]


@overload
async def paginated_select_async(
    *,
    statement: Select,
    filters: Sequence[BaseParam] | None = None,
    order_by: BaseParam | None = None,
    offset: BaseParam | None = None,
    limit: BaseParam | None = None,
    session: AsyncSession,
    return_total_entries: Literal[True] = True,
) -> tuple[Select, int]: ...


@overload
async def paginated_select_async(
    *,
    statement: Select,
    filters: Sequence[BaseParam] | None = None,
    order_by: BaseParam | None = None,
    offset: BaseParam | None = None,
    limit: BaseParam | None = None,
    session: AsyncSession,
    return_total_entries: Literal[False],
) -> tuple[Select, None]: ...


async def paginated_select_async(
    *,
    statement: Select,
    filters: Sequence[BaseParam | None] | None = None,
    order_by: BaseParam | None = None,
    offset: BaseParam | None = None,
    limit: BaseParam | None = None,
    session: AsyncSession,
    return_total_entries: bool = True,
) -> tuple[Select, int | None]:
    statement = apply_filters_to_select(
        statement=statement,
        filters=filters,
    )

    total_entries = None
    if return_total_entries:
        total_entries = await get_query_count_async(statement, session=session)

    # TODO: Re-enable when permissions are handled. Readable / writable entities,
    # for instance:
    # readable_dags = get_auth_manager().get_permitted_dag_ids(user=g.user)
    # dags_select = dags_select.where(DagModel.dag_id.in_(readable_dags))

    statement = apply_filters_to_select(
        statement=statement,
        filters=[order_by, offset, limit],
    )

    return statement, total_entries


@overload
def paginated_select(
    *,
    statement: Select,
    filters: Sequence[BaseParam] | None = None,
    order_by: BaseParam | None = None,
    offset: BaseParam | None = None,
    limit: BaseParam | None = None,
    session: Session = NEW_SESSION,
    return_total_entries: Literal[True] = True,
) -> tuple[Select, int]: ...


@overload
def paginated_select(
    *,
    statement: Select,
    filters: Sequence[BaseParam] | None = None,
    order_by: BaseParam | None = None,
    offset: BaseParam | None = None,
    limit: BaseParam | None = None,
    session: Session = NEW_SESSION,
    return_total_entries: Literal[False],
) -> tuple[Select, None]: ...


@provide_session
def paginated_select(
    *,
    statement: Select,
    filters: Sequence[BaseParam] | None = None,
    order_by: BaseParam | None = None,
    offset: BaseParam | None = None,
    limit: BaseParam | None = None,
    session: Session = NEW_SESSION,
    return_total_entries: bool = True,
) -> tuple[Select, int | None]:
    statement = apply_filters_to_select(
        statement=statement,
        filters=filters,
    )

    total_entries = None
    if return_total_entries:
        total_entries = get_query_count(statement, session=session)

    # TODO: Re-enable when permissions are handled. Readable / writable entities,
    # for instance:
    # readable_dags = get_auth_manager().get_permitted_dag_ids(user=g.user)
    # dags_select = dags_select.where(DagModel.dag_id.in_(readable_dags))

    statement = apply_filters_to_select(statement=statement, filters=[order_by, offset, limit])

    return statement, total_entries


def action_logging(event: str | None):
    async def log_action(
        request: Request,
        session: SessionDep,
        user: Annotated[BaseUser, Depends(get_user_with_exception_handling)],
    ):
        """Log user actions."""
        if not user:
            user_name = "anonymous"
            user_display = ""
        else:
            user_name = getattr(user, "role", "unknown_role")
            user_display = getattr(user, "username", "unknown_user")

        # Extract basic request details
        query_params = dict(request.query_params)
        extra_fields = {
            "path": request.url.path,
            "method": request.method,
            "query_params": query_params,
        }

        # Add JSON body if present
        json_body = {}
        try:
            if request.headers.get("content-type") == "application/json":
                json_body = await request.json()
                extra_fields.update({k: secrets_masker.redact(v, k) for k, v in json_body.items()})
        except Exception as e:
            extra_fields["json_error"] = str(e)

        # Merge query parameters and JSON body to extract key fields
        params = {**query_params, **json_body}

        # Extract relevant fields for logging
        task_id = params.get("task_id")
        dag_id = params.get("dag_id")
        run_id = params.get("run_id") or params.get("dag_run_id")
        logical_date = params.get("logical_date")

        parsed_logical_date = None
        if logical_date:
            try:
                parsed_date = pendulum.parse(logical_date, strict=False)
                if isinstance(parsed_date, (pendulum.DateTime, pendulum.Date)):
                    parsed_logical_date = parsed_date.isoformat()
                else:
                    extra_fields["logical_date_error"] = (
                        f"Unsupported type for logical_date: {type(parsed_date).__name__}"
                    )
            except pendulum.exceptions.ParserError:
                extra_fields["logical_date_error"] = f"Invalid logical_date: {logical_date}"

        # Mask sensitive fields
        fields_skip_logging = {
            "csrf_token",
            "_csrf_token",
            "is_paused",
        }
        extra_fields = {k: v for k, v in extra_fields.items() if k not in fields_skip_logging}

        # Set event name dynamically if not provided
        event_name = event or request.url.path.strip("/").replace("/", ".")

        # Create log entry
        log = Log(
            event=event_name,
            task_instance=None,
            owner=user_name,
            owner_display_name=user_display,
            extra=json.dumps(extra_fields),
            task_id=task_id,
            dag_id=dag_id,
            run_id=run_id,
            logical_date=parsed_logical_date,
        )
        try:
            session.add(log)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e

    return log_action
