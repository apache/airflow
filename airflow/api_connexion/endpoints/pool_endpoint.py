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

from http import HTTPStatus
from typing import TYPE_CHECKING

from flask import Response
from marshmallow import ValidationError
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError

from airflow.api_connexion import security
from airflow.api_connexion.endpoints.request_dict import get_json_request_dict
from airflow.api_connexion.exceptions import AlreadyExists, BadRequest, NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.pool_schema import PoolCollection, pool_collection_schema, pool_schema
from airflow.models.pool import Pool
from airflow.utils.api_migration import mark_fastapi_migration_done
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.www.decorators import action_logging

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_connexion.types import APIResponse, UpdateMask


@mark_fastapi_migration_done
@security.requires_access_pool("DELETE")
@action_logging
@provide_session
def delete_pool(*, pool_name: str, session: Session = NEW_SESSION) -> APIResponse:
    """Delete a pool."""
    if pool_name == "default_pool":
        raise BadRequest(detail="Default Pool can't be deleted")
    affected_count = session.execute(delete(Pool).where(Pool.pool == pool_name)).rowcount

    if affected_count == 0:
        raise NotFound(detail=f"Pool with name:'{pool_name}' not found")
    return Response(status=HTTPStatus.NO_CONTENT)


@mark_fastapi_migration_done
@security.requires_access_pool("GET")
@provide_session
def get_pool(*, pool_name: str, session: Session = NEW_SESSION) -> APIResponse:
    """Get a pool."""
    obj = session.scalar(select(Pool).where(Pool.pool == pool_name))
    if obj is None:
        raise NotFound(detail=f"Pool with name:'{pool_name}' not found")
    return pool_schema.dump(obj)


@mark_fastapi_migration_done
@security.requires_access_pool("GET")
@format_parameters({"limit": check_limit})
@provide_session
def get_pools(
    *,
    limit: int,
    order_by: str = "id",
    offset: int | None = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get all pools."""
    to_replace = {"name": "pool"}
    allowed_sort_attrs = ["name", "slots", "id"]
    total_entries = session.scalars(func.count(Pool.id)).one()
    query = select(Pool)
    query = apply_sorting(query, order_by, to_replace, allowed_sort_attrs)
    pools = session.scalars(query.offset(offset).limit(limit)).all()
    return pool_collection_schema.dump(PoolCollection(pools=pools, total_entries=total_entries))


@mark_fastapi_migration_done
@security.requires_access_pool("PUT")
@action_logging
@provide_session
def patch_pool(
    *,
    pool_name: str,
    update_mask: UpdateMask = None,
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Update a pool."""
    request_dict = get_json_request_dict()
    # Only slots and include_deferred can be modified in 'default_pool'
    if pool_name == Pool.DEFAULT_POOL_NAME and request_dict.get("name", None) != Pool.DEFAULT_POOL_NAME:
        if update_mask and all(mask.strip() in {"slots", "include_deferred"} for mask in update_mask):
            pass
        else:
            raise BadRequest(detail="Default Pool's name can't be modified")

    pool = session.scalar(select(Pool).where(Pool.pool == pool_name).limit(1))
    if not pool:
        raise NotFound(detail=f"Pool with name:'{pool_name}' not found")

    try:
        patch_body = pool_schema.load(request_dict)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))

    if update_mask:
        update_mask = [i.strip() for i in update_mask]
        _patch_body = {}
        try:
            # MyPy infers a List[Optional[str]]  type here but it should be a List[str]
            # there is no way field is None here (UpdateMask is a List[str])
            # so if pool_schema.declared_fields[field].attribute is None file is returned
            update_mask = [
                (
                    pool_schema.declared_fields[field].attribute  # type: ignore[misc]
                    if pool_schema.declared_fields[field].attribute
                    else field
                )
                for field in update_mask
            ]
        except KeyError as err:
            raise BadRequest(detail=f"Invalid field: {err.args[0]} in update mask")
        _patch_body = {field: patch_body[field] for field in update_mask}
        patch_body = _patch_body

    else:
        required_fields = {"name", "slots"}
        fields_diff = required_fields.difference(get_json_request_dict())
        if fields_diff:
            raise BadRequest(detail=f"Missing required property(ies): {sorted(fields_diff)}")

    for key, value in patch_body.items():
        setattr(pool, key, value)
    session.commit()
    return pool_schema.dump(pool)


@mark_fastapi_migration_done
@security.requires_access_pool("POST")
@action_logging
@provide_session
def post_pool(*, session: Session = NEW_SESSION) -> APIResponse:
    """Create a pool."""
    required_fields = {"name", "slots"}  # Pool would require both fields in the post request
    fields_diff = required_fields.difference(get_json_request_dict())
    if fields_diff:
        raise BadRequest(detail=f"Missing required property(ies): {sorted(fields_diff)}")

    try:
        post_body = pool_schema.load(get_json_request_dict(), session=session)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))

    pool = Pool(**post_body)
    try:
        session.add(pool)
        session.commit()
        return pool_schema.dump(pool)
    except IntegrityError:
        raise AlreadyExists(detail=f"Pool: {post_body['pool']} already exists")
