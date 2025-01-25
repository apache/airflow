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

from typing import Annotated, Callable

from pydantic import BeforeValidator, ConfigDict, Field

from airflow.api_fastapi.core_api.base import BaseModel


def _call_function(function: Callable[[], int]) -> int:
    """
    Call the given function.

    Used for the BeforeValidator to get the actual values from the bound method.
    """
    return function()


class BasePool(BaseModel):
    """Base serializer for Pool."""

    pool: str = Field(serialization_alias="name")
    slots: int
    description: str | None
    include_deferred: bool


class PoolResponse(BasePool):
    """Pool serializer for responses."""

    occupied_slots: Annotated[int, BeforeValidator(_call_function)]
    running_slots: Annotated[int, BeforeValidator(_call_function)]
    queued_slots: Annotated[int, BeforeValidator(_call_function)]
    scheduled_slots: Annotated[int, BeforeValidator(_call_function)]
    open_slots: Annotated[int, BeforeValidator(_call_function)]
    deferred_slots: Annotated[int, BeforeValidator(_call_function)]


class PoolCollectionResponse(BaseModel):
    """Pool Collection serializer for responses."""

    pools: list[PoolResponse]
    total_entries: int


class PoolPatchBody(BaseModel):
    """Pool serializer for patch bodies."""

    model_config = ConfigDict(populate_by_name=True, from_attributes=True)

    name: str | None = Field(default=None, alias="pool")
    slots: int | None = None
    description: str | None = None
    include_deferred: bool | None = None


class PoolBody(BasePool):
    """Pool serializer for post bodies."""

    pool: str = Field(alias="name", max_length=256)
    description: str | None = None
    include_deferred: bool = False


#
# def _pool_body_discriminator(pool_body: Any) -> str:
#     return "post" if "pool" in pool_body else "patch"
#
#
# class PoolBody:
#     """Pool serializer for request bodies."""
#
#     pool: Annotated[
#         Union[Annotated[PoolPostBody, Tag("post")], Annotated[PoolPatchBody, Tag("patch")]],
#         Discriminator(_pool_body_discriminator),
#     ]
