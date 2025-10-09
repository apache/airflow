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

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel


class AssetResponse(BaseModel):
    """Asset schema for responses with fields that are needed for Runtime."""

    name: str
    uri: str
    group: str
    extra: dict | None = None


class AssetAliasResponse(BaseModel):
    """Asset alias schema with fields that are needed for Runtime."""

    name: str
    group: str


class AssetProfile(StrictBaseModel):
    """
    Profile of an asset-like object.

    Asset will have name, uri defined, with type set to 'Asset'.
    AssetNameRef will have name defined, type set to 'AssetNameRef'.
    AssetUriRef will have uri defined, type set to 'AssetUriRef'.
    AssetAlias will have name defined, type set to 'AssetAlias'.

    Note that 'type' here is distinct from 'asset_type' the user declares on an
    Asset (or subclass). This field is for distinguishing between different
    asset-related types (Asset, AssetRef, or AssetAlias).
    """

    name: str | None = None
    uri: str | None = None
    type: str
