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

import re2

from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.serializers.providers import (
    ProviderCollectionResponse,
    ProviderResponse,
)
from airflow.providers_manager import ProviderInfo, ProvidersManager

providers_router = AirflowRouter(tags=["Provider"], prefix="/providers")


def _remove_rst_syntax(value: str) -> str:
    return re2.sub("[`_<>]", "", value.strip(" \n."))


def _provider_mapper(provider: ProviderInfo) -> ProviderResponse:
    return ProviderResponse(
        package_name=provider.data["package-name"],
        description=_remove_rst_syntax(provider.data["description"]),
        version=provider.version,
    )


@providers_router.get("/")
async def get_providers(
    limit: QueryLimit,
    offset: QueryOffset,
) -> ProviderCollectionResponse:
    """Get providers."""
    providers = sorted(
        [_provider_mapper(d) for d in ProvidersManager().providers.values()],
        key=lambda x: x.package_name,
    )
    total_entries = len(providers)

    if limit.value is not None and offset.value is not None:
        providers = providers[offset.value : offset.value + limit.value]
    return ProviderCollectionResponse(providers=providers, total_entries=total_entries)
