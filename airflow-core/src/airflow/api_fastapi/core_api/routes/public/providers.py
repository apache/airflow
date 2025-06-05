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

from fastapi import Depends

from airflow.api_fastapi.auth.managers.models.resource_details import AccessView
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.providers import ProviderCollectionResponse
from airflow.api_fastapi.core_api.security import requires_access_view
from airflow.api_fastapi.core_api.services.public.providers import _provider_mapper
from airflow.providers_manager import ProvidersManager

providers_router = AirflowRouter(tags=["Provider"], prefix="/providers")


@providers_router.get(
    "",
    dependencies=[Depends(requires_access_view(AccessView.PROVIDERS))],
)
def get_providers(
    limit: QueryLimit,
    offset: QueryOffset,
) -> ProviderCollectionResponse:
    """Get providers."""
    providers = sorted(
        [_provider_mapper(d) for d in ProvidersManager().providers.values()], key=lambda x: x.package_name
    )
    total_entries = len(providers)

    if limit.value is not None and offset.value is not None:
        providers = providers[offset.value : offset.value + limit.value]
    return ProviderCollectionResponse(providers=providers, total_entries=total_entries)
