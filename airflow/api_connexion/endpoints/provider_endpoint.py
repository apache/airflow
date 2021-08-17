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

import re
from typing import Dict, List

from airflow.api_connexion import security
from airflow.api_connexion.schemas.provider_schema import ProviderCollection, provider_collection_schema
from airflow.providers_manager import ProviderInfo, ProvidersManager
from airflow.security import permissions


def _remove_rst_syntax(value: str) -> str:
    return re.sub("[`_<>]", "", value.strip(" \n."))


def _provider_mapper(provider: ProviderInfo) -> Dict:
    return {
        "package_name": provider[1]["package-name"],
        "description": _remove_rst_syntax(provider[1]["description"]),
        "version": provider[0],
    }


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_PROVIDER)])
def get_providers():
    """Get providers"""
    providers_info: List[ProviderInfo] = list(ProvidersManager().providers.values())
    providers = [_provider_mapper(d) for d in providers_info]
    total_entries = len(providers)
    return provider_collection_schema.dump(
        ProviderCollection(providers=providers, total_entries=total_entries)
    )
