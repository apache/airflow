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

from typing import TYPE_CHECKING

from airflow.api_connexion import security
from airflow.api_connexion.parameters import check_limit, format_parameters
from airflow.api_connexion.schemas.plugin_schema import PluginCollection, plugin_collection_schema
from airflow.auth.managers.models.resource_details import AccessView
from airflow.plugins_manager import get_plugin_info
from airflow.utils.api_migration import mark_fastapi_migration_done

if TYPE_CHECKING:
    from airflow.api_connexion.types import APIResponse


@mark_fastapi_migration_done
@security.requires_access_view(AccessView.PLUGINS)
@format_parameters({"limit": check_limit})
def get_plugins(*, limit: int, offset: int = 0) -> APIResponse:
    """Get plugins endpoint."""
    plugins_info = get_plugin_info()
    collection = PluginCollection(plugins=plugins_info[offset:][:limit], total_entries=len(plugins_info))
    return plugin_collection_schema.dump(collection)
