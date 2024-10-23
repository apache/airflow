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

from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.serializers.plugins import PluginCollectionResponse, PluginResponse
from airflow.plugins_manager import get_plugin_info

plugins_router = AirflowRouter(tags=["Plugin"], prefix="/plugins")


@plugins_router.get("/")
async def get_plugins(
    limit: QueryLimit,
    offset: QueryOffset,
) -> PluginCollectionResponse:
    plugins_info = sorted(get_plugin_info(), key=lambda x: x["name"])
    return PluginCollectionResponse(
        plugins=[
            PluginResponse.model_validate(plugin_info)
            for plugin_info in plugins_info[offset.value :][: limit.value]
        ],
        total_entries=len(plugins_info),
    )
