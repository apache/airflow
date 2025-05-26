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

from typing import cast

from fastapi import Depends

from airflow import plugins_manager
from airflow.api_fastapi.auth.managers.models.resource_details import AccessView
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.plugins import (
    PluginCollectionResponse,
    PluginImportErrorCollectionResponse,
    PluginResponse,
)
from airflow.api_fastapi.core_api.security import requires_access_view

plugins_router = AirflowRouter(tags=["Plugin"], prefix="/plugins")


@plugins_router.get(
    "",
    dependencies=[Depends(requires_access_view(AccessView.PLUGINS))],
)
def get_plugins(
    limit: QueryLimit,
    offset: QueryOffset,
) -> PluginCollectionResponse:
    plugins_info = sorted(plugins_manager.get_plugin_info(), key=lambda x: x["name"])
    return PluginCollectionResponse(
        plugins=cast("list[PluginResponse]", plugins_info[offset.value :][: limit.value]),
        total_entries=len(plugins_info),
    )


@plugins_router.get(
    "/importErrors",
    dependencies=[Depends(requires_access_view(AccessView.PLUGINS))],
)
def import_errors() -> PluginImportErrorCollectionResponse:
    plugins_manager.ensure_plugins_loaded()  # make sure import_errors are loaded

    return PluginImportErrorCollectionResponse.model_validate(
        {
            "import_errors": [
                {"source": source, "error": error} for source, error in plugins_manager.import_errors.items()
            ],
            "total_entries": len(plugins_manager.import_errors),
        }
    )
