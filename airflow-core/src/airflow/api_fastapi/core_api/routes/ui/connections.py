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

from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.connections import ConnectionHookMetaData
from airflow.api_fastapi.core_api.security import requires_access_connection
from airflow.api_fastapi.core_api.services.ui.connections import HookMetaService

connections_router = AirflowRouter(tags=["Connection"], prefix="/connections")


@connections_router.get(
    "/hook_meta",
    dependencies=[Depends(requires_access_connection(method="GET"))],
)
def hook_meta_data() -> list[ConnectionHookMetaData]:
    """Retrieve information about available connection types (hook classes) and their parameters."""
    # Note:
    # This endpoint is implemented to serve the connections form in the UI. It is building on providers
    # manager and hooks being loaded in the API server. Target should be that the API server reads the
    # information from a database table to un-bundle the dependency for provider package install from
    # the API server.
    return HookMetaService.hook_meta_data()
