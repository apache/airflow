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

destination_snowflake = {
    "destinationDefinitionId": "424892c4-daac-4491-b35d-c6688ba547ba",
    "destinationId": "d401bafb-aa51-4c70-8a1c-1aa07f3d0a2d",
    "workspaceId": "ef6c54d6-08b4-4b02-bbf4-1044e178f984",
    "connectionConfiguration": {
        "host": "localhost",
        "role": "",
        "schema": "public",
        "database": "ol",
        "username": "",
        "warehouse": "compute_wh",
        "credentials": {"password": "", "auth_type": "Username and Password"},
        "disable_type_dedupe": False,
        "retention_period_days": 1,
    },
    "name": "Snowflake",
    "destinationName": "Snowflake",
    "icon": "https://connectors.airbyte.com/files/metadata/airbyte/destination-snowflake/latest/icon.svg",
    "isVersionOverrideApplied": False,
    "supportState": "supported",
}
