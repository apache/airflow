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

source_postgresql = {
    "sourceDefinitionId": "decd338e-5647-4c0b-adf4-da0e75f5a750",
    "sourceId": "574b08f1-6c9e-46be-95e5-8a1669ba0847",
    "workspaceId": "ef6c54d6-08b4-4b02-bbf4-1044e178f984",
    "connectionConfiguration": {
        "host": "host.docker.internal",
        "port": 5439,
        "schemas": ["public"],
        "database": "destination",
        "password": "**********",
        "ssl_mode": {"mode": "disable"},
        "username": "airbyte",
        "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
        "replication_method": {"method": "Standard"},
    },
    "name": "Postgres",
    "sourceName": "Postgres",
    "icon": "https://connectors.airbyte.com/files/metadata/airbyte/source-postgres/latest/icon.svg",
    "isVersionOverrideApplied": False,
    "supportState": "supported",
}
