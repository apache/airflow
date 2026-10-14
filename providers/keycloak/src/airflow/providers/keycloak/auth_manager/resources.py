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

from enum import Enum


class KeycloakResource(Enum):
    """Enum of Keycloak resources."""

    # Views over records that carry no per-Dag or per-team key to authorize on (audit log rows not
    # tied to a Dag, import errors for files with no registered Dag, ...). In multi-team mode they
    # are checked against this resource rather than ``VIEW`` so that they are not granted along
    # with the views every team role can read.
    ADMIN_VIEW = "AdminView"
    ASSET = "Asset"
    ASSET_ALIAS = "AssetAlias"
    BACKFILL = "Backfill"
    CONFIGURATION = "Configuration"
    CONNECTION = "Connection"
    CUSTOM = "Custom"
    DAG = "Dag"
    MENU = "Menu"
    POOL = "Pool"
    TEAM = "Team"
    VARIABLE = "Variable"
    VIEW = "View"
