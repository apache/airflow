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

from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_0_PLUS

# Resource Constants
RESOURCE_BACKFILL = "Backfills"
RESOURCE_DAG_VERSION = "DAG Versions"
RESOURCE_ASSET_ALIAS = "Asset Aliases"
if AIRFLOW_V_3_0_PLUS:
    RESOURCE_ASSET = "Assets"
else:
    from airflow.security.permissions import (  # type: ignore[attr-defined, no-redef]
        RESOURCE_DATASET as RESOURCE_ASSET,  # noqa: F401
    )
