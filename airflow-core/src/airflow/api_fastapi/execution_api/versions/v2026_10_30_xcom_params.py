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

from cadwyn import VersionChange, schema

# These models live next to the routes that use them, and the routes import this version package, so the
# changes that touch them cannot sit in ``v2026_10_30`` without a circular import.
from airflow.api_fastapi.execution_api.routes.xcoms import GetXcomFilterParams, GetXComSliceFilterParams


class AddRegionSelectorsToXComFilterParams(VersionChange):
    """Add the `region_id` and `region_index` fields to GetXComSliceFilterParams and GetXcomFilterParams."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(GetXComSliceFilterParams).field("region_id").didnt_exist,
        schema(GetXComSliceFilterParams).field("region_index").didnt_exist,
        schema(GetXcomFilterParams).field("region_id").didnt_exist,
        schema(GetXcomFilterParams).field("region_index").didnt_exist,
    )
