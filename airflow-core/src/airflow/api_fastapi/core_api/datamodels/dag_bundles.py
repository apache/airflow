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

from collections.abc import Iterable
from datetime import datetime

from pydantic import Field

from airflow.api_fastapi.core_api.base import BaseModel


class DagBundleResponse(BaseModel):
    """Dag bundle serializer for responses."""

    name: str
    active: bool | None = Field(
        description="Whether the bundle is still present in this deployment's configuration."
    )
    version: str | None = Field(
        description=(
            "The latest version Airflow has seen for the bundle. Null when the bundle does not "
            "support versioning, or when no Dag processor has refreshed it successfully yet."
        )
    )
    last_refreshed: datetime | None = Field(
        description=(
            "When a Dag processor last successfully refreshed the bundle. It advances even when the "
            "version did not change, and a failed refresh leaves it untouched."
        )
    )
    bundle_url: str | None = Field(
        description=(
            "A link to view the bundle at ``version``, when one is configured and the caller may "
            "read Dag versions."
        )
    )
    team_name: str | None = Field(description="The team owning the bundle, in a multi-team deployment.")
    import_error_count: int | None = Field(
        description=(
            "Number of Dag import errors recorded against this bundle that the caller is permitted "
            "to see, counted on the same terms as ``GET /importErrors``. Null when the caller may "
            "not read import errors."
        )
    )


class DagBundleCollectionResponse(BaseModel):
    """Dag bundle collection response."""

    dag_bundles: Iterable[DagBundleResponse]
    total_entries: int
