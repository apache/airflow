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

from typing import Any

from pydantic import BaseModel, Field


class GenerateSharedLinkRequest(BaseModel):
    """Request model for generating HITL shared links."""

    link_type: str = Field(
        default="direct_action",
        description="Type of link to generate: 'ui_redirect' for UI interaction or 'direct_action' for direct execution",
    )
    action: str | None = Field(
        default=None,
        description="Optional action to perform when link is accessed (e.g., 'approve', 'reject'). Required for direct_action links.",
    )
    chosen_options: list[str] | None = Field(
        default=None,
        description="Chosen options for direct_action links",
    )
    params_input: dict[str, Any] | None = Field(
        default=None,
        description="Parameters input for direct_action links",
    )
    expiration_hours: int | None = Field(
        default=None,
        description="Custom expiration time in hours",
    )


class GenerateSharedLinkResponse(BaseModel):
    """Response model for generated HITL shared links."""

    url: str
    expires_at: str
    link_type: str
    action: str | None
    dag_id: str
    dag_run_id: str
    task_id: str
    try_number: int
    map_index: int | None
    task_instance_uuid: str
