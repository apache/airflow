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

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class GenerateSharedLinkRequest(BaseModel):
    """Request model for generating Human-in-the-loop shared links."""

    link_type: Literal["ui_redirect", "perform_action"] = Field(
        default="ui_redirect",
        description="Type of link to generate: 'ui_redirect' for UI interaction or 'perform_action' for direct execution",
    )
    chosen_options: list[str] | None = Field(
        default=None,
        description="Chosen options for direct_action links",
    )
    params_input: dict[str, Any] | None = Field(
        default=None,
        description="Parameters input for direct_action links",
    )
    expires_at: datetime | None = Field(
        default=None,
        description="Time that the link should expire at",
    )


class GenerateSharedLinkResponse(BaseModel):
    """Response model for generated Human-in-the-loop shared links."""

    url: str
    expires_at: str
    link_type: str
