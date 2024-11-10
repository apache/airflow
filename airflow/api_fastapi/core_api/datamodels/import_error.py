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

from pydantic import BaseModel, ConfigDict, Field


class ImportErrorResponse(BaseModel):
    """Import Error Response."""

    id: int = Field(alias="import_error_id")
    timestamp: datetime
    filename: str
    stacktrace: str = Field(alias="stack_trace")

    model_config = ConfigDict(populate_by_name=True)


class ImportErrorCollectionResponse(BaseModel):
    """Import Error Collection Response."""

    import_errors: list[ImportErrorResponse]
    total_entries: int
