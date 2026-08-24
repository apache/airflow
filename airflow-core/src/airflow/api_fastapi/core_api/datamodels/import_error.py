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

from pydantic import Field, computed_field

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.api_fastapi.core_api.datamodels.dags import create_file_token


class ImportErrorResponse(BaseModel):
    """Import Error Response."""

    id: int = Field(alias="import_error_id")
    timestamp: datetime
    filename: str
    bundle_name: str | None
    stacktrace: str = Field(alias="stack_trace")

    @computed_field
    @property
    def file_token(self) -> str:
        """Return file token for reparsing the failed Dag file."""
        return create_file_token(
            bundle_name=self.bundle_name,
            relative_fileloc=self.filename,
        )


class ImportErrorCollectionResponse(BaseModel):
    """Import Error Collection Response."""

    import_errors: Iterable[ImportErrorResponse]
    total_entries: int
