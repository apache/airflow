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
from typing import Any
from uuid import UUID

from pydantic import Field, model_validator

from airflow.api_fastapi.core_api.base import StrictBaseModel
from airflow.api_fastapi.execution_api.datamodels.job import DagParseTokenBody
from airflow.models.dagwarning import DagWarningTypeValue


class ParseWarning(StrictBaseModel):
    """A warning attached to a Dag returned by this import."""

    dag_id: str = Field(min_length=1, max_length=250)
    warning_type: DagWarningTypeValue
    message: str


class ParseSourceCode(StrictBaseModel):
    """Source captured by the importer; null explicitly means unavailable."""

    source_code: str | None
    language: str = Field(min_length=1, max_length=64)


class DagParseResultBody(DagParseTokenBody):
    """One completed file or container import, including an empty result."""

    dispatch_sequence: int = Field(ge=1, le=2**53 - 1)
    bundle_version: str | None = Field(default=None, max_length=200)
    version_data: dict[str, Any] | None = None
    parse_duration: float = Field(ge=0, allow_inf_nan=False)
    serialized_dags: list[dict[str, Any]]
    import_errors: dict[str, str] = Field(default_factory=dict)
    warnings: list[ParseWarning] = Field(default_factory=list)
    parsed_definitions: list[str] = Field(default_factory=list)
    source_codes: dict[str, ParseSourceCode]

    @model_validator(mode="after")
    def validate_file_scope(self) -> DagParseResultBody:
        paths = [*self.parsed_definitions, *self.import_errors]
        dag_ids: set[str] = set()
        filelocs: set[str] = set()
        for document in self.serialized_dags:
            dag = document.get("dag")
            if not isinstance(dag, dict) or not isinstance(dag.get("dag_id"), str):
                raise ValueError("A serialized Dag must contain a dag_id")
            if dag["dag_id"] in dag_ids:
                raise ValueError("Duplicate Dag id in publication")
            dag_ids.add(dag["dag_id"])
            relative_fileloc = dag.get("relative_fileloc")
            if not isinstance(relative_fileloc, str):
                raise ValueError("Every definition must have a bundle-relative location")
            paths.append(relative_fileloc)
            fileloc = dag.get("fileloc")
            if not isinstance(fileloc, str):
                raise ValueError("A serialized Dag must contain a fileloc")
            filelocs.add(fileloc)
        for path in paths:
            if len(path) > 2000:
                raise ValueError("Definition location exceeds 2000 characters")
            self.validate_relative_fileloc(path)
            if path != self.relative_fileloc and not path.startswith(self.relative_fileloc + "/"):
                raise ValueError("Definition or diagnostic is outside the published file or container")
        if set(self.source_codes) != filelocs:
            raise ValueError("Supply source code, or explicit null, for every serialized fileloc")
        if any(warning.dag_id not in dag_ids for warning in self.warnings):
            raise ValueError("Warnings must refer to Dags in this publication")
        return self


class DagParseResultResponse(StrictBaseModel):
    """Receipt returned unchanged when the latest accepted publication is replayed."""

    attempt_id: UUID
    accepted_at: datetime
