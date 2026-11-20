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
"""
Response fields that locate a task instance inside a dynamic region.

A task instance outside every region has neither ``region_id`` nor ``region_index`` in a response:
they are left out rather than sent as null or as the stored placeholder, and the JSON schema marks
them optional but not nullable.
"""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, Field, model_serializer
from pydantic.json_schema import SkipJsonSchema

from airflow.models.dynamic_region import SENTINEL_REGION_ID


def _drop_null_default(schema: dict[str, Any]) -> None:
    schema.pop("default", None)


RegionId = Annotated[SkipJsonSchema[None] | UUID, Field(json_schema_extra=_drop_null_default)]
RegionIndex = Annotated[SkipJsonSchema[None] | int, Field(json_schema_extra=_drop_null_default)]


class OmitsMissingRegion(BaseModel):
    """Leave ``region_id`` and ``region_index`` out of the output unless the task instance has a region."""

    @model_serializer(mode="wrap")
    def _omit_missing_region(self, handler):
        data = handler(self)
        region_id = data.get("region_id")
        if region_id is None or str(region_id) == str(SENTINEL_REGION_ID):
            data.pop("region_id", None)
            data.pop("region_index", None)
        return data
