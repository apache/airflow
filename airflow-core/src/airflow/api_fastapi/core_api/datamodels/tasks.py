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

import inspect
from collections import abc
from datetime import datetime
from typing import Any

from pydantic import computed_field, field_validator, model_validator

from airflow.api_fastapi.common.types import TimeDeltaWithValidation
from airflow.api_fastapi.core_api.base import BaseModel
from airflow.serialization.serialized_objects import encode_priority_weight_strategy
from airflow.task.priority_strategy import PriorityWeightStrategy


def _get_class_ref(obj) -> dict[str, str | None]:
    """Return the class_ref dict for obj."""
    module_path = getattr(obj, "_task_module", None)
    if module_path is None:
        module_type = inspect.getmodule(obj)
        module_path = module_type.__name__ if module_type else None

    class_name = obj.task_type

    return {
        "module_path": module_path,
        "class_name": class_name,
    }


class TaskResponse(BaseModel):
    """Task serializer for responses."""

    task_id: str | None
    task_display_name: str | None
    owner: str | None
    start_date: datetime | None
    end_date: datetime | None
    trigger_rule: str | None
    depends_on_past: bool
    wait_for_downstream: bool
    retries: float | None
    queue: str | None
    pool: str | None
    pool_slots: float | None
    execution_timeout: TimeDeltaWithValidation | None
    retry_delay: TimeDeltaWithValidation | None
    retry_exponential_backoff: bool
    priority_weight: float | None
    weight_rule: str | None
    ui_color: str | None
    ui_fgcolor: str | None
    template_fields: list[str] | None
    downstream_task_ids: list[str] | None
    doc_md: str | None
    operator_name: str | None
    params: abc.MutableMapping | None
    class_ref: dict | None
    is_mapped: bool | None

    @model_validator(mode="before")
    @classmethod
    def validate_model(cls, task: Any) -> Any:
        task.__dict__.update({"class_ref": _get_class_ref(task), "is_mapped": task.is_mapped})
        return task

    @field_validator("weight_rule", mode="before")
    @classmethod
    def validate_weight_rule(cls, wr: str | PriorityWeightStrategy | None) -> str | None:
        """Validate the weight_rule property."""
        if wr is None:
            return None
        if isinstance(wr, str):
            return wr
        return encode_priority_weight_strategy(wr)

    @field_validator("params", mode="before")
    @classmethod
    def get_params(cls, params: abc.MutableMapping | None) -> dict | None:
        """Convert params attribute to dict representation."""
        if params is None:
            return None
        return {param_name: param_val.dump() for param_name, param_val in params.items()}

    # Mypy issue https://github.com/python/mypy/issues/1362
    @computed_field  # type: ignore[misc]
    @property
    def extra_links(self) -> list[str]:
        """Extract and return extra_links."""
        return getattr(self, "operator_extra_links", [])


class TaskCollectionResponse(BaseModel):
    """Task collection serializer for responses."""

    tasks: list[TaskResponse]
    total_entries: int
