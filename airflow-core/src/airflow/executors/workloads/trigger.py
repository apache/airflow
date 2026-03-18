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
"""Trigger workload schemas for executor communication."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

# Using noqa because Ruff wants this in a TYPE_CHECKING block but Pydantic fails if it is.
from airflow.executors.workloads.task import TaskInstanceDTO  # noqa: TCH001


class RunTrigger(BaseModel):
    """
    Execute an async "trigger" process that yields events.

    Consumers of this Workload must perform their own validation of the classpath input.
    """

    id: int
    ti: TaskInstanceDTO | None  # Could be none for asset-based triggers.
    classpath: str  # Dot-separated name of the module+fn to import and run this workload.
    encrypted_kwargs: str
    timeout_after: datetime | None = None
    type: Literal["RunTrigger"] = Field(init=False, default="RunTrigger")
