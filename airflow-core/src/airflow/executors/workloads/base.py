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
"""ORM models and Pydantic schemas for BaseWorkload."""

from __future__ import annotations

import os
from abc import ABC
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.tokens import JWTGenerator


class BaseWorkload:
    """
    Mixin for ORM models that can be scheduled as workloads.

    This mixin defines the interface that scheduler workloads (TaskInstance,
    ExecutorCallback, etc.) must implement to provide routing information to the scheduler.

    Subclasses must override:
    - get_dag_id() -> str | None
    - get_executor_name() -> str | None
    """

    def get_dag_id(self) -> str | None:
        """
        Return the DAG ID for scheduler routing.

        Must be implemented by subclasses.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement get_dag_id()")

    def get_executor_name(self) -> str | None:
        """
        Return the executor name for scheduler routing.

        Must be implemented by subclasses.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement get_executor_name()")


class BundleInfo(BaseModel):
    """Schema for telling task which bundle to run with."""

    name: str
    version: str | None = None


class BaseWorkloadSchema(BaseModel):
    """Base Pydantic schema for executor workload DTOs."""

    model_config = ConfigDict(populate_by_name=True)

    token: str
    """The identity token for this workload"""

    @staticmethod
    def generate_token(sub_id: str, generator: JWTGenerator | None = None) -> str:
        return generator.generate({"sub": sub_id}) if generator else ""


class BaseDagBundleWorkload(BaseWorkloadSchema, ABC):
    """Base class for Workloads that are associated with a DAG bundle."""

    dag_rel_path: os.PathLike[str]  # Filepath where the DAG can be found (likely prefixed with `DAG_FOLDER/`)
    bundle_info: BundleInfo
    log_path: str | None  # Rendered relative log filename template the task logs should be written to.
