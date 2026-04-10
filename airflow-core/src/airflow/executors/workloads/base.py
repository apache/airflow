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
from abc import ABC, abstractmethod
from collections.abc import Hashable
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.tokens import JWTGenerator
    from airflow.executors.workloads.types import WorkloadState


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

    token: str = Field(repr=False)
    """The identity token for this workload"""

    @staticmethod
    def generate_token(sub_id: str, generator: JWTGenerator | None = None) -> str:
        return generator.generate({"sub": sub_id}) if generator else ""


class BaseDagBundleWorkload(BaseWorkloadSchema, ABC):
    """Base class for Workloads that are associated with a DAG bundle."""

    dag_rel_path: os.PathLike[str]  # Filepath where the DAG can be found (likely prefixed with `DAG_FOLDER/`)
    bundle_info: BundleInfo
    log_path: str | None  # Rendered relative log filename template the task logs should be written to.

    @property
    @abstractmethod
    def key(self) -> Hashable:
        """
        Return the unique key identifying this workload instance.

        Used by executors for tracking queued/running workloads and reporting results.
        Must be a hashable value suitable for use in sets and as dict keys.

        Must be implemented by subclasses.
        """
        ...

    @property
    @abstractmethod
    def display_name(self) -> str:
        """
        Return a human-readable name for this workload, suitable for logging and process titles.

        Used by executors to set worker process titles and log messages.

        Must be implemented by subclasses.

        Example::

            # For a task workload:
            return str(self.ti.id)  # "4d828a62-a417-4936-a7a6-2b3fabacecab"

            # For a callback workload:
            return str(self.callback.id)  # "12345678-1234-5678-1234-567812345678"

            # Results in process titles like:
            # "airflow worker -- LocalExecutor: 4d828a62-a417-4936-a7a6-2b3fabacecab"
        """
        ...

    @property
    @abstractmethod
    def success_state(self) -> WorkloadState:
        """
        Return the state value representing successful completion of this workload type.

        Must be implemented by subclasses.
        """
        ...

    @property
    @abstractmethod
    def failure_state(self) -> WorkloadState:
        """
        Return the state value representing failed completion of this workload type.

        Must be implemented by subclasses.
        """
        ...

    @property
    def running_state(self) -> WorkloadState | None:
        """
        Return the state value representing that this workload is actively running.

        Called by the executor worker *before* execution begins. Subclasses may override
        this to emit an intermediate state transition (e.g. callbacks need
        QUEUED → RUNNING → SUCCESS/FAILED). Returns ``None`` by default, meaning
        no intermediate state is emitted.
        """
        return None
