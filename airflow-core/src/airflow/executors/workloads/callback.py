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
"""Callback workload schemas for executor communication."""

from __future__ import annotations

from enum import Enum
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from uuid import UUID

import structlog
from pydantic import BaseModel, Field, field_validator

from airflow.executors.workloads.base import BaseDagBundleWorkload, BundleInfo

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.tokens import JWTGenerator
    from airflow.models import DagRun
    from airflow.models.callback import Callback as CallbackModel, CallbackKey

log = structlog.get_logger(__name__)


class CallbackFetchMethod(str, Enum):
    """Methods used to fetch callback at runtime."""

    # For future use once Dag Processor callbacks (on_success_callback/on_failure_callback) get moved to executors
    DAG_ATTRIBUTE = "dag_attribute"

    # For deadline callbacks since they import callbacks through the import path
    IMPORT_PATH = "import_path"


class CallbackDTO(BaseModel):
    """Schema for Callback with minimal required fields needed for Executors and Task SDK."""

    id: str  # A uuid.UUID stored as a string
    fetch_method: CallbackFetchMethod
    data: dict

    @field_validator("id", mode="before")
    @classmethod
    def validate_id(cls, v):
        """Convert UUID to str if needed."""
        if isinstance(v, UUID):
            return str(v)
        return v

    @property
    def key(self) -> CallbackKey:
        """Return callback ID as key (CallbackKey = str)."""
        return self.id


class ExecuteCallback(BaseDagBundleWorkload):
    """Execute the given Callback."""

    callback: CallbackDTO

    type: Literal["ExecuteCallback"] = Field(init=False, default="ExecuteCallback")

    @classmethod
    def make(
        cls,
        callback: CallbackModel,
        dag_run: DagRun,
        dag_rel_path: Path | None = None,
        generator: JWTGenerator | None = None,
        bundle_info: BundleInfo | None = None,
    ) -> ExecuteCallback:
        """Create an ExecuteCallback workload from a Callback ORM model."""
        if not bundle_info:
            bundle_info = BundleInfo(
                name=dag_run.dag_model.bundle_name,
                version=dag_run.bundle_version,
            )
        fname = f"executor_callbacks/{callback.id}"  # TODO: better log file template

        return cls(
            callback=CallbackDTO.model_validate(callback, from_attributes=True),
            dag_rel_path=dag_rel_path or Path(dag_run.dag_model.relative_fileloc or ""),
            token=cls.generate_token(str(callback.id), generator),
            log_path=fname,
            bundle_info=bundle_info,
        )


def execute_callback_workload(
    callback: CallbackDTO,
    log,
) -> tuple[bool, str | None]:
    """
    Execute a callback function by importing and calling it, returning the success state.

    Supports two patterns:
    1. Functions - called directly with kwargs
    2. Classes that return callable instances (like BaseNotifier) - instantiated then called with context

    Example:
        # Function callback
        callback.data = {"path": "my_module.alert_func", "kwargs": {"msg": "Alert!", "context": {...}}}
        execute_callback_workload(callback, log)  # Calls alert_func(msg="Alert!", context={...})

        # Notifier callback
        callback.data = {"path": "airflow.providers.slack...SlackWebhookNotifier", "kwargs": {"text": "Alert!", "context": {...}}}
        execute_callback_workload(callback, log)  # SlackWebhookNotifier(text=..., context=...) then calls instance(context)

    :param callback: The Callback schema containing path and kwargs
    :param log: Logger instance for recording execution
    :return: Tuple of (success: bool, error_message: str | None)
    """
    callback_path = callback.data.get("path")
    callback_kwargs = callback.data.get("kwargs", {})

    if not callback_path:
        return False, "Callback path not found in data."

    try:
        # Import the callback callable
        # Expected format: "module.path.to.function_or_class"
        module_path, function_name = callback_path.rsplit(".", 1)
        module = import_module(module_path)
        callback_callable = getattr(module, function_name)

        log.debug("Executing callback %s(%s)...", callback_path, callback_kwargs)

        # If the callback is a callable, call it.  If it is a class, instantiate it.
        result = callback_callable(**callback_kwargs)

        # If the callback is a class then it is now instantiated and callable, call it.
        if callable(result):
            context = callback_kwargs.get("context", {})
            log.debug("Calling result with context for %s", callback_path)
            result = result(context)

        log.info("Callback %s executed successfully.", callback_path)
        return True, None

    except Exception as e:
        error_msg = f"Callback execution failed: {type(e).__name__}: {str(e)}"
        log.exception("Callback %s(%s) execution failed: %s", callback_path, callback_kwargs, error_msg)
        return False, error_msg
