#
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
"""Sensors that wait for an Informatica IDMC CDI run to finish."""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseSensorOperator, conf
from airflow.providers.informatica.hooks.idmc import (
    IDMCRunStatus,
    IDMCTimeoutException,
    InformaticaIDMCError,
    InformaticaIDMCHook,
)
from airflow.providers.informatica.triggers.idmc import (
    InformaticaIDMCTaskflowRunTrigger,
    InformaticaIDMCTaskRunTrigger,
)

if TYPE_CHECKING:
    from airflow.sdk import Context
    from airflow.triggers.base import BaseTrigger


class _BaseInformaticaIDMCRunSensor(BaseSensorOperator):
    """Common implementation for the IDMC run sensors."""

    _trigger_class: type[BaseTrigger]
    _status_method_name: str

    template_fields: tuple[str, ...] = ("informatica_idmc_conn_id", "run_id", "auth_version")

    def __init__(
        self,
        *,
        run_id: str,
        informatica_idmc_conn_id: str = InformaticaIDMCHook.default_conn_name,
        auth_version: str | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        hook_params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        if deferrable and "poke_interval" not in kwargs:
            kwargs["poke_interval"] = 30
        if deferrable and "timeout" not in kwargs:
            kwargs["timeout"] = 60 * 60 * 24 * 7
        super().__init__(**kwargs)
        self.run_id = run_id
        self.informatica_idmc_conn_id = informatica_idmc_conn_id
        self.auth_version = auth_version
        self.deferrable = deferrable
        self.hook_params = hook_params or {}

    @cached_property
    def hook(self) -> InformaticaIDMCHook:
        """Return a cached :class:`InformaticaIDMCHook` for this sensor."""
        return InformaticaIDMCHook(
            informatica_idmc_conn_id=self.informatica_idmc_conn_id,
            auth_version=self.auth_version,
            **self.hook_params,
        )

    def _get_run_status(self, run_id: str) -> dict[str, Any]:
        method = getattr(self.hook, self._status_method_name)
        return method(run_id, **self._status_kwargs())

    def _status_kwargs(self) -> dict[str, Any]:
        return {}

    def poke(self, context: Context) -> bool:
        info = self._get_run_status(self.run_id)
        status = info.get("status", IDMCRunStatus.RUNNING.value)
        if status == IDMCRunStatus.FAILED.value:
            raise InformaticaIDMCError(f"IDMC run {self.run_id} failed.")
        if status == IDMCRunStatus.CANCELLED.value:
            raise InformaticaIDMCError(f"IDMC run {self.run_id} was cancelled.")
        return IDMCRunStatus.is_successful(status)

    def execute(self, context: Context) -> str:
        if not self.deferrable:
            super().execute(context)
            return self.run_id

        if self.poke(context):
            return self.run_id

        self.defer(
            timeout=self.timeout,
            trigger=self._trigger_class(
                conn_id=self.informatica_idmc_conn_id,
                run_id=self.run_id,
                timeout=self.timeout,
                poll_interval=self.poke_interval,
                auth_version=self.auth_version,
                hook_params=self.hook_params,
                **self._status_kwargs(),
            ),
            method_name="execute_complete",
        )
        return self.run_id  # pragma: no cover

    def execute_complete(self, context: Context, event: dict[str, Any]) -> str:
        status = event.get("status", "")
        run_id = str(event.get("run_id", self.run_id))
        if status in {"timeout"}:
            raise IDMCTimeoutException(event.get("message") or f"IDMC run {run_id} timed out.")
        if status == "error":
            raise InformaticaIDMCError(event.get("message") or f"IDMC run {run_id} errored.")
        if status == IDMCRunStatus.FAILED.value:
            raise InformaticaIDMCError(event.get("message") or f"IDMC run {run_id} failed.")
        if status == IDMCRunStatus.CANCELLED.value:
            raise InformaticaIDMCError(event.get("message") or f"IDMC run {run_id} was cancelled.")
        self.log.info(event.get("message") or f"IDMC run {run_id} finished with status {status}.")
        return run_id


class InformaticaIDMCTaskRunSensor(_BaseInformaticaIDMCRunSensor):
    """Wait for an Informatica IDMC CDI task run (mapping / sync / etc.) to terminate."""

    _trigger_class = InformaticaIDMCTaskRunTrigger
    _status_method_name = "get_task_run_status"
    template_fields = _BaseInformaticaIDMCRunSensor.template_fields + ("idmc_task_id",)

    def __init__(self, *, idmc_task_id: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.idmc_task_id = idmc_task_id

    def _status_kwargs(self) -> dict[str, Any]:
        return {"task_id": self.idmc_task_id}


class InformaticaIDMCTaskflowRunSensor(_BaseInformaticaIDMCRunSensor):
    """Wait for an Informatica IDMC CDI taskflow run to terminate."""

    _trigger_class = InformaticaIDMCTaskflowRunTrigger
    _status_method_name = "get_taskflow_run_status"
