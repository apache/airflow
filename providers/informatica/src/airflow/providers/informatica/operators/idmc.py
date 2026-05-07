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
"""Operators for triggering Informatica IDMC CDI runs from Airflow."""

from __future__ import annotations

import time
from collections.abc import Mapping
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator, conf
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


_DEFAULT_TIMEOUT = 60 * 60 * 24 * 7  # one week, matches dbt cloud default


class _BaseInformaticaIDMCRunOperator(BaseOperator):
    """
    Common implementation for IDMC run operators (CDI task / taskflow).

    Concrete subclasses implement :meth:`_start_run` and provide the trigger
    class via :attr:`_trigger_class` plus the matching sync status method
    name :attr:`_status_method_name`.
    """

    _trigger_class: type[BaseTrigger]
    _status_method_name: str

    template_fields: tuple[str, ...] = (
        "informatica_idmc_conn_id",
        "auth_version",
    )

    def __init__(
        self,
        *,
        informatica_idmc_conn_id: str = InformaticaIDMCHook.default_conn_name,
        auth_version: str | None = None,
        wait_for_completion: bool = True,
        timeout: int = _DEFAULT_TIMEOUT,
        check_interval: int = 30,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        hook_params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.informatica_idmc_conn_id = informatica_idmc_conn_id
        self.auth_version = auth_version
        self.wait_for_completion = wait_for_completion
        self.timeout = timeout
        self.check_interval = check_interval
        self.deferrable = deferrable
        self.hook_params = hook_params or {}
        self.run_id: str | None = None

    @cached_property
    def hook(self) -> InformaticaIDMCHook:
        """Return a cached :class:`InformaticaIDMCHook` for this operator."""
        return InformaticaIDMCHook(
            informatica_idmc_conn_id=self.informatica_idmc_conn_id,
            auth_version=self.auth_version,
            **self.hook_params,
        )

    def _start_run(self, context: Context) -> dict[str, Any]:  # pragma: no cover - abstract
        raise NotImplementedError

    def _get_run_status(self, run_id: str) -> dict[str, Any]:
        method = getattr(self.hook, self._status_method_name)
        return method(run_id)

    def _wait_sync(self) -> str:
        deadline = time.time() + self.timeout if self.timeout else None
        while True:
            info = self._get_run_status(self.run_id)  # type: ignore[arg-type]
            status = info.get("status", IDMCRunStatus.RUNNING.value)
            if IDMCRunStatus.is_terminal(status):
                return status
            if deadline is not None and time.time() > deadline:
                raise InformaticaIDMCError(f"IDMC run {self.run_id} did not finish within {self.timeout}s.")
            time.sleep(self.check_interval)

    def _handle_terminal_status(self, status: str) -> str:
        if IDMCRunStatus.is_successful(status):
            self.log.info("IDMC run %s finished with status %s.", self.run_id, status)
            return status
        if status == IDMCRunStatus.CANCELLED.value:
            raise InformaticaIDMCError(f"IDMC run {self.run_id} was cancelled.")
        raise InformaticaIDMCError(f"IDMC run {self.run_id} failed with status {status}.")

    def execute(self, context: Context) -> str:
        result = self._start_run(context)
        self.run_id = str(result["run_id"])
        context["ti"].xcom_push(key="idmc_run_id", value=self.run_id)
        self.log.info("Started IDMC run %s.", self.run_id)

        if not self.wait_for_completion:
            return self.run_id

        if not self.deferrable:
            status = self._wait_sync()
            self._handle_terminal_status(status)
            return self.run_id

        # Deferrable path: peek once to avoid a round trip through the
        # Triggerer for runs that finished synchronously.
        info = self._get_run_status(self.run_id)
        status = info.get("status", IDMCRunStatus.RUNNING.value)
        if IDMCRunStatus.is_terminal(status):
            self._handle_terminal_status(status)
            return self.run_id

        end_time = time.time() + self.timeout
        self.defer(
            timeout=None,
            trigger=self._trigger_class(
                conn_id=self.informatica_idmc_conn_id,
                run_id=self.run_id,
                end_time=end_time,
                poll_interval=self.check_interval,
                auth_version=self.auth_version,
                hook_params=self.hook_params,
            ),
            method_name="execute_complete",
        )
        # ``self.defer`` raises TaskDeferred and never returns, but mypy
        # cannot see that — keep a return for type-checkers.
        return self.run_id  # pragma: no cover

    def execute_complete(self, context: Context, event: dict[str, Any]) -> str:
        run_id = str(event.get("run_id", self.run_id))
        self.run_id = run_id
        status = event.get("status", "")
        if status == "timeout":
            raise IDMCTimeoutException(event.get("message") or f"IDMC run {run_id} timed out.")
        if status == "error":
            raise InformaticaIDMCError(event.get("message") or f"IDMC run {run_id} errored.")
        return self._handle_terminal_status(status)

    def on_kill(self) -> None:
        if not self.run_id:
            return
        try:
            self.hook.cancel_task(self.run_id)
        except InformaticaIDMCError as exc:
            self.log.warning(
                "Best-effort cancel of IDMC run %s failed during task kill: %s",
                self.run_id,
                exc,
            )


class InformaticaIDMCRunTaskOperator(_BaseInformaticaIDMCRunOperator):
    """
    Run an Informatica IDMC CDI task (mapping / sync / replication / etc.).

    Either ``idmc_task_id`` (the IDMC internal id) or ``task_federated_id``
    must be provided.  When ``deferrable=True`` and ``wait_for_completion=True``
    the operator submits the run, then defers polling to the Triggerer until
    the run reaches a terminal state.

    Note that ``idmc_task_id`` is the IDMC-side task identifier; the Airflow
    DAG-level task id is still passed via the standard ``task_id`` operator
    argument inherited from :class:`BaseOperator`.

    :param idmc_task_id: IDMC internal task id.
    :param task_federated_id: Federated (organization-scoped) task id.  Use
        when the same task lives in multiple sub-orgs.
    :param idmc_task_type: One of ``MTT`` (default, mapping task), ``DSS``,
        ``DRS``, ``DMASK``, ``PCS``, ``RTM``, ``WORKFLOW``, or ``TASKFLOW``.
        Long-form labels (``"Mapping Task"``) are also accepted.
    :param callback_url: Optional callback URL invoked by IDMC when the run
        finishes.
    """

    _trigger_class = InformaticaIDMCTaskRunTrigger
    _status_method_name = "get_task_run_status"

    template_fields = _BaseInformaticaIDMCRunOperator.template_fields + (
        "idmc_task_id",
        "task_federated_id",
        "idmc_task_type",
        "callback_url",
    )

    def __init__(
        self,
        *,
        idmc_task_id: str | None = None,
        task_federated_id: str | None = None,
        idmc_task_type: str = "MTT",
        callback_url: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if not idmc_task_id and not task_federated_id:
            raise ValueError(
                "InformaticaIDMCRunTaskOperator requires either idmc_task_id or task_federated_id."
            )
        self.idmc_task_id = idmc_task_id
        self.task_federated_id = task_federated_id
        self.idmc_task_type = idmc_task_type
        self.callback_url = callback_url

    def _start_run(self, context: Context) -> dict[str, Any]:
        return self.hook.start_task(
            task_id=self.idmc_task_id,
            task_federated_id=self.task_federated_id,
            task_type=self.idmc_task_type,
            callback_url=self.callback_url,
        )


class InformaticaIDMCRunTaskflowOperator(_BaseInformaticaIDMCRunOperator):
    """
    Run an Informatica IDMC CDI taskflow by its REST API name.

    :param taskflow_api_name: The "API Name" property of the taskflow.
    :param input_parameters: Optional mapping of input parameters; passed
        through to IDMC as ``inputs`` in the request body.
    :param callback_url: Optional callback URL invoked by IDMC when the
        taskflow finishes.
    """

    _trigger_class = InformaticaIDMCTaskflowRunTrigger
    _status_method_name = "get_taskflow_run_status"

    template_fields = _BaseInformaticaIDMCRunOperator.template_fields + (
        "taskflow_api_name",
        "input_parameters",
        "callback_url",
    )

    def __init__(
        self,
        *,
        taskflow_api_name: str,
        input_parameters: Mapping[str, Any] | None = None,
        callback_url: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.taskflow_api_name = taskflow_api_name
        self.input_parameters = dict(input_parameters) if input_parameters else None
        self.callback_url = callback_url

    def _start_run(self, context: Context) -> dict[str, Any]:
        return self.hook.start_taskflow(
            self.taskflow_api_name,
            input_parameters=self.input_parameters,
            callback_url=self.callback_url,
        )
