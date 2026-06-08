"""Airflow listener that captures log tails on task terminal states."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from airflow.listeners import hookimpl

from dag_triage.config import capture_on_success, is_triage_enabled
from dag_triage.log_tail import empty_log_tail_result, fetch_log_tail
from dag_triage.log_tail_store import get_log_tail_store

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
    from airflow.utils.state import TaskInstanceState

log = logging.getLogger(__name__)

_listener: DagTriageListener | None = None


class DagTriageListener:
    """Capture task log tails when tasks reach terminal states."""

    def _capture_log_tail(
        self,
        task_instance: RuntimeTaskInstance | TaskInstance,
        *,
        hook_name: str,
    ) -> None:
        if not is_triage_enabled():
            return

        try:
            result = fetch_log_tail(task_instance)
        except Exception as exc:
            log.exception(
                "Unexpected error fetching log tail in %s for %s.%s",
                hook_name,
                task_instance.dag_id,
                task_instance.task_id,
            )
            result = empty_log_tail_result(task_instance, error=f"Unexpected error: {exc}")

        get_log_tail_store().put(task_instance, result)

        if result.error:
            log.warning(
                "dag_triage captured partial or empty log tail in %s for %s.%s: %s",
                hook_name,
                task_instance.dag_id,
                task_instance.task_id,
                result.error,
            )
        else:
            log.debug(
                "dag_triage captured %s log lines in %s for %s.%s",
                result.line_count,
                hook_name,
                task_instance.dag_id,
                task_instance.task_id,
            )

    @hookimpl
    def on_task_instance_failed(
        self,
        previous_state: TaskInstanceState | None,
        task_instance: RuntimeTaskInstance | TaskInstance,
        error: None | str | BaseException,
    ) -> None:
        self._capture_log_tail(task_instance, hook_name="on_task_instance_failed")

    @hookimpl
    def on_task_instance_skipped(
        self,
        previous_state: TaskInstanceState | None,
        task_instance: RuntimeTaskInstance | TaskInstance,
    ) -> None:
        self._capture_log_tail(task_instance, hook_name="on_task_instance_skipped")

    @hookimpl
    def on_task_instance_success(
        self,
        previous_state: TaskInstanceState | None,
        task_instance: RuntimeTaskInstance | TaskInstance,
    ) -> None:
        if not capture_on_success():
            return
        self._capture_log_tail(task_instance, hook_name="on_task_instance_success")


def get_dag_triage_listener() -> DagTriageListener:
    global _listener
    if _listener is None:
        _listener = DagTriageListener()
    return _listener
