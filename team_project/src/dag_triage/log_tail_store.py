"""In-memory cache of fetched log tails for listener and on-demand UI access."""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

from dag_triage.log_tail import LogTailResult, fetch_log_tail

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancehistory import TaskInstanceHistory

log = logging.getLogger(__name__)


def _cache_key(task_instance: TaskInstance | TaskInstanceHistory) -> str:
    return (
        f"{task_instance.dag_id}|{task_instance.task_id}|{task_instance.run_id}|"
        f"{task_instance.map_index}|{task_instance.try_number}"
    )


class LogTailStore:
    """Thread-safe store of ``LogTailResult`` objects keyed by task instance."""

    def __init__(self) -> None:
        self._results: dict[str, LogTailResult] = {}
        self._lock = threading.Lock()

    def put(self, task_instance: TaskInstance | TaskInstanceHistory, result: LogTailResult) -> None:
        with self._lock:
            self._results[_cache_key(task_instance)] = result

    def get(self, task_instance: TaskInstance | TaskInstanceHistory) -> LogTailResult | None:
        with self._lock:
            return self._results.get(_cache_key(task_instance))

    def get_or_fetch(
        self,
        task_instance: TaskInstance | TaskInstanceHistory,
        *,
        refresh: bool = False,
        tail_lines: int | None = None,
    ) -> LogTailResult:
        if not refresh:
            cached = self.get(task_instance)
            if cached is not None:
                return cached

        result = fetch_log_tail(task_instance, tail_lines=tail_lines)
        self.put(task_instance, result)
        return result


_store: LogTailStore | None = None
_store_lock = threading.Lock()


def get_log_tail_store() -> LogTailStore:
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = LogTailStore()
    return _store


def get_task_log_tail(
    task_instance: TaskInstance | TaskInstanceHistory,
    *,
    refresh: bool = False,
    tail_lines: int | None = None,
) -> LogTailResult:
    """Return a cached or freshly fetched log tail for on-demand UI access."""
    return get_log_tail_store().get_or_fetch(
        task_instance,
        refresh=refresh,
        tail_lines=tail_lines,
    )
