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
Object-storage results backend.

Each DQ check writes a keyed JSON document plus read indexes optimized for the UI:

    runs/by_task/dag_id=<dag>/task_id=<task>/date=<2026-07-04>/<run_uid>.json
        Canonical run record: ``{"run": ..., "results": [...], "summary": ...}``.

    runs/by_task_instance/dag_id=<dag>/task_id=<task>/<safe_run_id>__<map_index>.json
        Latest result for a task-instance page. Last write wins across retries.

    rules/by_rule/rule_uid=<uid>/<started_at>__<run_uid>.json
        One rule result plus run context: ``{"run": ..., "result": ...}``.

    rules/by_task_rule/dag_id=<dag>/task_id=<task>/rule_uid=<uid>/<started_at>__<run_uid>.json
        Same payload, scoped for task-level rule history views.

The duplicate files are intentional read indexes: DQ tasks write once, while the UI reads
many times. Keeping these indexes avoids scanning all task runs for common UI views.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from airflow.providers.dq.results import DQRun, RuleResult, build_summary
from airflow.sdk import ObjectStoragePath

log = logging.getLogger(__name__)


class ObjectStorageResultsBackend:
    """Persist DQ results as JSON files via ``ObjectStoragePath``."""

    def __init__(self, results_path: str, conn_id: str | None = None) -> None:
        self.root = ObjectStoragePath(results_path, conn_id=conn_id)

    def write_run(self, run: DQRun, results: list[RuleResult]) -> None:
        timestamp = run.started_at or datetime.now(tz=timezone.utc).isoformat()
        payload = self._build_run_payload(run, results)

        self._write_run_file(run, timestamp[:10], payload)
        self._write_task_instance_index(run, payload)
        self._write_rule_indexes(run, results, timestamp)

    def read_task_rule_history(
        self, dag_id: str, task_id: str, rule_uid: str, limit: int = 100, before: str | None = None
    ) -> dict[str, Any]:
        """Return recent results for one rule produced by one task, newest first."""
        rule_dir = (
            self.root
            / "rules"
            / "by_task_rule"
            / f"dag_id={dag_id}"
            / f"task_id={task_id}"
            / f"rule_uid={rule_uid}"
        )
        return self._read_rule_history_dir(rule_dir, limit, before)

    def read_task_runs(
        self, dag_id: str, task_id: str, limit: int = 50, before: str | None = None
    ) -> dict[str, Any]:
        """
        Return recent data quality runs for one task, newest first.

        Returns ``{"items": [...], "next_cursor": ...}``. ``before`` is the opaque
        ``next_cursor`` from the previous page, so "load more" only reads runs it hasn't
        shown yet. ``next_cursor`` is set by reading one extra entry past ``limit``: cheap
        on every page except the last one, where there's no way to confirm history is
        exhausted without walking to the end.

        ``date=`` partition names sort correctly as plain strings, so directories are walked
        newest-first and scanning stops as soon as ``limit + 1`` matching runs have been
        collected — a task with years of history doesn't pay for a full scan on every page.
        """
        task_dir = self.root / "runs" / "by_task" / f"dag_id={dag_id}" / f"task_id={task_id}"
        if not task_dir.exists():
            return {"items": [], "next_cursor": None}

        date_dirs = sorted((path for path in task_dir.iterdir() if path.is_dir()), reverse=True)
        runs = []
        for date_dir in date_dirs:
            for path in date_dir.iterdir():
                if not path.name.endswith(".json"):
                    continue
                payload = self._read_json(path)
                if payload is None:
                    continue
                cursor = self._get_run_payload_cursor(payload)
                if before is not None and cursor >= before:
                    continue
                runs.append(payload)
            if len(runs) > limit:
                break

        ordered = sorted(runs, key=self._get_run_payload_cursor, reverse=True)
        page = ordered[:limit]
        next_cursor = self._get_run_payload_cursor(page[-1]) if len(ordered) > limit and page else None
        return {"items": page, "next_cursor": next_cursor}

    def read_by_task_instance(
        self, dag_id: str, task_id: str, run_id: str, map_index: int = -1
    ) -> dict[str, Any]:
        """Read the latest run for one task instance as ``{"run": ..., "results": ..., "summary": ...}``."""
        path = (
            self.root
            / "runs"
            / "by_task_instance"
            / f"dag_id={dag_id}"
            / f"task_id={task_id}"
            / f"{self._get_safe_key(run_id)}__{map_index}.json"
        )
        return self._read_json_or_raise(path)

    def _write_run_file(self, run: DQRun, date_part: str, payload: dict[str, Any]) -> None:
        run_dir = (
            self.root
            / "runs"
            / "by_task"
            / f"dag_id={run.dag_id}"
            / f"task_id={run.task_id}"
            / f"date={date_part}"
        )
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / f"{run.run_uid}.json").write_text(json.dumps(payload, default=str))

    def _write_task_instance_index(self, run: DQRun, payload: dict[str, Any]) -> None:
        ti_dir = self.root / "runs" / "by_task_instance" / f"dag_id={run.dag_id}" / f"task_id={run.task_id}"
        ti_dir.mkdir(parents=True, exist_ok=True)
        (ti_dir / f"{self._get_safe_key(run.run_id)}__{run.map_index}.json").write_text(
            json.dumps(payload, default=str)
        )

    def _write_rule_indexes(self, run: DQRun, results: list[RuleResult], timestamp: str) -> None:
        run_context = self._build_run_context(run)
        compact_ts = self._get_safe_key(timestamp)
        for result in results:
            payload = {"run": run_context, "result": result.to_dict()}
            self._write_rule_index(
                self.root / "rules" / "by_rule" / f"rule_uid={result.rule_uid}",
                compact_ts,
                run.run_uid,
                payload,
            )
            self._write_rule_index(
                self.root
                / "rules"
                / "by_task_rule"
                / f"dag_id={run.dag_id}"
                / f"task_id={run.task_id}"
                / f"rule_uid={result.rule_uid}",
                compact_ts,
                run.run_uid,
                payload,
            )

    def _write_rule_index(
        self, rule_dir: ObjectStoragePath, compact_ts: str, run_uid: str, payload: dict[str, Any]
    ) -> None:
        rule_dir.mkdir(parents=True, exist_ok=True)
        (rule_dir / f"{compact_ts}__{run_uid}.json").write_text(json.dumps(payload, default=str))

    def _read_rule_history_dir(
        self, rule_dir: ObjectStoragePath, limit: int, before: str | None = None
    ) -> dict[str, Any]:
        """
        Read rule-result records newest-first, as ``{"items": [...], "next_cursor": ...}``.

        Reads one entry past ``limit`` to determine ``next_cursor``: cheap on every page
        except the last one, where confirming there's nothing older means reading to the end.
        """
        if not rule_dir.exists():
            return {"items": [], "next_cursor": None}

        history: list[dict[str, Any]] = []
        for path in sorted(rule_dir.iterdir(), key=lambda p: p.name, reverse=True):
            payload = self._read_json(path)
            if payload is None:
                continue
            record = {**payload["result"], "run": payload["run"]}
            cursor = self._get_rule_history_cursor(record)
            if before is not None and cursor >= before:
                continue
            history.append(record)
            if len(history) > limit:
                break

        page = history[:limit]
        next_cursor = self._get_rule_history_cursor(page[-1]) if len(history) > limit and page else None
        return {"items": page, "next_cursor": next_cursor}

    @staticmethod
    def _get_safe_key(value: str) -> str:
        """Sanitize a value (for example an Airflow ``run_id``) for an object key segment."""
        return value.replace("/", "_").replace(":", "_").replace("+", "_")

    @staticmethod
    def _build_run_payload(run: DQRun, results: list[RuleResult]) -> dict[str, Any]:
        result_records = [result.to_dict() for result in results]
        return {
            "run": run.to_dict(),
            "results": result_records,
            "summary": build_summary(run, results),
        }

    @staticmethod
    def _build_run_context(run: DQRun) -> dict[str, Any]:
        return {
            "run_uid": run.run_uid,
            "dag_id": run.dag_id,
            "task_id": run.task_id,
            "run_id": run.run_id,
            "map_index": run.map_index,
            "started_at": run.started_at,
            "table_ref": run.table_ref,
        }

    @staticmethod
    def _get_run_payload_cursor(payload: dict[str, Any]) -> str:
        run = payload["run"]
        return ObjectStorageResultsBackend._build_cursor(run.get("started_at"), run.get("run_uid"))

    @staticmethod
    def _get_rule_history_cursor(record: dict[str, Any]) -> str:
        run = record["run"]
        return ObjectStorageResultsBackend._build_cursor(run.get("started_at"), run.get("run_uid"))

    @staticmethod
    def _build_cursor(started_at: str | None, run_uid: str | None) -> str:
        return f"{started_at or ''}|{run_uid or ''}"

    def _read_json_or_raise(self, path: ObjectStoragePath) -> dict[str, Any]:
        return json.loads(path.read_text())

    def _read_json(self, path: ObjectStoragePath) -> dict[str, Any] | None:
        try:
            return self._read_json_or_raise(path)
        except (OSError, json.JSONDecodeError):
            log.warning("Skipping unreadable DQ result file %s", path)
            return None
