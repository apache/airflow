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

import json
from typing import Any

import pytest

from airflow.providers.dataquality.backends import get_backend_from_config
from airflow.providers.dataquality.backends.object_storage import ObjectStorageResultsBackend
from airflow.providers.dataquality.results import DQRun, RuleResult
from airflow.providers.dataquality.rules import Severity

from tests_common.test_utils.config import conf_vars


def make_run(run_uid: str = "abc123", started_at: str = "2026-07-04T06:00:00+00:00") -> DQRun:
    return DQRun(
        dag_id="orders_pipeline",
        task_id="dq",
        run_id="scheduled__2026-07-04",
        run_uid=run_uid,
        ruleset_name="orders",
        table_ref="analytics.orders",
        asset_names=("dq_example_orders",),
        started_at=started_at,
        finished_at="2026-07-04T06:00:03+00:00",
    )


def make_result(rule_uid: str = "rule-1", status: str = "pass") -> RuleResult:
    return RuleResult(
        rule_uid=rule_uid,
        rule_name="ids_not_null",
        status=status,
        observed_value=0,
        condition={"equal_to": 0},
        dimension="completeness",
        severity=Severity.ERROR,
        duration_ms=12.5,
    )


class TestObjectStorageResultsBackend:
    @pytest.fixture
    def backend(self, tmp_path):
        return ObjectStorageResultsBackend(results_path=f"file://{tmp_path}")

    def test_write_run_creates_run_file_with_run_and_results(self, backend):
        backend.write_run(make_run(), [make_result()])

        record = backend.read_by_task_instance("orders_pipeline", "dq", "scheduled__2026-07-04")
        assert record["run"]["run_uid"] == "abc123"
        assert record["run"]["dag_id"] == "orders_pipeline"
        assert record["results"][0]["rule_uid"] == "rule-1"
        assert record["results"][0]["status"] == "pass"

    def test_write_run_stores_keyed_json_payload(self, backend):
        backend.write_run(make_run(), [make_result()])

        path = (
            backend.root
            / "runs"
            / "by_task"
            / "dag_id=orders_pipeline"
            / "task_id=dq"
            / "date=2026-07-04"
            / "abc123.json"
        )
        payload = json.loads(path.read_text())

        assert payload["run"]["run_uid"] == "abc123"
        assert payload["results"][0]["rule_uid"] == "rule-1"
        assert payload["summary"]["passed"] == 1

    def test_write_run_stores_task_rule_index(self, backend):
        backend.write_run(make_run(), [make_result()])

        path = (
            backend.root
            / "rules"
            / "by_task_rule"
            / "dag_id=orders_pipeline"
            / "task_id=dq"
            / "rule_uid=rule-1"
            / "2026-07-04T06_00_00_00_00__abc123.json"
        )
        payload = json.loads(path.read_text())

        assert payload["run"]["dag_id"] == "orders_pipeline"
        assert payload["run"]["task_id"] == "dq"
        assert payload["result"]["rule_uid"] == "rule-1"

    def test_write_run_does_not_store_global_rule_index(self, backend):
        backend.write_run(make_run(), [make_result()])

        path = backend.root / "rules" / "by_rule" / "rule_uid=rule-1"

        assert not path.exists()

    def test_rule_history_is_newest_first(self, backend):
        backend.write_run(
            make_run(run_uid="run1", started_at="2026-07-01T06:00:00+00:00"),
            [make_result(status="pass")],
        )
        backend.write_run(
            make_run(run_uid="run2", started_at="2026-07-02T06:00:00+00:00"),
            [make_result(status="fail")],
        )

        result = backend.read_task_rule_history("orders_pipeline", "dq", "rule-1")

        assert [record["status"] for record in result["items"]] == ["fail", "pass"]
        assert result["items"][0]["run"]["run_uid"] == "run2"
        assert result["next_cursor"] is None

    def test_rule_history_includes_task_instance_route_context(self, backend):
        backend.write_run(make_run(), [make_result()])

        history = backend.read_task_rule_history("orders_pipeline", "dq", "rule-1")["items"]

        assert history[0]["run"]["dag_id"] == "orders_pipeline"
        assert history[0]["run"]["task_id"] == "dq"
        assert history[0]["run"]["run_id"] == "scheduled__2026-07-04"
        assert history[0]["run"]["map_index"] == -1

    def test_rule_history_respects_limit(self, backend):
        for day in range(1, 4):
            backend.write_run(
                make_run(run_uid=f"run{day}", started_at=f"2026-07-0{day}T06:00:00+00:00"),
                [make_result()],
            )

        result = backend.read_task_rule_history("orders_pipeline", "dq", "rule-1", limit=2)

        assert len(result["items"]) == 2
        assert result["next_cursor"] == "2026-07-02T06:00:00+00:00|run2"

    def test_rule_history_before_cursor_pages_further_back(self, backend):
        for day in range(1, 4):
            backend.write_run(
                make_run(run_uid=f"run{day}", started_at=f"2026-07-0{day}T06:00:00+00:00"),
                [make_result()],
            )

        first_page = backend.read_task_rule_history("orders_pipeline", "dq", "rule-1", limit=2)
        second_page = backend.read_task_rule_history(
            "orders_pipeline", "dq", "rule-1", limit=2, before=first_page["next_cursor"]
        )

        assert [r["run"]["run_uid"] for r in first_page["items"]] == ["run3", "run2"]
        assert [r["run"]["run_uid"] for r in second_page["items"]] == ["run1"]
        assert second_page["next_cursor"] is None

    def test_concurrent_style_writes_do_not_collide(self, backend):
        backend.write_run(make_run(run_uid="run1"), [make_result()])
        backend.write_run(make_run(run_uid="run2"), [make_result()])

        runs = backend.read_task_runs("orders_pipeline", "dq")["items"]
        assert {run["run"]["run_uid"] for run in runs} == {"run1", "run2"}

    def test_read_by_task_instance_matches_run(self, backend):
        backend.write_run(make_run(), [make_result()])

        record = backend.read_by_task_instance("orders_pipeline", "dq", "scheduled__2026-07-04")

        assert record["run"]["run_uid"] == "abc123"
        assert record["results"][0]["rule_uid"] == "rule-1"
        assert record["summary"]["passed"] == 1

    def test_read_by_task_instance_unknown_raises(self, backend):
        with pytest.raises(FileNotFoundError):
            backend.read_by_task_instance("orders_pipeline", "dq", "no_such_run")

    def test_read_by_task_instance_last_write_wins_across_retries(self, backend):
        first = make_run(run_uid="try1")
        backend.write_run(first, [make_result(status="fail")])
        second = make_run(run_uid="try2")
        backend.write_run(second, [make_result(status="pass")])

        record = backend.read_by_task_instance("orders_pipeline", "dq", "scheduled__2026-07-04")

        assert record["run"]["run_uid"] == "try2"
        assert record["results"][0]["status"] == "pass"
        assert record["summary"]["passed"] == 1

    def test_read_by_task_instance_sanitizes_run_id(self, backend):
        run_id = "manual__2026-07-04T06:00:00+00:00"
        run = DQRun(dag_id="orders_pipeline", task_id="dq", run_id=run_id, run_uid="abc123")
        backend.write_run(run, [make_result()])

        record = backend.read_by_task_instance("orders_pipeline", "dq", run_id)

        assert record["run"]["run_id"] == run_id

    def test_read_task_runs_returns_newest_first_with_summaries(self, backend):
        backend.write_run(
            make_run(run_uid="run1", started_at="2026-07-01T06:00:00+00:00"),
            [make_result(status="pass")],
        )
        backend.write_run(
            make_run(run_uid="run2", started_at="2026-07-02T06:00:00+00:00"),
            [make_result(status="fail")],
        )

        result = backend.read_task_runs("orders_pipeline", "dq")
        runs = result["items"]

        assert [record["run"]["run_uid"] for record in runs] == ["run2", "run1"]
        assert runs[0]["summary"]["failed"] == 1
        assert runs[1]["summary"]["passed"] == 1
        assert result["next_cursor"] is None

    def test_read_task_runs_respects_limit(self, backend):
        for day in range(1, 4):
            backend.write_run(
                make_run(run_uid=f"run{day}", started_at=f"2026-07-0{day}T06:00:00+00:00"),
                [make_result()],
            )

        result = backend.read_task_runs("orders_pipeline", "dq", limit=2)

        assert len(result["items"]) == 2
        assert result["next_cursor"] == "2026-07-02T06:00:00+00:00|run2"

    def test_read_task_runs_before_cursor_pages_further_back(self, backend):
        for day in range(1, 4):
            backend.write_run(
                make_run(run_uid=f"run{day}", started_at=f"2026-07-0{day}T06:00:00+00:00"),
                [make_result()],
            )

        first_page = backend.read_task_runs("orders_pipeline", "dq", limit=2)
        second_page = backend.read_task_runs(
            "orders_pipeline", "dq", limit=2, before=first_page["next_cursor"]
        )

        assert [r["run"]["run_uid"] for r in first_page["items"]] == ["run3", "run2"]
        assert [r["run"]["run_uid"] for r in second_page["items"]] == ["run1"]
        assert second_page["next_cursor"] is None

    def test_read_task_runs_cursor_keeps_same_timestamp_records(self, backend):
        for run_uid in ("run1", "run2", "run3"):
            backend.write_run(
                make_run(run_uid=run_uid, started_at="2026-07-04T06:00:00+00:00"),
                [make_result()],
            )

        first_page = backend.read_task_runs("orders_pipeline", "dq", limit=2)
        second_page = backend.read_task_runs(
            "orders_pipeline", "dq", limit=2, before=first_page["next_cursor"]
        )

        assert [r["run"]["run_uid"] for r in first_page["items"]] == ["run3", "run2"]
        assert [r["run"]["run_uid"] for r in second_page["items"]] == ["run1"]
        assert second_page["next_cursor"] is None

    def test_read_task_runs_stops_scanning_once_limit_is_reached(self, backend, monkeypatch):
        for day in range(1, 5):
            backend.write_run(
                make_run(run_uid=f"run{day}", started_at=f"2026-07-0{day}T06:00:00+00:00"),
                [make_result()],
            )

        read_paths: list[Any] = []
        original_read_json = backend._read_json
        monkeypatch.setattr(
            backend, "_read_json", lambda path: (read_paths.append(path), original_read_json(path))[1]
        )

        result = backend.read_task_runs("orders_pipeline", "dq", limit=1)

        assert [record["run"]["run_uid"] for record in result["items"]] == ["run4"]
        assert result["next_cursor"] == "2026-07-04T06:00:00+00:00|run4"
        # Determining next_cursor reads one entry past the limit (date=2026-07-03's run3), but
        # no further — date=2026-07-02/01 must not be read.
        assert len(read_paths) == 2

    def test_read_task_rule_history_filters_to_task(self, backend):
        backend.write_run(make_run(run_uid="run1"), [make_result()])
        backend.write_run(
            DQRun(
                dag_id="other_pipeline",
                task_id="dq",
                run_id="scheduled__2026-07-04",
                run_uid="run2",
                started_at="2026-07-04T07:00:00+00:00",
            ),
            [make_result()],
        )

        history = backend.read_task_rule_history("orders_pipeline", "dq", "rule-1")["items"]

        assert len(history) == 1
        assert history[0]["run"]["dag_id"] == "orders_pipeline"

    def test_read_task_rule_history_respects_limit(self, backend):
        for day in range(1, 4):
            backend.write_run(
                make_run(run_uid=f"run{day}", started_at=f"2026-07-0{day}T06:00:00+00:00"),
                [make_result()],
            )

        result = backend.read_task_rule_history("orders_pipeline", "dq", "rule-1", limit=2)

        assert [record["run"]["run_uid"] for record in result["items"]] == ["run3", "run2"]
        assert result["next_cursor"] == "2026-07-02T06:00:00+00:00|run2"

    def test_read_task_rule_history_before_cursor_pages_further_back(self, backend):
        for day in range(1, 4):
            backend.write_run(
                make_run(run_uid=f"run{day}", started_at=f"2026-07-0{day}T06:00:00+00:00"),
                [make_result()],
            )

        first_page = backend.read_task_rule_history("orders_pipeline", "dq", "rule-1", limit=2)
        second_page = backend.read_task_rule_history(
            "orders_pipeline", "dq", "rule-1", limit=2, before=first_page["next_cursor"]
        )

        assert [r["run"]["run_uid"] for r in second_page["items"]] == ["run1"]
        assert second_page["next_cursor"] is None

    def test_read_task_rule_history_cursor_keeps_same_timestamp_records(self, backend):
        for run_uid in ("run1", "run2", "run3"):
            backend.write_run(
                make_run(run_uid=run_uid, started_at="2026-07-04T06:00:00+00:00"),
                [make_result()],
            )

        first_page = backend.read_task_rule_history("orders_pipeline", "dq", "rule-1", limit=2)
        second_page = backend.read_task_rule_history(
            "orders_pipeline", "dq", "rule-1", limit=2, before=first_page["next_cursor"]
        )

        assert [r["run"]["run_uid"] for r in first_page["items"]] == ["run3", "run2"]
        assert [r["run"]["run_uid"] for r in second_page["items"]] == ["run1"]
        assert second_page["next_cursor"] is None


class TestGetBackendFromConfig:
    @conf_vars({("dataquality", "results_path"): None})
    def test_no_results_path_returns_none(self):
        assert get_backend_from_config() is None

    def test_results_path_builds_object_storage_backend(self, tmp_path):
        with conf_vars({("dataquality", "results_path"): f"file://{tmp_path}"}):
            backend = get_backend_from_config()
        assert isinstance(backend, ObjectStorageResultsBackend)
