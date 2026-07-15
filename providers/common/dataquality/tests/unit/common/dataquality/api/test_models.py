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

from airflow.providers.common.dataquality.api.models import (
    PaginatedResponse,
    RuleHistoryRecordModel,
    TaskDQRunModel,
)
from airflow.providers.common.dataquality.backends.object_storage import ObjectStorageResultsBackend
from airflow.providers.common.dataquality.results import DQRun, RuleResult


def make_backend(tmp_path) -> ObjectStorageResultsBackend:
    return ObjectStorageResultsBackend(results_path=f"file://{tmp_path}")


class TestTaskDQRunModel:
    def test_validates_backend_run_payload(self, tmp_path):
        backend = make_backend(tmp_path)
        dq_run = DQRun(
            dag_id="orders_pipeline",
            task_id="dq",
            run_id="manual__2026-07-04",
            started_at="2026-07-04T00:00:00+00:00",
        )
        backend.write_run(dq_run, [RuleResult(rule_uid="r1", rule_name="nulls", status="pass")])

        payload = backend.read_by_task_instance("orders_pipeline", "dq", "manual__2026-07-04")
        model = TaskDQRunModel(**payload)

        assert model.run.dag_id == "orders_pipeline"
        assert model.results[0].rule_name == "nulls"
        assert model.summary.passed == 1


class TestPaginatedResponseOfTaskDQRunModel:
    def test_validates_backend_task_runs_page(self, tmp_path):
        backend = make_backend(tmp_path)
        for day in range(1, 3):
            backend.write_run(
                DQRun(
                    dag_id="orders_pipeline",
                    task_id="dq",
                    run_id=f"manual__2026-07-0{day}",
                    run_uid=f"run{day}",
                    started_at=f"2026-07-0{day}T00:00:00+00:00",
                ),
                [RuleResult(rule_uid="r1", rule_name="nulls", status="pass")],
            )

        page = backend.read_task_runs("orders_pipeline", "dq")
        model = PaginatedResponse[TaskDQRunModel](**page)

        assert [item.run.run_uid for item in model.items] == ["run2", "run1"]
        assert model.next_cursor is None


class TestPaginatedResponseOfRuleHistoryRecordModel:
    def test_validates_backend_rule_history_page(self, tmp_path):
        backend = make_backend(tmp_path)
        backend.write_run(
            DQRun(
                dag_id="orders_pipeline",
                task_id="dq",
                run_id="manual__2026-07-04",
                started_at="2026-07-04T00:00:00+00:00",
            ),
            [RuleResult(rule_uid="r1", rule_name="nulls", status="fail")],
        )

        page = backend.read_task_rule_history("orders_pipeline", "dq", "r1")
        model = PaginatedResponse[RuleHistoryRecordModel](**page)

        assert model.items[0].rule_name == "nulls"
        assert model.items[0].run.task_id == "dq"
        assert model.next_cursor is None
