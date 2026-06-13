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
"""Pin bidirectional migration of the lang-SDK `lineage` field (#67111)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from airflow.sdk.execution_time.comms import SucceedTask, TaskState
from airflow.sdk.execution_time.schema import get_schema_version_migrator


@pytest.fixture(scope="module")
def migrator():
    return get_schema_version_migrator()


class TestLineageUpgradeFromOldClient:
    def test_succeed_task_without_lineage_upgrades_to_head_with_none(self, migrator):
        body = {"type": "SucceedTask", "state": "success", "end_date": "2026-06-03T12:00:00+00:00"}
        assert migrator.upgrade(body, SucceedTask, "2026-06-16")["lineage"] is None

    def test_task_state_without_lineage_upgrades_to_head_with_none(self, migrator):
        body = {"type": "TaskState", "state": "failed", "end_date": "2026-06-03T12:00:00+00:00"}
        assert migrator.upgrade(body, TaskState, "2026-06-16")["lineage"] is None


class TestLineageUpgradeFromNewClient:
    def test_succeed_task_with_lineage_upgrades_verbatim(self, migrator):
        body = {
            "type": "SucceedTask",
            "state": "success",
            "end_date": "2026-06-03T12:00:00+00:00",
            "lineage": {"producer": "java-sdk", "rows": 42},
        }
        assert migrator.upgrade(body, SucceedTask, "2026-06-30")["lineage"] == {
            "producer": "java-sdk",
            "rows": 42,
        }

    def test_task_state_with_lineage_upgrades_verbatim(self, migrator):
        body = {
            "type": "TaskState",
            "state": "failed",
            "end_date": "2026-06-03T12:00:00+00:00",
            "lineage": {"error": "boom"},
        }
        assert migrator.upgrade(body, TaskState, "2026-06-30")["lineage"] == {"error": "boom"}


class TestLineageDowngradeToOldClient:
    def test_succeed_task_downgrade_strips_lineage(self, migrator):
        msg = SucceedTask(
            end_date=datetime(2026, 6, 3, 12, 0, tzinfo=timezone.utc),
            lineage={"producer": "java-sdk"},
        )
        assert "lineage" not in migrator.downgrade(msg, "2026-06-16").model_dump()

    def test_task_state_downgrade_strips_lineage(self, migrator):
        msg = TaskState(
            state="failed",
            end_date=datetime(2026, 6, 3, 12, 0, tzinfo=timezone.utc),
            lineage={"error": "boom"},
        )
        assert "lineage" not in migrator.downgrade(msg, "2026-06-16").model_dump()
