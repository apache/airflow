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

from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import TypeAdapter

from airflow.executors.workloads import BundleInfo, ExecuteTask
from airflow.providers.edge3.models.types import ExecuteTypeBody, is_callback_execute

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if AIRFLOW_V_3_3_PLUS:
    from airflow.executors.workloads import CallbackFetchMethod, ExecuteCallback, TaskInstanceDTO
    from airflow.executors.workloads.callback import CallbackDTO


def _make_execute_task() -> ExecuteTask:
    ti = TaskInstanceDTO(
        id=uuid4(),
        dag_version_id=uuid4(),
        task_id="test_task",
        dag_id="test_dag",
        run_id="test_run",
        try_number=1,
        map_index=-1,
        pool_slots=1,
        queue="default",
        priority_weight=1,
    )
    return ExecuteTask(
        ti=ti,
        dag_rel_path=Path("test_dag.py"),
        token="test_token",
        bundle_info=BundleInfo(name="test_bundle", version="1.0"),
        log_path="test.log",
    )


def _make_execute_callback() -> ExecuteCallback:
    callback_data = CallbackDTO(
        id=str(uuid4()),
        fetch_method=CallbackFetchMethod.IMPORT_PATH,
        data={
            "path": "builtins.dict",
            "kwargs": {"a": 1, "b": 2, "c": 3},
        },
    )
    return ExecuteCallback(
        callback=callback_data,
        dag_rel_path=Path("test.py"),
        bundle_info=BundleInfo(name="test_bundle", version="1.0"),
        token="test_token",
        log_path="test.log",
    )


@pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="ExecuteTypeBody union requires Airflow 3.3+")
class TestIsCallbackExecute:
    def test_returns_false_for_execute_task(self):
        workload = _make_execute_task()
        assert is_callback_execute(workload) is False

    def test_returns_true_for_execute_callback(self):
        workload = _make_execute_callback()
        assert is_callback_execute(workload) is True


@pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="ExecuteTypeBody union requires Airflow 3.3+")
class TestExecuteTypeBody:
    def setup_method(self):
        self.adapter: TypeAdapter = TypeAdapter(ExecuteTypeBody)

    def test_validate_execute_task_json(self):
        workload = _make_execute_task()
        json_str = workload.model_dump_json()

        result = self.adapter.validate_json(json_str)

        assert isinstance(result, ExecuteTask)
        assert result.ti.dag_id == "test_dag"

    def test_validate_execute_callback_json(self):
        workload = _make_execute_callback()
        json_str = workload.model_dump_json()

        result = self.adapter.validate_json(json_str)

        assert isinstance(result, ExecuteCallback)
        assert result.callback.fetch_method == CallbackFetchMethod.IMPORT_PATH

    def test_validate_execute_task_dict(self):
        workload = _make_execute_task()
        data = workload.model_dump()

        result = self.adapter.validate_python(data)

        assert isinstance(result, ExecuteTask)

    def test_validate_execute_callback_dict(self):
        workload = _make_execute_callback()
        data = workload.model_dump()

        result = self.adapter.validate_python(data)

        assert isinstance(result, ExecuteCallback)

    def test_roundtrip_execute_task(self):
        original = _make_execute_task()
        json_str = self.adapter.dump_json(original)
        restored = self.adapter.validate_json(json_str)

        assert isinstance(restored, ExecuteTask)
        assert restored.ti.task_id == original.ti.task_id
        assert restored.ti.dag_id == original.ti.dag_id

    def test_roundtrip_execute_callback(self):
        original = _make_execute_callback()
        json_str = self.adapter.dump_json(original)
        restored = self.adapter.validate_json(json_str)

        assert isinstance(restored, ExecuteCallback)
        assert restored.callback.id == original.callback.id
