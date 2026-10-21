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

import inspect

import pytest

from airflow.sdk import task_handler
from airflow.sdk.definitions.task_handler import task_handler_registry


@pytest.fixture(autouse=True)
def clear_handlers():
    task_handler_registry.clear()
    yield
    task_handler_registry.clear()


def test_task_handler_registers_function_and_signature():
    @task_handler(dag_id="dag", task_id="task")
    def handler(value: int) -> str:
        return str(value)

    assert task_handler_registry.get("dag", "task") is handler
    assert task_handler_registry.describe("dag", "task") == inspect.signature(handler)


def test_load_from_path_imports_decorated_handlers(tmp_path):
    (tmp_path / "handlers.py").write_text(
        "from airflow.sdk import task_handler\n"
        "@task_handler(dag_id='dag', task_id='task')\n"
        "def handler(value):\n"
        "    return value\n"
    )

    task_handler_registry.load_from_path(tmp_path)

    assert task_handler_registry.get("dag", "task") is not None


def test_load_from_path_reloads_a_changed_bundle(tmp_path):
    handlers_file = tmp_path / "handlers.py"
    handlers_file.write_text(
        "from airflow.sdk import task_handler\n"
        "@task_handler(dag_id='dag', task_id='task')\n"
        "def handler():\n"
        "    return 1\n"
    )
    task_handler_registry.load_from_path(tmp_path, bundle_name="python-handlers")
    handlers_file.write_text(
        "from airflow.sdk import task_handler\n"
        "@task_handler(dag_id='dag', task_id='task')\n"
        "def handler():\n"
        "    return 22\n"
    )

    task_handler_registry.load_from_path(tmp_path, bundle_name="python-handlers")

    assert task_handler_registry.get("dag", "task")() == 22


def test_task_handler_rejects_duplicate_registration():
    @task_handler(dag_id="dag", task_id="task")
    def first():
        return None

    with pytest.raises(ValueError, match="already registered"):

        @task_handler(dag_id="dag", task_id="task")
        def second():
            return None

    assert task_handler_registry.get("dag", "task") is first


@pytest.mark.parametrize(("dag_id", "task_id"), [("", "task"), ("dag", "")])
def test_task_handler_requires_non_empty_ids(dag_id, task_id):
    with pytest.raises(ValueError, match="non-empty strings"):
        task_handler(dag_id=dag_id, task_id=task_id)
