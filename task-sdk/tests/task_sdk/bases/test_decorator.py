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

import ast
import importlib.util
import textwrap
from pathlib import Path

from airflow.sdk.bases.decorator import DecoratedOperator

RAW_CODE = """
from airflow.sdk import task

@task.kubernetes(
    namespace="airflow",
    image="python:3.12",
)
def a_task():
##################
    return "success A"
"""


class DummyK8sDecoratedOperator(DecoratedOperator):
    custom_operator_name = "@task.kubernetes"


class TestBaseDecorator:
    def test_get_python_source_strips_decorator_and_comment(self, tmp_path: Path):
        module_path = tmp_path / "tmp_mod.py"
        module_path.write_text(textwrap.dedent(RAW_CODE))

        spec = importlib.util.spec_from_file_location("tmp_mod", module_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        a_task_callable = module.a_task

        op = DummyK8sDecoratedOperator(task_id="t", python_callable=a_task_callable)
        cleaned = op.get_python_source()

        # Decorator & comment should be gone
        assert "@task.kubernetes" not in cleaned
        assert "##################" not in cleaned

        # Returned source must be valid Python
        ast.parse(cleaned)
        assert cleaned.lstrip().splitlines()[0].startswith("def a_task")
