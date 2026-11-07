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

from unittest import mock

import pytest
from jinja2.exceptions import UndefinedError

from airflow.providers.cncf.kubernetes.python_kubernetes_script import (
    remove_task_decorator,
    write_python_script,
)


class TestRemoveTaskDecorator:
    @pytest.mark.parametrize(
        ("source", "expected"),
        [
            pytest.param(
                "@task.kubernetes\ndef my_task():\n    pass\n",
                "def my_task():\n    pass\n",
                id="no-arguments",
            ),
            pytest.param(
                '@task.kubernetes(image="python:3.9")\ndef my_task():\n    pass\n',
                "def my_task():\n    pass\n",
                id="with-arguments",
            ),
            pytest.param(
                "@task.kubernetes(retries=get_retries(3))\ndef my_task():\n    pass\n",
                "def my_task():\n    pass\n",
                id="nested-parentheses",
            ),
        ],
    )
    def test_removes_task_decorator(self, source, expected):
        assert remove_task_decorator(source, "@task.kubernetes") == expected

    def test_removes_setup_and_teardown_alongside(self):
        source = (
            "@setup\ndef my_setup():\n    pass\n\n"
            "@task.kubernetes\ndef my_task():\n    pass\n\n"
            "@teardown\ndef my_teardown():\n    pass\n"
        )
        expected = "def my_setup():\n    pass\n\ndef my_task():\n    pass\n\ndef my_teardown():\n    pass\n"
        assert remove_task_decorator(source, "@task.kubernetes") == expected

    def test_returns_source_unchanged_when_decorator_missing(self):
        source = "def my_task():\n    pass\n"
        assert remove_task_decorator(source, "@task.kubernetes") == source


class TestWritePythonScript:
    @pytest.fixture
    def jinja_context(self):
        return {
            "pickling_library": "pickle",
            "python_callable_source": "def my_task():\n    return 42\n",
            "python_callable": "my_task",
            "op_args": [],
            "op_kwargs": {},
        }

    def test_renders_template_to_file(self, tmp_path, jinja_context):
        target = tmp_path / "script.py"
        write_python_script(jinja_context, str(target))

        rendered = target.read_text()
        assert "import pickle" in rendered
        assert "def my_task():\n    return 42\n" in rendered
        assert 'res = my_task(*arg_dict["args"], **arg_dict["kwargs"])' in rendered

    def test_native_environment_used_when_requested(self, tmp_path, jinja_context):
        with (
            mock.patch(
                "airflow.providers.cncf.kubernetes.python_kubernetes_script.NativeEnvironment"
            ) as mock_native_env,
            mock.patch("airflow.providers.cncf.kubernetes.python_kubernetes_script.Environment") as mock_env,
        ):
            write_python_script(
                jinja_context, str(tmp_path / "script.py"), render_template_as_native_obj=True
            )

        mock_native_env.assert_called_once()
        mock_env.assert_not_called()

    def test_standard_environment_used_by_default(self, tmp_path, jinja_context):
        with (
            mock.patch(
                "airflow.providers.cncf.kubernetes.python_kubernetes_script.NativeEnvironment"
            ) as mock_native_env,
            mock.patch("airflow.providers.cncf.kubernetes.python_kubernetes_script.Environment") as mock_env,
        ):
            write_python_script(jinja_context, str(tmp_path / "script.py"))

        mock_env.assert_called_once()
        mock_native_env.assert_not_called()

    def test_missing_context_variable_raises(self, tmp_path):
        with pytest.raises(UndefinedError):
            write_python_script({}, str(tmp_path / "script.py"))
