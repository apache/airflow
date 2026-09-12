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
from jinja2 import Environment, StrictUndefined, UndefinedError
from jinja2.nativetypes import NativeEnvironment

from airflow.providers.cncf.kubernetes.python_kubernetes_script import (
    remove_task_decorator,
    write_python_script,
)


@pytest.mark.parametrize(
    ("python_source", "expected"),
    [
        (
            "@task.kubernetes\ndef callable():\n    return 1\n",
            "def callable():\n    return 1\n",
        ),
        (
            '@task.kubernetes(image=build_image(name="worker"), retries=2)\ndef callable():\n    return 1\n',
            "def callable():\n    return 1\n",
        ),
        (
            "@setup\n@teardown\n@task.kubernetes()\ndef callable():\n    return 1\n",
            "def callable():\n    return 1\n",
        ),
    ],
)
def test_remove_task_decorator_removes_task_and_lifecycle_decorators(python_source, expected):
    assert remove_task_decorator(python_source, "@task.kubernetes") == expected


def test_remove_task_decorator_returns_source_unchanged_without_task_decorator():
    python_source = "@other_decorator\ndef callable():\n    return 1\n"

    assert remove_task_decorator(python_source, "@task.kubernetes") == python_source


def test_write_python_script_renders_template(tmp_path):
    filename = tmp_path / "script.py"

    write_python_script(
        {
            "op_args": [],
            "op_kwargs": {},
            "pickling_library": "pickle",
            "python_callable": "callable",
            "python_callable_source": "def callable():\n    return 1",
        },
        str(filename),
    )

    rendered_script = filename.read_text()
    assert "import pickle" in rendered_script
    assert 'arg_dict = {"args": [], "kwargs": {}}' in rendered_script
    assert "def callable():\n    return 1" in rendered_script
    assert 'res = callable(*arg_dict["args"], **arg_dict["kwargs"])' in rendered_script


@mock.patch(
    "airflow.providers.cncf.kubernetes.python_kubernetes_script.NativeEnvironment",
    wraps=NativeEnvironment,
)
def test_write_python_script_uses_native_environment_for_native_rendering(mock_native_environment, tmp_path):
    filename = tmp_path / "script.py"

    write_python_script(
        {
            "op_args": [],
            "op_kwargs": {},
            "pickling_library": "pickle",
            "python_callable": "callable",
            "python_callable_source": "def callable():\n    return 1",
        },
        str(filename),
        render_template_as_native_obj=True,
    )

    mock_native_environment.assert_called_once_with(
        loader=mock.ANY,
        undefined=StrictUndefined,
    )
    assert "def callable():" in filename.read_text()


@mock.patch(
    "airflow.providers.cncf.kubernetes.python_kubernetes_script.Environment",
    wraps=Environment,
)
def test_write_python_script_raises_for_missing_template_context(mock_environment, tmp_path):
    with pytest.raises(UndefinedError):
        write_python_script({}, str(tmp_path / "script.py"))

    mock_environment.assert_called_once_with(
        loader=mock.ANY,
        undefined=StrictUndefined,
        autoescape=mock.ANY,
    )
