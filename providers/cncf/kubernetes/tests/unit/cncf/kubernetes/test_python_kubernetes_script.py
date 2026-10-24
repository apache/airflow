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

import jinja2.exceptions
import pytest

from airflow.providers.cncf.kubernetes.python_kubernetes_script import (
    _balance_parens,
    remove_task_decorator,
    write_python_script,
)


def test_balance_parens():
    assert _balance_parens("(foo)remainder") == "remainder"
    assert _balance_parens("(a(b(c))d)end") == "end"
    assert _balance_parens("((arg1, arg2))after") == "after"


def test_remove_task_decorator_no_arguments():
    source = "@task.kubernetes\ndef my_func():\n    return 42\n"
    expected = "def my_func():\n    return 42\n"
    assert remove_task_decorator(source, "@task.kubernetes") == expected


def test_remove_task_decorator_with_arguments():
    source = '@task.kubernetes(image="python:3.10")\ndef my_func():\n    return 42\n'
    expected = "def my_func():\n    return 42\n"
    assert remove_task_decorator(source, "@task.kubernetes") == expected


def test_remove_task_decorator_nested_parentheses():
    source = '@task.kubernetes(image=get_image(tag="v1"), env=dict(A="1"))\ndef my_func():\n    return 42\n'
    expected = "def my_func():\n    return 42\n"
    assert remove_task_decorator(source, "@task.kubernetes") == expected


def test_remove_task_decorator_with_setup_and_teardown():
    source_setup = '@setup\n@task.kubernetes(image="python:3.10")\ndef my_setup():\n    return 1\n'
    assert remove_task_decorator(source_setup, "@task.kubernetes") == "def my_setup():\n    return 1\n"

    source_teardown = "@teardown\n@task.kubernetes\ndef my_teardown():\n    return 0\n"
    assert remove_task_decorator(source_teardown, "@task.kubernetes") == "def my_teardown():\n    return 0\n"


def test_remove_task_decorator_absent():
    source = "def regular_func():\n    return 42\n"
    assert remove_task_decorator(source, "@task.kubernetes") == source


def test_write_python_script_standard(tmp_path):
    target_file = tmp_path / "script.py"
    context = {
        "pickling_library": "pickle",
        "op_args": False,
        "op_kwargs": False,
        "python_callable_source": "def my_task(): pass",
        "python_callable": "my_task",
    }
    write_python_script(jinja_context=context, filename=str(target_file))
    assert target_file.exists()
    content = target_file.read_text()
    assert "import pickle" in content
    assert "def my_task(): pass" in content
    assert 'res = my_task(*arg_dict["args"], **arg_dict["kwargs"])' in content


def test_write_python_script_native_env(tmp_path):
    target_file = tmp_path / "script_native.py"
    context = {
        "pickling_library": "dill",
        "op_args": True,
        "op_kwargs": True,
        "python_callable_source": "def compute(a, b): return a + b",
        "python_callable": "compute",
    }
    write_python_script(
        jinja_context=context,
        filename=str(target_file),
        render_template_as_native_obj=True,
    )
    assert target_file.exists()
    content = target_file.read_text()
    assert "import dill" in content
    assert 'with open(sys.argv[1], "rb") as file:' in content


def test_write_python_script_strict_undefined_raises(tmp_path):
    target_file = tmp_path / "script_err.py"
    incomplete_context = {
        "python_callable": "func",
    }
    with pytest.raises(jinja2.exceptions.UndefinedError):
        write_python_script(jinja_context=incomplete_context, filename=str(target_file))
