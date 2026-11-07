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

import textwrap

import pytest
from jinja2 import UndefinedError
from jinja2.nativetypes import NativeEnvironment

from airflow.providers.cncf.kubernetes.python_kubernetes_script import (
    _balance_parens,
    remove_task_decorator,
    write_python_script,
)


class TestBalanceParens:
    """Tests for the _balance_parens helper."""

    def test_simple_parens(self):
        # After "(arg)\nrest", consuming balanced parens should leave "\nrest"
        result = _balance_parens("(arg)\nrest")
        assert result == "\nrest"

    def test_nested_parens(self):
        result = _balance_parens("(a, (b, c))\ndef foo(): pass")
        assert result == "\ndef foo(): pass"

    def test_deeply_nested_parens(self):
        result = _balance_parens("(a, (b, (c, d)))\ncode")
        assert result == "\ncode"

    def test_empty_parens(self):
        result = _balance_parens("()\ncode")
        assert result == "\ncode"


class TestRemoveTaskDecorator:
    """Tests for remove_task_decorator."""

    def test_decorator_no_args(self):
        source = textwrap.dedent("""\
            @task.kubernetes
            def my_func():
                pass
        """)
        result = remove_task_decorator(source, "@task.kubernetes")
        assert "@task.kubernetes" not in result
        assert "def my_func():" in result
        assert "    pass" in result

    def test_decorator_with_args(self):
        source = textwrap.dedent("""\
            @task.kubernetes(image="python:3.9")
            def my_func():
                pass
        """)
        result = remove_task_decorator(source, "@task.kubernetes")
        assert "@task.kubernetes" not in result
        assert "def my_func():" in result

    def test_decorator_with_nested_parens(self):
        source = textwrap.dedent("""\
            @task.kubernetes(image="python:3.9", resources=dict(limit_cpu=("1", "2")))
            def my_func():
                pass
        """)
        result = remove_task_decorator(source, "@task.kubernetes")
        assert "@task.kubernetes" not in result
        assert "def my_func():" in result

    def test_decorator_with_setup_and_teardown(self):
        source = textwrap.dedent("""\
            @setup
            @task.kubernetes
            @teardown
            def my_func():
                pass
        """)
        result = remove_task_decorator(source, "@task.kubernetes")
        assert "@task.kubernetes" not in result
        assert "@setup" not in result
        assert "@teardown" not in result
        assert "def my_func():" in result

    def test_decorator_not_present_returns_source_unchanged(self):
        source = textwrap.dedent("""\
            def my_func():
                pass
        """)
        result = remove_task_decorator(source, "@task.kubernetes")
        assert result == source

    def test_setup_only_stripped(self):
        source = textwrap.dedent("""\
            @setup
            def my_func():
                pass
        """)
        result = remove_task_decorator(source, "@task.kubernetes")
        assert "@setup" not in result
        assert "def my_func():" in result

    def test_teardown_only_stripped(self):
        source = textwrap.dedent("""\
            @teardown
            def my_func():
                pass
        """)
        result = remove_task_decorator(source, "@task.kubernetes")
        assert "@teardown" not in result
        assert "def my_func():" in result

    @pytest.mark.parametrize(
        "decorator_name",
        [
            "@task.kubernetes",
            "@task.docker",
            "@task.virtualenv",
        ],
    )
    def test_various_decorator_names(self, decorator_name):
        source = f"{decorator_name}\ndef my_func():\n    pass\n"
        result = remove_task_decorator(source, decorator_name)
        assert decorator_name not in result
        assert "def my_func():" in result

    def test_inner_remove_task_decorator_reads_enclosing_scope(self):
        """Pin the current behaviour: the inner _remove_task_decorator closure reads
        `python_source` from the enclosing scope rather than its own `py_source` argument.
        Both happen to be the same object on every loop iteration, so the result is
        correct today, but this test pins that behaviour."""
        source = textwrap.dedent("""\
            @task.kubernetes
            def my_func():
                return 42
        """)
        result = remove_task_decorator(source, "@task.kubernetes")
        expected = textwrap.dedent("""\
            def my_func():
                return 42
        """)
        assert result == expected

    def test_decorator_with_multiline_args(self):
        source = textwrap.dedent("""\
            @task.kubernetes(
                image="python:3.9",
                namespace="default",
            )
            def my_func():
                pass
        """)
        result = remove_task_decorator(source, "@task.kubernetes")
        assert "@task.kubernetes" not in result
        assert "def my_func():" in result


class TestWritePythonScript:
    """Tests for write_python_script."""

    def test_renders_template_to_file(self, tmp_path):
        output_file = str(tmp_path / "script.py")
        jinja_context = {
            "op_args": True,
            "op_kwargs": True,
            "pickling_library": "pickle",
            "python_callable_source": "def my_func():\n    return 42",
            "python_callable": "my_func",
        }
        write_python_script(jinja_context, output_file)
        content = (tmp_path / "script.py").read_text()
        assert "import pickle" in content
        assert "def my_func():" in content
        assert "my_func(*arg_dict" in content

    def test_renders_without_op_args(self, tmp_path):
        output_file = str(tmp_path / "script.py")
        jinja_context = {
            "op_args": False,
            "op_kwargs": False,
            "pickling_library": "pickle",
            "python_callable_source": "def my_func():\n    return 42",
            "python_callable": "my_func",
        }
        write_python_script(jinja_context, output_file)
        content = (tmp_path / "script.py").read_text()
        assert 'arg_dict = {"args": [], "kwargs": {}}' in content

    def test_render_template_as_native_obj_uses_native_environment(self, tmp_path, monkeypatch):
        """When render_template_as_native_obj=True, NativeEnvironment is used."""
        captured_envs = []
        original_get_template = NativeEnvironment.get_template

        def spy_get_template(self, *args, **kwargs):
            captured_envs.append(type(self))
            return original_get_template(self, *args, **kwargs)

        monkeypatch.setattr(NativeEnvironment, "get_template", spy_get_template)

        output_file = str(tmp_path / "script.py")
        jinja_context = {
            "op_args": False,
            "op_kwargs": False,
            "pickling_library": "pickle",
            "python_callable_source": "def my_func():\n    pass",
            "python_callable": "my_func",
        }
        write_python_script(jinja_context, output_file, render_template_as_native_obj=True)
        assert NativeEnvironment in captured_envs

    def test_strict_undefined_raises_on_missing_variable(self, tmp_path):
        """StrictUndefined should raise when a required template variable is missing."""
        output_file = str(tmp_path / "script.py")
        jinja_context = {
            "op_args": True,
            "op_kwargs": True,
            # Missing 'pickling_library', 'python_callable_source', 'python_callable'
        }
        with pytest.raises(UndefinedError):
            write_python_script(jinja_context, output_file)

    def test_strict_undefined_raises_with_native_env(self, tmp_path):
        """StrictUndefined should also raise with NativeEnvironment."""
        output_file = str(tmp_path / "script.py")
        jinja_context = {
            "op_args": True,
            "op_kwargs": True,
        }
        with pytest.raises(UndefinedError):
            write_python_script(jinja_context, output_file, render_template_as_native_obj=True)
