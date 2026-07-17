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

from datetime import datetime, timezone
from unittest.mock import MagicMock, NonCallableMagicMock

import jinja2
import pytest

from airflow.sdk import DAG, ObjectStoragePath
from airflow.sdk.definitions._internal.templater import LiteralValue, SandboxedEnvironment, Templater


class TestTemplater:
    def test_get_template_env(self):
        # Test get_template_env when a Dag is provided
        templater = Templater()
        dag = DAG(dag_id="test_dag", schedule=None, render_template_as_native_obj=True)
        env = templater.get_template_env(dag)
        assert isinstance(env, jinja2.Environment)
        assert not env.sandboxed

        # Test get_template_env when no Dag is provided
        templater = Templater()
        env = templater.get_template_env()
        assert isinstance(env, jinja2.Environment)
        assert env.sandboxed

    def test_prepare_template(self):
        # Test that prepare_template is a no-op
        templater = Templater()
        templater.prepare_template()

    def test_resolve_template_files_logs_exception(self, caplog):
        templater = Templater()
        templater.message = "template_file.txt"
        templater.template_fields = ["message"]
        templater.template_ext = [".txt"]
        templater.resolve_template_files()
        assert "Failed to resolve template field 'message'" in caplog.text

    def test_render_object_storage_path(self):
        templater = Templater()
        path = ObjectStoragePath("s3://bucket/key/{{ ds }}/part")
        context = {"ds": "2006-02-01"}
        jinja_env = templater.get_template_env()
        rendered_content = templater._render_object_storage_path(path, context, jinja_env)
        assert rendered_content == ObjectStoragePath("s3://bucket/key/2006-02-01/part")

    def test_render_template(self):
        context = {"name": "world"}
        templater = Templater()
        templater.message = "Hello {{ name }}"
        templater.template_fields = ["message"]
        templater.template_ext = [".txt"]
        rendered_content = templater.render_template(templater.message, context)
        assert rendered_content == "Hello world"

    def test_not_render_literal_value(self):
        templater = Templater()
        templater.template_ext = []
        context = {}
        content = LiteralValue("Hello {{ name }}")

        rendered_content = templater.render_template(content, context)

        assert rendered_content == "Hello {{ name }}"

    def test_render_template_pathlib_path_not_resolved(self):
        """Test that pathlib.Path objects are not incorrectly resolved via their resolve() method.

        pathlib.Path has a resolve() method for filesystem resolution, which should not be
        confused with the Resolvable.resolve(context) protocol used by the templater.
        See: https://github.com/apache/airflow/issues/55412
        """
        import pathlib

        templater = Templater()
        templater.template_ext = []
        context = {"ds": "2006-02-01"}
        path = pathlib.PurePosixPath("/some/path/to/file.txt")

        rendered = templater.render_template(path, context)

        # The path should be returned as-is, not passed through resolve(context)
        assert rendered == path
        assert isinstance(rendered, pathlib.PurePosixPath)

    def test_not_render_file_literal_value(self):
        templater = Templater()
        templater.template_ext = [".txt"]
        context = {}
        content = LiteralValue("template_file.txt")

        rendered_content = templater.render_template(content, context)

        assert rendered_content == "template_file.txt"

    def test_do_render_template_fields_basic(self):
        """Test that _do_render_template_fields renders a simple string template field in-place."""
        templater = Templater()
        templater.template_ext = []

        parent = MagicMock(spec=["greeting"])
        parent.greeting = "Hello {{ name }}"

        context = {"name": "world"}
        jinja_env = templater.get_template_env()

        templater._do_render_template_fields(parent, ["greeting"], context, jinja_env, set())

        assert parent.greeting == "Hello world"

    def test_do_render_template_fields_multiple_fields(self):
        """Test rendering multiple template fields at once."""
        templater = Templater()
        templater.template_ext = []

        parent = MagicMock(spec=["first", "second"])
        parent.first = "Hello {{ name }}"
        parent.second = "Date: {{ ds }}"

        context = {"name": "world", "ds": "2024-01-01"}
        jinja_env = templater.get_template_env()

        templater._do_render_template_fields(parent, ["first", "second"], context, jinja_env, set())

        assert parent.first == "Hello world"
        assert parent.second == "Date: 2024-01-01"

    def test_do_render_template_fields_callable_value(self):
        """Test that callable field values are called with context and jinja_env."""
        templater = Templater()
        templater.template_ext = []

        callback = MagicMock(spec=lambda context, jinja_env: None, return_value="resolved")
        parent = MagicMock(spec=["my_field"])
        parent.my_field = callback

        context = {"key": "value"}
        jinja_env = templater.get_template_env()

        templater._do_render_template_fields(parent, ["my_field"], context, jinja_env, set())

        callback.assert_called_once_with(context=context, jinja_env=jinja_env)
        assert parent.my_field == "resolved"

    def test_do_render_template_fields_skips_falsy_values(self):
        """Test that falsy field values (empty string, None, 0) are skipped."""
        templater = Templater()
        templater.template_ext = []

        parent = MagicMock(spec=["empty_str", "none_val"])
        parent.empty_str = ""
        parent.none_val = None

        context = {"name": "world"}
        jinja_env = templater.get_template_env()

        templater._do_render_template_fields(parent, ["empty_str", "none_val"], context, jinja_env, set())

        # Falsy values should not be touched
        assert parent.empty_str == ""
        assert parent.none_val is None

    def test_do_render_template_fields_missing_attribute(self):
        """Test that a missing attribute on parent raises AttributeError."""
        templater = Templater()
        templater.template_ext = []

        parent = MagicMock(spec=["existing"])
        parent.existing = "value"

        context = {}
        jinja_env = templater.get_template_env()

        with pytest.raises(
            AttributeError,
            match="'nonexistent' is configured as a template field",
        ):
            templater._do_render_template_fields(parent, ["nonexistent"], context, jinja_env, set())

    def test_do_render_template_fields_exception_logged_with_task_id(self, caplog):
        """Test that rendering errors are logged with task_id when available and re-raised."""
        templater = Templater()
        templater.template_ext = []
        templater.task_id = "my_task"

        parent = MagicMock(spec=["bad_field"])
        parent.bad_field = "{{ undefined_var }}"

        context = {}
        jinja_env = SandboxedEnvironment(undefined=jinja2.StrictUndefined, cache_size=0)

        with pytest.raises(jinja2.UndefinedError):
            templater._do_render_template_fields(parent, ["bad_field"], context, jinja_env, set())

        assert "Exception rendering Jinja template for task 'my_task', field 'bad_field'" in caplog.text

    def test_do_render_template_fields_exception_logged_without_task_id(self, caplog):
        """Test that rendering errors are logged with parent type name when no task_id."""
        templater = Templater()
        templater.template_ext = []

        parent = MagicMock(spec=["bad_field"])
        parent.bad_field = "{{ undefined_var }}"

        context = {}
        jinja_env = SandboxedEnvironment(undefined=jinja2.StrictUndefined, cache_size=0)

        with pytest.raises(jinja2.UndefinedError):
            templater._do_render_template_fields(parent, ["bad_field"], context, jinja_env, set())

        assert "Exception rendering Jinja template for MagicMock, field 'bad_field'" in caplog.text

    def test_do_render_template_fields_nested_template_fields(self):
        """Test rendering nested objects that have their own template_fields."""
        templater = Templater()
        templater.template_ext = []

        inner = NonCallableMagicMock(spec=["template_fields", "message"])
        inner.template_fields = ["message"]
        inner.message = "Hello {{ name }}"

        parent = MagicMock(spec=["nested"])
        parent.nested = inner

        context = {"name": "world"}
        jinja_env = templater.get_template_env()

        templater._do_render_template_fields(parent, ["nested"], context, jinja_env, set())

        assert inner.message == "Hello world"

    def test_do_render_template_fields_seen_oids_prevents_reprocessing(self):
        """Test that already-seen objects (by id) are not re-rendered."""
        templater = Templater()
        templater.template_ext = []

        parent = MagicMock(spec=["greeting"])
        parent.greeting = "Hello {{ name }}"

        context = {"name": "world"}
        jinja_env = templater.get_template_env()

        # Pre-populate seen_oids with the parent's greeting value id
        seen_oids = {id(parent.greeting)}

        templater._do_render_template_fields(parent, ["greeting"], context, jinja_env, seen_oids)

        # The value should NOT be rendered because render_template checks
        # `id(value) in seen_oids` and short-circuits, returning the original
        # unrendered string.
        assert parent.greeting == "Hello {{ name }}"

    def test_do_render_template_fields_renders_dict_values(self):
        """Test that dict field values have their inner templates rendered."""
        templater = Templater()
        templater.template_ext = []

        parent = MagicMock(spec=["params"])
        parent.params = {"key": "{{ value }}"}

        context = {"value": "rendered"}
        jinja_env = templater.get_template_env()

        templater._do_render_template_fields(parent, ["params"], context, jinja_env, set())

        assert parent.params == {"key": "rendered"}

    def test_do_render_template_fields_renders_list_values(self):
        """Test that list field values have their inner templates rendered."""
        templater = Templater()
        templater.template_ext = []

        parent = MagicMock(spec=["items"])
        parent.items = ["{{ a }}", "{{ b }}"]

        context = {"a": "first", "b": "second"}
        jinja_env = templater.get_template_env()

        templater._do_render_template_fields(parent, ["items"], context, jinja_env, set())

        assert parent.items == ["first", "second"]


@pytest.fixture
def env():
    return SandboxedEnvironment(undefined=jinja2.StrictUndefined, cache_size=0)


def test_protected_access(env):
    class Test:
        _protected = 123

    assert env.from_string(r"{{ obj._protected }}").render(obj=Test) == "123"


def test_private_access(env):
    with pytest.raises(jinja2.exceptions.SecurityError):
        env.from_string(r"{{ func.__code__ }}").render(func=test_private_access)


@pytest.mark.parametrize(
    ("name", "expected"),
    (
        ("ds", "2012-07-24"),
        ("ds_nodash", "20120724"),
        ("ts", "2012-07-24T03:04:52+00:00"),
        ("ts_nodash", "20120724T030452"),
        ("ts_nodash_with_tz", "20120724T030452+0000"),
    ),
)
def test_filters(env, name, expected):
    when = datetime(2012, 7, 24, 3, 4, 52, tzinfo=timezone.utc)
    result = env.from_string("{{ date |" + name + " }}").render(date=when)
    assert result == expected
