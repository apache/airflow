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

    def test_not_render_file_literal_value(self):
        templater = Templater()
        templater.template_ext = [".txt"]
        context = {}
        content = LiteralValue("template_file.txt")

        rendered_content = templater.render_template(content, context)

        assert rendered_content == "template_file.txt"


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
