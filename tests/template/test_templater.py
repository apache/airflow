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

import jinja2

from airflow.io.path import ObjectStoragePath
from airflow.models.dag import DAG
from airflow.template.templater import LiteralValue, Templater
from airflow.utils.context import Context


class TestTemplater:
    def test_get_template_env(self):
        # Test get_template_env when a DAG is provided
        templater = Templater()
        dag = DAG(dag_id="test_dag", schedule=None, render_template_as_native_obj=True)
        env = templater.get_template_env(dag)
        assert isinstance(env, jinja2.Environment)
        assert not env.sandboxed

        # Test get_template_env when no DAG is provided
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
        context = Context({"ds": "2006-02-01"})  # type: ignore
        jinja_env = templater.get_template_env()
        rendered_content = templater._render_object_storage_path(path, context, jinja_env)
        assert rendered_content == ObjectStoragePath("s3://bucket/key/2006-02-01/part")

    def test_render_template(self):
        context = Context({"name": "world"})  # type: ignore
        templater = Templater()
        templater.message = "Hello {{ name }}"
        templater.template_fields = ["message"]
        templater.template_ext = [".txt"]
        rendered_content = templater.render_template(templater.message, context)
        assert rendered_content == "Hello world"

    def test_not_render_literal_value(self):
        templater = Templater()
        templater.template_ext = []
        context = Context()
        content = LiteralValue("Hello {{ name }}")

        rendered_content = templater.render_template(content, context)

        assert rendered_content == "Hello {{ name }}"

    def test_not_render_file_literal_value(self):
        templater = Templater()
        templater.template_ext = [".txt"]
        context = Context()
        content = LiteralValue("template_file.txt")

        rendered_content = templater.render_template(content, context)

        assert rendered_content == "template_file.txt"
