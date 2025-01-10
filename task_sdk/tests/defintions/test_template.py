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

from airflow.sdk.definitions._internal.templater import Templater
from airflow.sdk.definitions.template import literal


def test_not_render_literal_value():
    templater = Templater()
    templater.template_ext = []
    context = {}
    content = literal("Hello {{ name }}")

    rendered_content = templater.render_template(content, context)

    assert rendered_content == "Hello {{ name }}"


def test_not_render_file_literal_value():
    templater = Templater()
    templater.template_ext = [".txt"]
    context = {}
    content = literal("template_file.txt")

    rendered_content = templater.render_template(content, context)

    assert rendered_content == "template_file.txt"
