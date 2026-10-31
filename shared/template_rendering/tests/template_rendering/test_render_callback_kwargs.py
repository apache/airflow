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

import logging

import pytest
from jinja2.sandbox import SandboxedEnvironment

from airflow_shared.template_rendering import render_callback_kwargs


@pytest.fixture
def env():
    return SandboxedEnvironment(cache_size=0)


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        pytest.param(
            {"message": "Dag {{ dag_id }} missed its deadline on {{ ds }}"},
            {"message": "Dag my_dag missed its deadline on 2024-06-15"},
            id="renders_templated_string",
        ),
        pytest.param(
            {"message": "plain text", "count": 42, "data": {"nested": "{{ dag_id }}"}},
            {"message": "plain text", "count": 42, "data": {"nested": "{{ dag_id }}"}},
            id="other_values_pass_through",
        ),
        pytest.param(
            {"context": "{{ dag_id }}", "message": "{{ dag_id }}"},
            {"context": "{{ dag_id }}", "message": "my_dag"},
            id="context_key_is_never_rendered",
        ),
    ],
)
def test_render_callback_kwargs(env, kwargs, expected):
    assert render_callback_kwargs(kwargs, {"dag_id": "my_dag", "ds": "2024-06-15"}, env) == expected


def test_render_callback_kwargs_uses_the_given_environment(env):
    env.filters["shout"] = str.upper
    rendered = render_callback_kwargs({"message": "{{ dag_id | shout }}"}, {"dag_id": "my_dag"}, env)
    assert rendered == {"message": "MY_DAG"}


def test_render_callback_kwargs_passes_bad_template_unrendered(env, caplog):
    kwargs = {"message": "{{ this is not valid jinja", "other": "{{ dag_id }}"}
    with caplog.at_level(logging.WARNING):
        rendered = render_callback_kwargs(kwargs, {"dag_id": "my_dag"}, env)
    assert rendered == {"message": "{{ this is not valid jinja", "other": "my_dag"}
    assert "Could not render the template in callback kwarg 'message'" in caplog.text
    assert "TemplateSyntaxError" in caplog.text
