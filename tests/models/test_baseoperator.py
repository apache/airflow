# -*- coding: utf-8 -*-
#
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

import datetime
import unittest
from unittest import mock
import uuid
from collections import namedtuple

import jinja2
from parameterized import parameterized

from airflow.models import DAG, BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.decorators import apply_defaults
from tests.models import DEFAULT_DATE


class TestOperator(BaseOperator):
    """Operator for testing purposes."""

    template_fields = ("arg1", "arg2")

    @apply_defaults
    def __init__(self, arg1: str = "", arg2: str = "", **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1
        self.arg2 = arg2

    def execute(self, context):
        pass


# Namedtuple for testing purposes
TestNamedTuple = namedtuple("TestNamedTuple", ["var1", "var2"])


class TestBaseOperator(unittest.TestCase):
    @parameterized.expand(
        [
            ("{{ foo }}", {"foo": "bar"}, "bar"),
            ("{{ foo }}", {}, ""),
            (["{{ foo }}_1", "{{ foo }}_2"], {"foo": "bar"}, ["bar_1", "bar_2"]),
            (("{{ foo }}_1", "{{ foo }}_2"), {"foo": "bar"}, ("bar_1", "bar_2")),
            (
                {"key1": "{{ foo }}_1", "key2": "{{ foo }}_2"},
                {"foo": "bar"},
                {"key1": "bar_1", "key2": "bar_2"},
            ),
            (
                {"key_{{ foo }}_1": 1, "key_2": "{{ foo }}_2"},
                {"foo": "bar"},
                {"key_{{ foo }}_1": 1, "key_2": "bar_2"},
            ),
            (datetime.date(2018, 12, 6), {"foo": "bar"}, datetime.date(2018, 12, 6)),
            (datetime.datetime(2018, 12, 6, 10, 55), {"foo": "bar"}, datetime.datetime(2018, 12, 6, 10, 55)),
            (TestNamedTuple("{{ foo }}_1", "{{ foo }}_2"), {"foo": "bar"}, TestNamedTuple("bar_1", "bar_2")),
            ({"{{ foo }}_1", "{{ foo }}_2"}, {"foo": "bar"}, {"bar_1", "bar_2"}),
            # By default, Jinja2 drops one (single) trailing newline
            ("{{ foo }}\n\n", {"foo": "bar"}, "bar\n"),
        ]
    )
    def test_render_template(self, content, context, expected_output):
        """Test render_template given various input types."""
        with DAG("test-dag", start_date=DEFAULT_DATE):
            task = DummyOperator(task_id="op1")

        result = task.render_template(content, context)
        self.assertEqual(result, expected_output)

    def test_render_template_fields(self):
        """Verify if operator attributes are correctly templated."""
        with DAG("test-dag", start_date=DEFAULT_DATE):
            task = TestOperator(task_id="op1", arg1="{{ foo }}", arg2="{{ bar }}")

        # Assert nothing is templated yet
        self.assertEqual(task.arg1, "{{ foo }}")
        self.assertEqual(task.arg2, "{{ bar }}")

        # Trigger templating and verify if attributes are templated correctly
        task.render_template_fields(context={"foo": "footemplated", "bar": "bartemplated"})
        self.assertEqual(task.arg1, "footemplated")
        self.assertEqual(task.arg2, "bartemplated")

    @parameterized.expand(
        [
            ({"user_defined_macros": {"foo": "bar"}}, "{{ foo }}", {}, "bar"),
            ({"user_defined_macros": {"foo": "bar"}}, 1, {}, 1),
            (
                {"user_defined_filters": {"hello": lambda name: "Hello %s" % name}},
                "{{ 'world' | hello }}",
                {},
                "Hello world",
            ),
        ]
    )
    def test_render_template_fields_with_dag_settings(self, dag_kwargs, content, context, expected_output):
        """Test render_template with additional DAG settings."""
        with DAG("test-dag", start_date=DEFAULT_DATE, **dag_kwargs):
            task = DummyOperator(task_id="op1")

        result = task.render_template(content, context)
        self.assertEqual(result, expected_output)

    @parameterized.expand([(object(),), (uuid.uuid4(),)])
    def test_render_template_fields_no_change(self, content):
        """Tests if non-templatable types remain unchanged."""
        with DAG("test-dag", start_date=DEFAULT_DATE):
            task = DummyOperator(task_id="op1")

        result = task.render_template(content, {"foo": "bar"})
        self.assertEqual(content, result)

    def test_render_template_field_undefined_strict(self):
        """Test render_template with template_undefined configured."""
        with DAG("test-dag", start_date=DEFAULT_DATE, template_undefined=jinja2.StrictUndefined):
            task = DummyOperator(task_id="op1")

        with self.assertRaises(jinja2.UndefinedError):
            task.render_template("{{ foo }}", {})

    @mock.patch("jinja2.Environment", autospec=True)
    def test_jinja_env_creation(self, mock_jinja_env):
        """Verify if a Jinja environment is created only once when templating."""
        with DAG("test-dag", start_date=DEFAULT_DATE):
            task = TestOperator(task_id="op1", arg1="{{ foo }}", arg2="{{ bar }}")

        task.render_template_fields(context={"foo": "whatever", "bar": "whatever"})
        self.assertEqual(mock_jinja_env.call_count, 1)

    def test_set_jinja_env_additional_option(self):
        """Test render_template given various input types."""
        with DAG("test-dag",
                 start_date=DEFAULT_DATE,
                 jinja_environment_kwargs={'keep_trailing_newline': True}):
            task = DummyOperator(task_id="op1")

        result = task.render_template("{{ foo }}\n\n", {"foo": "bar"})
        self.assertEqual(result, "bar\n\n")

    def test_override_jinja_env_option(self):
        """Test render_template given various input types."""
        with DAG("test-dag",
                 start_date=DEFAULT_DATE,
                 jinja_environment_kwargs={'cache_size': 50}):
            task = DummyOperator(task_id="op1")

        result = task.render_template("{{ foo }}", {"foo": "bar"})
        self.assertEqual(result, "bar")
