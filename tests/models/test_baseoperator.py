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
import uuid
from collections import namedtuple
from unittest import mock

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


class ClassWithCustomAttributes:
    """Class for testing purpose: allows to create objects with custom attributes in one single statement."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __str__(self):
        return "{}({})".format(ClassWithCustomAttributes.__name__, str(self.__dict__))

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)


# Objects with circular references (for testing purpose)
object1 = ClassWithCustomAttributes(
    attr="{{ foo }}_1",
    template_fields=["ref"]
)
object2 = ClassWithCustomAttributes(
    attr="{{ foo }}_2",
    ref=object1,
    template_fields=["ref"]
)
setattr(object1, 'ref', object2)


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
            (None, {}, None),
            ([], {}, []),
            ({}, {}, {}),
            (
                # check nested fields can be templated
                ClassWithCustomAttributes(att1="{{ foo }}_1", att2="{{ foo }}_2", template_fields=["att1"]),
                {"foo": "bar"},
                ClassWithCustomAttributes(att1="bar_1", att2="{{ foo }}_2", template_fields=["att1"]),
            ),
            (
                # check deep nested fields can be templated
                ClassWithCustomAttributes(nested1=ClassWithCustomAttributes(att1="{{ foo }}_1",
                                                                            att2="{{ foo }}_2",
                                                                            template_fields=["att1"]),
                                          nested2=ClassWithCustomAttributes(att3="{{ foo }}_3",
                                                                            att4="{{ foo }}_4",
                                                                            template_fields=["att3"]),
                                          template_fields=["nested1"]),
                {"foo": "bar"},
                ClassWithCustomAttributes(nested1=ClassWithCustomAttributes(att1="bar_1",
                                                                            att2="{{ foo }}_2",
                                                                            template_fields=["att1"]),
                                          nested2=ClassWithCustomAttributes(att3="{{ foo }}_3",
                                                                            att4="{{ foo }}_4",
                                                                            template_fields=["att3"]),
                                          template_fields=["nested1"]),
            ),
            (
                # check null value on nested template field
                ClassWithCustomAttributes(att1=None,
                                          template_fields=["att1"]),
                {},
                ClassWithCustomAttributes(att1=None,
                                          template_fields=["att1"]),
            ),
            (
                # check there is no RecursionError on circular references
                object1,
                {"foo": "bar"},
                object1,
            ),
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

    def test_nested_template_fields_declared_must_exist(self):
        """Test render_template when a nested template field is missing."""
        with DAG("test-dag", start_date=DEFAULT_DATE):
            task = DummyOperator(task_id="op1")

        with self.assertRaises(AttributeError) as e:
            task.render_template(ClassWithCustomAttributes(template_fields=["missing_field"]), {})

        self.assertEqual("'ClassWithCustomAttributes' object has no attribute 'missing_field'",
                         str(e.exception))

    def test_jinja_invalid_expression_is_just_propagated(self):
        """Test render_template propagates Jinja invalid expression errors."""
        with DAG("test-dag", start_date=DEFAULT_DATE):
            task = DummyOperator(task_id="op1")

        with self.assertRaises(jinja2.exceptions.TemplateSyntaxError):
            task.render_template("{{ invalid expression }}", {})

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
