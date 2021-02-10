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

from tempfile import mkdtemp
from unittest import TestCase

import jinja2
import pytest

from airflow import DAG
from airflow.models import DagBag
from airflow.operators.bash_operator import BashOperator
from airflow.upgrade.rules.undefined_jinja_varaibles import UndefinedJinjaVariablesRule
from tests.models import DEFAULT_DATE


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


class TestUndefinedJinjaVariablesRule(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.empty_dir = mkdtemp()

    def setUpValidDag(self):
        self.valid_dag = DAG(
            dag_id="test-defined-jinja-variables", start_date=DEFAULT_DATE
        )

        BashOperator(
            task_id="templated_string",
            depends_on_past=False,
            bash_command="echo",
            env={
                "integer": "{{ params.integer }}",
                "float": "{{ params.float }}",
                "string": "{{ params.string }}",
                "boolean": "{{ params.boolean }}",
            },
            params={
                "integer": 1,
                "float": 1.0,
                "string": "test_string",
                "boolean": True,
            },
            dag=self.valid_dag,
        )

    def setUpDagToSkip(self):
        self.skip_dag = DAG(
            dag_id="test-defined-jinja-variables",
            start_date=DEFAULT_DATE,
            template_undefined=jinja2.Undefined,
        )

        BashOperator(
            task_id="templated_string",
            depends_on_past=False,
            bash_command="{{ undefined }}",
            dag=self.skip_dag,
        )

    def setUpInvalidDag(self):
        self.invalid_dag = DAG(
            dag_id="test-undefined-jinja-variables", start_date=DEFAULT_DATE
        )

        invalid_template_command = """
            {% for i in range(5) %}
                echo "{{ params.defined_variable }}"
                echo "{{ execution_date.today }}"
                echo "{{ execution_date.invalid_element }}"
                echo "{{ params.undefined_variable }}"
                echo "{{ foo }}"
            {% endfor %}
            """

        nested_validation = ClassWithCustomAttributes(
            nested1=ClassWithCustomAttributes(
                att1="{{ nested.undefined }}", template_fields=["att1"]
            ),
            nested2=ClassWithCustomAttributes(
                att2="{{ bar }}", template_fields=["att2"]
            ),
            template_fields=["nested1", "nested2"],
        )

        BashOperator(
            task_id="templated_string",
            depends_on_past=False,
            bash_command=invalid_template_command,
            env={
                "undefined_object": "{{ undefined_object.element }}",
                "nested_object": nested_validation,
            },
            params={"defined_variable": "defined_value"},
            dag=self.invalid_dag,
        )

    def setUp(self):
        self.setUpValidDag()
        self.setUpDagToSkip()
        self.setUpInvalidDag()

    def test_description_and_title_is_defined(self):
        rule = UndefinedJinjaVariablesRule()
        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

    def test_valid_check(self):
        dagbag = DagBag(dag_folder=self.empty_dir, include_examples=False)
        dagbag.dags[self.valid_dag.dag_id] = self.valid_dag
        rule = UndefinedJinjaVariablesRule()

        messages = rule.check(dagbag)

        assert len(messages) == 0

    def test_skipping_dag_check(self):
        dagbag = DagBag(dag_folder=self.empty_dir, include_examples=False)
        dagbag.dags[self.skip_dag.dag_id] = self.skip_dag
        rule = UndefinedJinjaVariablesRule()

        messages = rule.check(dagbag)

        assert len(messages) == 0

    @pytest.mark.quarantined
    def test_invalid_check(self):
        dagbag = DagBag(dag_folder=self.empty_dir, include_examples=False)
        dagbag.dags[self.invalid_dag.dag_id] = self.invalid_dag
        rule = UndefinedJinjaVariablesRule()

        messages = rule.check(dagbag)

        expected_messages = [
            "Possible UndefinedJinjaVariable -> DAG: test-undefined-jinja-variables, "
            "Task: templated_string, Attribute: bash_command, Error: no such element: "
            "dict object['undefined_variable']",
            "Possible UndefinedJinjaVariable -> DAG: test-undefined-jinja-variables, "
            "Task: templated_string, Attribute: bash_command, Error: no such element: "
            "pendulum.pendulum.Pendulum object['invalid_element']",
            "Possible UndefinedJinjaVariable -> DAG: test-undefined-jinja-variables, "
            "Task: templated_string, Attribute: bash_command, Error: foo",
            "Possible UndefinedJinjaVariable -> DAG: test-undefined-jinja-variables, "
            "Task: templated_string, Attribute: env, Error: Could not find the "
            "object 'undefined_object",
            "Possible UndefinedJinjaVariable -> DAG: test-undefined-jinja-variables, "
            "Task: templated_string, Attribute: env, Error: Could not find the object 'nested'",
            "Possible UndefinedJinjaVariable -> DAG: test-undefined-jinja-variables, "
            "Task: templated_string, Attribute: env, Error: bar  NestedTemplateField=att2 "
            "NestedTemplateField=nested2",
            "Possible UndefinedJinjaVariable -> DAG: test-undefined-jinja-variables, "
            "Task: templated_string, Attribute: env, Error: no such element: "
            "dict object['undefined']  NestedTemplateField=att1 NestedTemplateField=nested1",
            "Possible UndefinedJinjaVariable -> DAG: test-undefined-jinja-variables, "
            "Task: templated_string, Attribute: env, Error: no such element: dict object['element']",
        ]

        assert len(messages) == len(expected_messages)
        assert [m for m in messages if m in expected_messages], len(messages) == len(
            expected_messages
        )
