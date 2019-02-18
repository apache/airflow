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

import copy
import logging
import os
import unittest
from collections import namedtuple
from datetime import timedelta, date

import jinja2

from airflow.exceptions import AirflowException
from airflow.models import TaskInstance as TI, DAG, DagRun
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.utils import timezone
from airflow.utils.db import create_session
from airflow.utils.state import State

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)

TI_CONTEXT_ENV_VARS = ['AIRFLOW_CTX_DAG_ID',
                       'AIRFLOW_CTX_TASK_ID',
                       'AIRFLOW_CTX_EXECUTION_DATE',
                       'AIRFLOW_CTX_DAG_RUN_ID']


class Call:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


def build_recording_function(calls_collection):
    """
    We can not use a Mock instance as a PythonOperator callable function or some tests fail with a
    TypeError: Object of type Mock is not JSON serializable
    Then using this custom function recording custom Call objects for further testing
    (replacing Mock.assert_called_with assertion method)
    """
    def recording_function(*args):
        calls_collection.append(Call(*args))
    return recording_function


class TestPythonOperator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self):
        super().setUp()
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL)
        self.addCleanup(self.dag.clear)
        self.clear_run()
        self.addCleanup(self.clear_run)
        try:
            delattr(ClassWithCustomAttributes, 'template_fields')
        except AttributeError:
            pass

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

        for var in TI_CONTEXT_ENV_VARS:
            if var in os.environ:
                del os.environ[var]

    def do_run(self):
        self.run = True

    def clear_run(self):
        self.run = False

    def is_run(self):
        return self.run

    def test_python_operator_run(self):
        """Tests that the python callable is invoked on task run."""
        task = PythonOperator(
            python_callable=self.do_run,
            task_id='python_operator',
            dag=self.dag)
        self.assertFalse(self.is_run())
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.assertTrue(self.is_run())

    def test_python_operator_python_callable_is_callable(self):
        """Tests that PythonOperator will only instantiate if
        the python_callable argument is callable."""
        not_callable = {}
        with self.assertRaises(AirflowException):
            PythonOperator(
                python_callable=not_callable,
                task_id='python_operator',
                dag=self.dag)
        not_callable = None
        with self.assertRaises(AirflowException):
            PythonOperator(
                python_callable=not_callable,
                task_id='python_operator',
                dag=self.dag)

    def _assert_calls_equal(self, first, second):
        self.assertIsInstance(first, Call)
        self.assertIsInstance(second, Call)
        self.assertTupleEqual(first.args, second.args)

    def test_python_callable_arguments_are_templatized(self):
        """Test PythonOperator op_args are templatized"""
        recorded_calls = []

        # Create a named tuple and ensure it is still preserved
        # after the rendering is done
        Named = namedtuple('Named', ['var1', 'var2'])
        named_tuple = Named('{{ ds }}', 'unchanged')

        task = PythonOperator(
            task_id='python_operator',
            # a Mock instance cannot be used as a callable function or test fails with a
            # TypeError: Object of type Mock is not JSON serializable
            python_callable=build_recording_function(recorded_calls),
            op_args=[
                4,
                date(2019, 1, 1),
                "dag {{dag.dag_id}} ran on {{ds}}.",
                named_tuple
            ],
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        ds_templated = DEFAULT_DATE.date().isoformat()
        self.assertEqual(1, len(recorded_calls))
        self._assert_calls_equal(
            recorded_calls[0],
            Call(4,
                 date(2019, 1, 1),
                 "dag {} ran on {}.".format(self.dag.dag_id, ds_templated),
                 Named(ds_templated, 'unchanged'))
        )

    def test_python_callable_keyword_arguments_are_templatized(self):
        """Test PythonOperator op_kwargs are templatized"""
        recorded_calls = []

        task = PythonOperator(
            task_id='python_operator',
            # a Mock instance cannot be used as a callable function or test fails with a
            # TypeError: Object of type Mock is not JSON serializable
            python_callable=build_recording_function(recorded_calls),
            op_kwargs={
                'an_int': 4,
                'a_date': date(2019, 1, 1),
                'a_templated_string': "dag {{dag.dag_id}} ran on {{ds}}."
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self.assertEqual(1, len(recorded_calls))
        self._assert_calls_equal(
            recorded_calls[0],
            Call(an_int=4,
                 a_date=date(2019, 1, 1),
                 a_templated_string="dag {} ran on {}.".format(
                     self.dag.dag_id, DEFAULT_DATE.date().isoformat()))
        )

    def test_python_operator_shallow_copy_attr(self):
        not_callable = lambda x: x
        original_task = PythonOperator(
            python_callable=not_callable,
            task_id='python_operator',
            op_kwargs={'certain_attrs': ''},
            dag=self.dag
        )
        new_task = copy.deepcopy(original_task)
        # shallow copy op_kwargs
        self.assertEqual(id(original_task.op_kwargs['certain_attrs']),
                         id(new_task.op_kwargs['certain_attrs']))
        # shallow copy python_callable
        self.assertEqual(id(original_task.python_callable),
                         id(new_task.python_callable))

    def _env_var_check_callback(self):
        self.assertEqual('test_dag', os.environ['AIRFLOW_CTX_DAG_ID'])
        self.assertEqual('hive_in_python_op', os.environ['AIRFLOW_CTX_TASK_ID'])
        self.assertEqual(DEFAULT_DATE.isoformat(),
                         os.environ['AIRFLOW_CTX_EXECUTION_DATE'])
        self.assertEqual('manual__' + DEFAULT_DATE.isoformat(),
                         os.environ['AIRFLOW_CTX_DAG_RUN_ID'])

    def test_echo_env_variables(self):
        """
        Test that env variables are exported correctly to the
        python callback in the task.
        """
        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=False,
        )

        t = PythonOperator(task_id='hive_in_python_op',
                           dag=self.dag,
                           python_callable=self._env_var_check_callback
                           )
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_conflicting_kwargs(self):
        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=False,
        )

        # dag is not allowed since it is a reserved keyword
        def fn(dag):
            # An ValueError should be triggered since we're using dag as a
            # reserved keyword
            raise RuntimeError("Should not be triggered, dag: {}".format(dag))

        python_operator = PythonOperator(
            task_id='python_operator',
            op_args=[1],
            python_callable=fn,
            dag=self.dag
        )

        with self.assertRaises(ValueError) as context:
            python_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
            self.assertTrue('dag' in context.exception, "'dag' not found in the exception")

    def test_context_with_conflicting_op_args(self):
        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=False,
        )

        def fn(custom, dag):
            self.assertEqual(1, custom, "custom should be 1")
            self.assertIsNotNone(dag, "dag should be set")

        python_operator = PythonOperator(
            task_id='python_operator',
            op_kwargs={'custom': 1},
            python_callable=fn,
            dag=self.dag
        )
        python_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_context_with_kwargs(self):
        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=False,
        )

        def fn(**context):
            # check if context is being set
            self.assertGreater(len(context), 0, "Context has not been injected")

        python_operator = PythonOperator(
            task_id='python_operator',
            op_kwargs={'custom': 1},
            python_callable=fn,
            dag=self.dag
        )
        python_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_template_field_value_rendering(self):
        """Test PythonOperator template field with a simple templated string"""
        recorded_calls = []

        task = PythonOperator(
            task_id='python_operator',
            python_callable=build_recording_function(recorded_calls),
            provide_context=True,
            templates_dict={
                'a_templated_string': "dag {{dag.dag_id}} ran on {{ds}}."
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self.assertEqual(1, len(recorded_calls))
        self.assertTrue('templates_dict' in recorded_calls[0].kwargs)
        self.assertDictEqual(
            recorded_calls[0].kwargs['templates_dict'],
            {'a_templated_string': "dag {} ran on {}.".format(self.dag.dag_id, DEFAULT_DATE.date())}
        )

    def test_template_field_object_rendering(self):
        """Test PythonOperator template field with a nested templated string attribute"""
        recorded_calls = []
        ClassWithCustomAttributes.template_fields = ['templated_string_attr']

        task = PythonOperator(
            task_id='python_operator',
            python_callable=build_recording_function(recorded_calls),
            provide_context=True,
            templates_dict={
                'a_templated_object': ClassWithCustomAttributes(
                    templated_string_attr="dag {{dag.dag_id}} ran on {{ds}}."
                )
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self.assertEqual(1, len(recorded_calls))
        self.assertTrue('templates_dict' in recorded_calls[0].kwargs)
        self.assertDictEqual(
            recorded_calls[0].kwargs['templates_dict'],
            {
                'a_templated_object': ClassWithCustomAttributes(
                    templated_string_attr="dag {} ran on {}.".format(self.dag.dag_id, DEFAULT_DATE.date())
                )
            }
        )

    def test_multiple_template_fields_rendering(self):
        """Test PythonOperator template field with multiple templated and non templated values"""
        recorded_calls = []
        ClassWithCustomAttributes.template_fields = ['templated_string_attr']

        task = PythonOperator(
            task_id='python_operator',
            python_callable=build_recording_function(recorded_calls),
            provide_context=True,
            templates_dict={
                'an_int': 4,
                'a_date': date(2019, 2, 18),
                'a_string': "this is a static string",
                'a_templated_string': "this is a templated string for dag {{dag.dag_id}}",
                'an_object': ClassWithCustomAttributes(
                    templated_string_attr="static string here",
                    static_string_attr='static string',
                    int_attr=5,
                    date_attr=date(2019, 2, 19)
                ),
                'a_templated_object': ClassWithCustomAttributes(
                    templated_string_attr="dag {{dag.dag_id}}",
                    static_string_attr="static_string",
                    int_attr=6,
                    date_attr=date(2019, 2, 20)
                )
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self.assertEqual(1, len(recorded_calls))
        self.assertTrue('templates_dict' in recorded_calls[0].kwargs)
        self.assertDictEqual(
            recorded_calls[0].kwargs['templates_dict'],
            {
                'an_int': 4,
                'a_date': date(2019, 2, 18),
                'a_string': "this is a static string",
                'a_templated_string': "this is a templated string for dag {}".format(self.dag.dag_id),
                'an_object': ClassWithCustomAttributes(
                    templated_string_attr="static string here",
                    static_string_attr='static string',
                    int_attr=5,
                    date_attr=date(2019, 2, 19)
                ),
                'a_templated_object': ClassWithCustomAttributes(
                    templated_string_attr="dag {}".format(self.dag.dag_id),
                    static_string_attr="static_string",
                    int_attr=6,
                    date_attr=date(2019, 2, 20)
                )
            }
        )

    def test_inner_attributes_are_rendered_when_marked_as_template_fields_only(self):
        recorded_calls = []
        ClassWithCustomAttributes.template_fields = ['templated_string_attr']

        task = PythonOperator(
            task_id='python_operator',
            python_callable=build_recording_function(recorded_calls),
            provide_context=True,
            templates_dict={
                'a_templated_object': ClassWithCustomAttributes(
                    templated_string_attr="dag {{dag.dag_id}} ran on {{ds}}.",
                    static_string_attr="dag {{dag.dag_id}} ran on {{ds}}."
                )
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self.assertEqual(1, len(recorded_calls))
        self.assertTrue('templates_dict' in recorded_calls[0].kwargs)
        self.assertDictEqual(
            recorded_calls[0].kwargs['templates_dict'],
            {
                'a_templated_object': ClassWithCustomAttributes(
                    templated_string_attr="dag {} ran on {}.".format(self.dag.dag_id, DEFAULT_DATE.date()),
                    static_string_attr="dag {{dag.dag_id}} ran on {{ds}}."
                )
            }
        )

    def test_inner_attributes_are_not_rendered_when_object_has_no_template_fields(self):
        recorded_calls = []

        task = PythonOperator(
            task_id='python_operator',
            python_callable=build_recording_function(recorded_calls),
            provide_context=True,
            templates_dict={
                'a_templated_string': "dag {{dag.dag_id}} ran on {{ds}}.",
                'a_non_templated_object': ClassWithCustomAttributes(
                    templated_string_attr="dag {{dag.dag_id}} ran on {{ds}}."
                )
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self.assertEqual(1, len(recorded_calls))
        self.assertTrue('templates_dict' in recorded_calls[0].kwargs)
        self.assertDictEqual(
            recorded_calls[0].kwargs['templates_dict'],
            {
                'a_templated_string': "dag {} ran on {}.".format(self.dag.dag_id, DEFAULT_DATE.date()),
                'a_non_templated_object': ClassWithCustomAttributes(
                    templated_string_attr="dag {{dag.dag_id}} ran on {{ds}}."
                )
            }
        )

    def test_template_can_be_rendered_recursively_on_deep_attributes(self):
        recorded_calls = []
        ClassWithCustomAttributes.template_fields = ['templated_string_attr', 'ref']

        object1 = ClassWithCustomAttributes(
            templated_string_attr="object 1 on dag {{dag.dag_id}}",
            ref=None
        )
        object2 = ClassWithCustomAttributes(
            templated_string_attr="object 2 on dag {{dag.dag_id}}",
            ref=object1
        )

        task = PythonOperator(
            task_id='python_operator',
            python_callable=build_recording_function(recorded_calls),
            provide_context=True,
            templates_dict={
                'a_templated_object': object2
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self.assertEqual(1, len(recorded_calls))
        self.assertTrue('templates_dict' in recorded_calls[0].kwargs)
        self.assertDictEqual(
            recorded_calls[0].kwargs['templates_dict'],
            {
                'a_templated_object': ClassWithCustomAttributes(
                    templated_string_attr="object 2 on dag {}".format(self.dag.dag_id),
                    ref=ClassWithCustomAttributes(
                        templated_string_attr="object 1 on dag {}".format(self.dag.dag_id),
                        ref=None
                    )
                )
            }
        )

    def test_nested_template_fields_can_be_defined_at_the_class_level(self):
        recorded_calls = []
        ClassWithCustomAttributes.template_fields = ['templated_string_attr']

        object1 = ClassWithCustomAttributes(
            templated_string_attr="object 1 on dag {{dag.dag_id}}"
        )
        object2 = ClassWithCustomAttributes(
            templated_string_attr="object 2 on dag {{dag.dag_id}}"
        )

        task = PythonOperator(
            task_id='python_operator',
            python_callable=build_recording_function(recorded_calls),
            provide_context=True,
            templates_dict={
                'an_object': object1,
                'another_object': object2
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self.assertEqual(1, len(recorded_calls))
        self.assertTrue('templates_dict' in recorded_calls[0].kwargs)
        self.assertDictEqual(
            recorded_calls[0].kwargs['templates_dict'],
            {
                'an_object': ClassWithCustomAttributes(
                    templated_string_attr="object 1 on dag {}".format(self.dag.dag_id)
                ),
                'another_object': ClassWithCustomAttributes(
                    templated_string_attr="object 2 on dag {}".format(self.dag.dag_id)
                )
            }
        )

    def test_nested_template_fields_can_be_defined_at_the_instance_level(self):
        recorded_calls = []

        object1 = ClassWithCustomAttributes(
            templated_string_attr="object 1 on dag {{dag.dag_id}}",
            template_fields=['templated_string_attr']
        )
        object2 = ClassWithCustomAttributes(
            templated_string_attr="object 2 on dag {{dag.dag_id}}",
        )

        task = PythonOperator(
            task_id='python_operator',
            python_callable=build_recording_function(recorded_calls),
            provide_context=True,
            templates_dict={
                'an_object': object1,
                'another_object': object2
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self.assertEqual(1, len(recorded_calls))
        self.assertTrue('templates_dict' in recorded_calls[0].kwargs)
        self.assertDictEqual(
            recorded_calls[0].kwargs['templates_dict'],
            {
                'an_object': ClassWithCustomAttributes(
                    templated_string_attr="object 1 on dag {}".format(self.dag.dag_id),
                    template_fields=['templated_string_attr']
                ),
                'another_object': ClassWithCustomAttributes(
                    templated_string_attr="object 2 on dag {{dag.dag_id}}"
                )
            }
        )

    def test_template_are_not_rendered_recursively_when_reference_is_not_a_template_field(self):
        recorded_calls = []
        ClassWithCustomAttributes.template_fields = ['templated_string_attr']

        object1 = ClassWithCustomAttributes(
            templated_string_attr="object 1 on dag {{dag.dag_id}}",
            ref=None
        )
        object2 = ClassWithCustomAttributes(
            templated_string_attr="object 2 on dag {{dag.dag_id}}",
            ref=object1
        )

        task = PythonOperator(
            task_id='python_operator',
            python_callable=build_recording_function(recorded_calls),
            provide_context=True,
            templates_dict={
                'a_templated_object': object2
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self.assertEqual(1, len(recorded_calls))
        self.assertTrue('templates_dict' in recorded_calls[0].kwargs)
        self.assertDictEqual(
            recorded_calls[0].kwargs['templates_dict'],
            {
                'a_templated_object': ClassWithCustomAttributes(
                    templated_string_attr="object 2 on dag {}".format(self.dag.dag_id),
                    ref=ClassWithCustomAttributes(
                        templated_string_attr="object 1 on dag {{dag.dag_id}}",
                        ref=None
                    )
                )
            }
        )

    def test_template_rendering_on_circular_references(self):
        """Test PythonOperator template field with objects having circular references"""
        recorded_calls = []
        ClassWithCustomAttributes.template_fields = ['templated_string_attr', 'ref']

        object1 = ClassWithCustomAttributes(
            templated_string_attr="object 1 on dag {{dag.dag_id}}"
        )
        object2 = ClassWithCustomAttributes(
            templated_string_attr="object 2 on dag {{dag.dag_id}}",
            ref=object1
        )
        setattr(object1, 'ref', object2)

        task = PythonOperator(
            task_id='python_operator',
            python_callable=build_recording_function(recorded_calls),
            provide_context=True,
            templates_dict={
                'an_object': object1,
                'another_object': object2
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self.assertEqual(1, len(recorded_calls))
        self.assertTrue('templates_dict' in recorded_calls[0].kwargs)

        resolved_dict = recorded_calls[0].kwargs['templates_dict']

        # template_dict is still referencing the same objects:
        self.assertEqual(id(object1), id(resolved_dict['an_object']))
        self.assertEqual(id(object2), id(resolved_dict['another_object']))
        # templated nested attributes have been resolved on template objects:
        self.assertEqual("object 1 on dag {}".format(self.dag.dag_id), object1.templated_string_attr)
        self.assertEqual("object 2 on dag {}".format(self.dag.dag_id), object2.templated_string_attr)

    def test_invalid_template_string(self):
        task = PythonOperator(
            task_id='python_operator',
            python_callable=lambda **kwargs: 'done!',
            provide_context=True,
            templates_dict={
                'a_templated_string': "dag {{invalid expression}}.",
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        with self.assertRaises(jinja2.exceptions.TemplateSyntaxError):
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_invalid_inner_attribute_template_string(self):
        ClassWithCustomAttributes.template_fields = ['templated_attribute']

        task = PythonOperator(
            task_id='python_operator',
            python_callable=lambda **kwargs: 'done!',
            provide_context=True,
            templates_dict={
                'a_templated_object': ClassWithCustomAttributes(
                    templated_attribute="dag {{invalid expression}}."
                ),
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        with self.assertRaises(jinja2.exceptions.TemplateSyntaxError):
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_template_rendering_fails_when_unknown_field_is_marked_as_template(self):
        ClassWithCustomAttributes.template_fields = ['unknown_attribute']

        task = PythonOperator(
            task_id='python_operator',
            python_callable=lambda **kwargs: 'done!',
            provide_context=True,
            templates_dict={
                'a_templated_object': ClassWithCustomAttributes(
                    static_string="a static string"
                ),
            },
            dag=self.dag)

        self.dag.create_dagrun(
            run_id='manual__' + DEFAULT_DATE.isoformat(),
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        with self.assertRaises(AttributeError) as expected_error:
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.assertRegexpMatches(
            str(expected_error.exception),
            "('ClassWithCustomAttributes' object|ClassWithCustomAttributes instance) has no attribute "
            "'unknown_attribute'"
        )


class ClassWithCustomAttributes:
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


class TestBranchOperator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self):
        self.dag = DAG('branch_operator_test',
                       default_args={
                           'owner': 'airflow',
                           'start_date': DEFAULT_DATE},
                       schedule_interval=INTERVAL)

        self.branch_1 = DummyOperator(task_id='branch_1', dag=self.dag)
        self.branch_2 = DummyOperator(task_id='branch_2', dag=self.dag)

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def test_without_dag_run(self):
        """This checks the defensive against non existent tasks in a dag run"""
        self.branch_op = BranchPythonOperator(task_id='make_choice',
                                              dag=self.dag,
                                              python_callable=lambda: 'branch_1')
        self.branch_1.set_upstream(self.branch_op)
        self.branch_2.set_upstream(self.branch_op)
        self.dag.clear()

        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        with create_session() as session:
            tis = session.query(TI).filter(
                TI.dag_id == self.dag.dag_id,
                TI.execution_date == DEFAULT_DATE
            )

            for ti in tis:
                if ti.task_id == 'make_choice':
                    self.assertEqual(ti.state, State.SUCCESS)
                elif ti.task_id == 'branch_1':
                    # should exist with state None
                    self.assertEqual(ti.state, State.NONE)
                elif ti.task_id == 'branch_2':
                    self.assertEqual(ti.state, State.SKIPPED)
                else:
                    raise Exception

    def test_branch_list_without_dag_run(self):
        """This checks if the BranchPythonOperator supports branching off to a list of tasks."""
        self.branch_op = BranchPythonOperator(task_id='make_choice',
                                              dag=self.dag,
                                              python_callable=lambda: ['branch_1', 'branch_2'])
        self.branch_1.set_upstream(self.branch_op)
        self.branch_2.set_upstream(self.branch_op)
        self.branch_3 = DummyOperator(task_id='branch_3', dag=self.dag)
        self.branch_3.set_upstream(self.branch_op)
        self.dag.clear()

        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        with create_session() as session:
            tis = session.query(TI).filter(
                TI.dag_id == self.dag.dag_id,
                TI.execution_date == DEFAULT_DATE
            )

            expected = {
                "make_choice": State.SUCCESS,
                "branch_1": State.NONE,
                "branch_2": State.NONE,
                "branch_3": State.SKIPPED,
            }

            for ti in tis:
                if ti.task_id in expected:
                    self.assertEqual(ti.state, expected[ti.task_id])
                else:
                    raise Exception

    def test_with_dag_run(self):
        self.branch_op = BranchPythonOperator(task_id='make_choice',
                                              dag=self.dag,
                                              python_callable=lambda: 'branch_1')

        self.branch_1.set_upstream(self.branch_op)
        self.branch_2.set_upstream(self.branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.NONE)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.SKIPPED)
            else:
                raise Exception

    def test_with_skip_in_branch_downstream_dependencies(self):
        self.branch_op = BranchPythonOperator(task_id='make_choice',
                                              dag=self.dag,
                                              python_callable=lambda: 'branch_1')

        self.branch_op >> self.branch_1 >> self.branch_2
        self.branch_op >> self.branch_2
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.NONE)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.NONE)
            else:
                raise Exception

    def test_with_skip_in_branch_downstream_dependencies2(self):
        self.branch_op = BranchPythonOperator(task_id='make_choice',
                                              dag=self.dag,
                                              python_callable=lambda: 'branch_2')

        self.branch_op >> self.branch_1 >> self.branch_2
        self.branch_op >> self.branch_2
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.SKIPPED)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.NONE)
            else:
                raise Exception


class TestShortCircuitOperator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def test_without_dag_run(self):
        """This checks the defensive against non existent tasks in a dag run"""
        value = False
        dag = DAG('shortcircuit_operator_test_without_dag_run',
                  default_args={
                      'owner': 'airflow',
                      'start_date': DEFAULT_DATE
                  },
                  schedule_interval=INTERVAL)
        short_op = ShortCircuitOperator(task_id='make_choice',
                                        dag=dag,
                                        python_callable=lambda: value)
        branch_1 = DummyOperator(task_id='branch_1', dag=dag)
        branch_1.set_upstream(short_op)
        branch_2 = DummyOperator(task_id='branch_2', dag=dag)
        branch_2.set_upstream(branch_1)
        upstream = DummyOperator(task_id='upstream', dag=dag)
        upstream.set_downstream(short_op)
        dag.clear()

        short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        with create_session() as session:
            tis = session.query(TI).filter(
                TI.dag_id == dag.dag_id,
                TI.execution_date == DEFAULT_DATE
            )

            for ti in tis:
                if ti.task_id == 'make_choice':
                    self.assertEqual(ti.state, State.SUCCESS)
                elif ti.task_id == 'upstream':
                    # should not exist
                    raise Exception
                elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                    self.assertEqual(ti.state, State.SKIPPED)
                else:
                    raise Exception

            value = True
            dag.clear()

            short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
            for ti in tis:
                if ti.task_id == 'make_choice':
                    self.assertEqual(ti.state, State.SUCCESS)
                elif ti.task_id == 'upstream':
                    # should not exist
                    raise Exception
                elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                    self.assertEqual(ti.state, State.NONE)
                else:
                    raise Exception

    def test_with_dag_run(self):
        value = False
        dag = DAG('shortcircuit_operator_test_with_dag_run',
                  default_args={
                      'owner': 'airflow',
                      'start_date': DEFAULT_DATE
                  },
                  schedule_interval=INTERVAL)
        short_op = ShortCircuitOperator(task_id='make_choice',
                                        dag=dag,
                                        python_callable=lambda: value)
        branch_1 = DummyOperator(task_id='branch_1', dag=dag)
        branch_1.set_upstream(short_op)
        branch_2 = DummyOperator(task_id='branch_2', dag=dag)
        branch_2.set_upstream(branch_1)
        upstream = DummyOperator(task_id='upstream', dag=dag)
        upstream.set_downstream(short_op)
        dag.clear()

        logging.error("Tasks %s", dag.tasks)
        dr = dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        upstream.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 4)
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'upstream':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.SKIPPED)
            else:
                raise Exception

        value = True
        dag.clear()
        dr.verify_integrity()
        upstream.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        short_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 4)
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'upstream':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1' or ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.NONE)
            else:
                raise Exception
