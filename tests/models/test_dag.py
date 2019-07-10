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
import logging
import os
import re
import unittest
from unittest.mock import patch
import uuid
from tempfile import NamedTemporaryFile

import jinja2
import pendulum
import six

from airflow import models, settings, configuration
from airflow.exceptions import AirflowException, AirflowDagCycleException
from airflow.models import DAG, DagModel, TaskInstance as TI
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.weight_rule import WeightRule
from tests.models import DEFAULT_DATE


class DagTest(unittest.TestCase):

    def test_params_not_passed_is_empty_dict(self):
        """
        Test that when 'params' is _not_ passed to a new Dag, that the params
        attribute is set to an empty dictionary.
        """
        dag = models.DAG('test-dag')

        self.assertEqual(dict, type(dag.params))
        self.assertEqual(0, len(dag.params))

    def test_params_passed_and_params_in_default_args_no_override(self):
        """
        Test that when 'params' exists as a key passed to the default_args dict
        in addition to params being passed explicitly as an argument to the
        dag, that the 'params' key of the default_args dict is merged with the
        dict of the params argument.
        """
        params1 = {'parameter1': 1}
        params2 = {'parameter2': 2}

        dag = models.DAG('test-dag',
                         default_args={'params': params1},
                         params=params2)

        params_combined = params1.copy()
        params_combined.update(params2)
        self.assertEqual(params_combined, dag.params)

    def test_dag_as_context_manager(self):
        """
        Test DAG as a context manager.
        When used as a context manager, Operators are automatically added to
        the DAG (unless they specify a different DAG)
        """
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})
        dag2 = DAG(
            'dag2',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner2'})

        with dag:
            op1 = DummyOperator(task_id='op1')
            op2 = DummyOperator(task_id='op2', dag=dag2)

        self.assertIs(op1.dag, dag)
        self.assertEqual(op1.owner, 'owner1')
        self.assertIs(op2.dag, dag2)
        self.assertEqual(op2.owner, 'owner2')

        with dag2:
            op3 = DummyOperator(task_id='op3')

        self.assertIs(op3.dag, dag2)
        self.assertEqual(op3.owner, 'owner2')

        with dag:
            with dag2:
                op4 = DummyOperator(task_id='op4')
            op5 = DummyOperator(task_id='op5')

        self.assertIs(op4.dag, dag2)
        self.assertIs(op5.dag, dag)
        self.assertEqual(op4.owner, 'owner2')
        self.assertEqual(op5.owner, 'owner1')

        with DAG('creating_dag_in_cm', start_date=DEFAULT_DATE) as dag:
            DummyOperator(task_id='op6')

        self.assertEqual(dag.dag_id, 'creating_dag_in_cm')
        self.assertEqual(dag.tasks[0].task_id, 'op6')

        with dag:
            with dag:
                op7 = DummyOperator(task_id='op7')
            op8 = DummyOperator(task_id='op8')
        op9 = DummyOperator(task_id='op8')
        op9.dag = dag2

        self.assertEqual(op7.dag, dag)
        self.assertEqual(op8.dag, dag)
        self.assertEqual(op9.dag, dag2)

    def test_dag_topological_sort(self):
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        # A -> B
        # A -> C -> D
        # ordered: B, D, C, A or D, B, C, A or D, C, B, A
        with dag:
            op1 = DummyOperator(task_id='A')
            op2 = DummyOperator(task_id='B')
            op3 = DummyOperator(task_id='C')
            op4 = DummyOperator(task_id='D')
            op1.set_upstream([op2, op3])
            op3.set_upstream(op4)

        topological_list = dag.topological_sort()
        logging.info(topological_list)

        tasks = [op2, op3, op4]
        self.assertTrue(topological_list[0] in tasks)
        tasks.remove(topological_list[0])
        self.assertTrue(topological_list[1] in tasks)
        tasks.remove(topological_list[1])
        self.assertTrue(topological_list[2] in tasks)
        tasks.remove(topological_list[2])
        self.assertTrue(topological_list[3] == op1)

        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        # C -> (A u B) -> D
        # C -> E
        # ordered: E | D, A | B, C
        with dag:
            op1 = DummyOperator(task_id='A')
            op2 = DummyOperator(task_id='B')
            op3 = DummyOperator(task_id='C')
            op4 = DummyOperator(task_id='D')
            op5 = DummyOperator(task_id='E')
            op1.set_downstream(op3)
            op2.set_downstream(op3)
            op1.set_upstream(op4)
            op2.set_upstream(op4)
            op5.set_downstream(op3)

        topological_list = dag.topological_sort()
        logging.info(topological_list)

        set1 = [op4, op5]
        self.assertTrue(topological_list[0] in set1)
        set1.remove(topological_list[0])

        set2 = [op1, op2]
        set2.extend(set1)
        self.assertTrue(topological_list[1] in set2)
        set2.remove(topological_list[1])

        self.assertTrue(topological_list[2] in set2)
        set2.remove(topological_list[2])

        self.assertTrue(topological_list[3] in set2)

        self.assertTrue(topological_list[4] == op3)

        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        self.assertEqual(tuple(), dag.topological_sort())

    def test_dag_naive_start_date_string(self):
        DAG('DAG', default_args={'start_date': '2019-06-01'})

    def test_dag_naive_start_end_dates_strings(self):
        DAG('DAG', default_args={'start_date': '2019-06-01', 'end_date': '2019-06-05'})

    def test_dag_start_date_propagates_to_end_date(self):
        """
        Tests that a start_date string with a timezone and an end_date string without a timezone
        are accepted and that the timezone from the start carries over the end

        This test is a little indirect, it works by setting start and end equal except for the
        timezone and then testing for equality after the DAG construction.  They'll be equal
        only if the same timezone was applied to both.

        An explicit check the the `tzinfo` attributes for both are the same is an extra check.
        """
        dag = DAG('DAG', default_args={'start_date': '2019-06-05T00:00:00+05:00',
                                       'end_date': '2019-06-05T00:00:00'})
        self.assertEqual(dag.default_args['start_date'], dag.default_args['end_date'])
        self.assertEqual(dag.default_args['start_date'].tzinfo, dag.default_args['end_date'].tzinfo)

    def test_dag_naive_default_args_start_date(self):
        dag = DAG('DAG', default_args={'start_date': datetime.datetime(2018, 1, 1)})
        self.assertEqual(dag.timezone, settings.TIMEZONE)
        dag = DAG('DAG', start_date=datetime.datetime(2018, 1, 1))
        self.assertEqual(dag.timezone, settings.TIMEZONE)

    def test_dag_none_default_args_start_date(self):
        """
        Tests if a start_date of None in default_args
        works.
        """
        dag = DAG('DAG', default_args={'start_date': None})
        self.assertEqual(dag.timezone, settings.TIMEZONE)

    def test_dag_task_priority_weight_total(self):
        width = 5
        depth = 5
        weight = 5
        pattern = re.compile('stage(\\d*).(\\d*)')
        # Fully connected parallel tasks. i.e. every task at each parallel
        # stage is dependent on every task in the previous stage.
        # Default weight should be calculated using downstream descendants
        with DAG('dag', start_date=DEFAULT_DATE,
                 default_args={'owner': 'owner1'}) as dag:
            pipeline = [
                [DummyOperator(
                    task_id='stage{}.{}'.format(i, j), priority_weight=weight)
                    for j in range(0, width)] for i in range(0, depth)
            ]
            for d, stage in enumerate(pipeline):
                if d == 0:
                    continue
                for current_task in stage:
                    for prev_task in pipeline[d - 1]:
                        current_task.set_upstream(prev_task)

            for task in six.itervalues(dag.task_dict):
                match = pattern.match(task.task_id)
                task_depth = int(match.group(1))
                # the sum of each stages after this task + itself
                correct_weight = ((depth - (task_depth + 1)) * width + 1) * weight

                calculated_weight = task.priority_weight_total
                self.assertEqual(calculated_weight, correct_weight)

        # Same test as above except use 'upstream' for weight calculation
        weight = 3
        with DAG('dag', start_date=DEFAULT_DATE,
                 default_args={'owner': 'owner1'}) as dag:
            pipeline = [
                [DummyOperator(
                    task_id='stage{}.{}'.format(i, j), priority_weight=weight,
                    weight_rule=WeightRule.UPSTREAM)
                    for j in range(0, width)] for i in range(0, depth)
            ]
            for d, stage in enumerate(pipeline):
                if d == 0:
                    continue
                for current_task in stage:
                    for prev_task in pipeline[d - 1]:
                        current_task.set_upstream(prev_task)

            for task in six.itervalues(dag.task_dict):
                match = pattern.match(task.task_id)
                task_depth = int(match.group(1))
                # the sum of each stages after this task + itself
                correct_weight = (task_depth * width + 1) * weight

                calculated_weight = task.priority_weight_total
                self.assertEqual(calculated_weight, correct_weight)

        # Same test as above except use 'absolute' for weight calculation
        weight = 10
        with DAG('dag', start_date=DEFAULT_DATE,
                 default_args={'owner': 'owner1'}) as dag:
            pipeline = [
                [DummyOperator(
                    task_id='stage{}.{}'.format(i, j), priority_weight=weight,
                    weight_rule=WeightRule.ABSOLUTE)
                    for j in range(0, width)] for i in range(0, depth)
            ]
            for d, stage in enumerate(pipeline):
                if d == 0:
                    continue
                for current_task in stage:
                    for prev_task in pipeline[d - 1]:
                        current_task.set_upstream(prev_task)

            for task in six.itervalues(dag.task_dict):
                match = pattern.match(task.task_id)
                task_depth = int(match.group(1))
                # the sum of each stages after this task + itself
                correct_weight = weight

                calculated_weight = task.priority_weight_total
                self.assertEqual(calculated_weight, correct_weight)

        # Test if we enter an invalid weight rule
        with DAG('dag', start_date=DEFAULT_DATE,
                 default_args={'owner': 'owner1'}) as dag:
            with self.assertRaises(AirflowException):
                DummyOperator(task_id='should_fail', weight_rule='no rule')

    def test_get_num_task_instances(self):
        test_dag_id = 'test_get_num_task_instances_dag'
        test_task_id = 'task_1'

        test_dag = DAG(dag_id=test_dag_id, start_date=DEFAULT_DATE)
        test_task = DummyOperator(task_id=test_task_id, dag=test_dag)

        ti1 = TI(task=test_task, execution_date=DEFAULT_DATE)
        ti1.state = None
        ti2 = TI(task=test_task, execution_date=DEFAULT_DATE + datetime.timedelta(days=1))
        ti2.state = State.RUNNING
        ti3 = TI(task=test_task, execution_date=DEFAULT_DATE + datetime.timedelta(days=2))
        ti3.state = State.QUEUED
        ti4 = TI(task=test_task, execution_date=DEFAULT_DATE + datetime.timedelta(days=3))
        ti4.state = State.RUNNING
        session = settings.Session()
        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)
        session.merge(ti4)
        session.commit()

        self.assertEqual(
            0,
            DAG.get_num_task_instances(test_dag_id, ['fakename'], session=session)
        )
        self.assertEqual(
            4,
            DAG.get_num_task_instances(test_dag_id, [test_task_id], session=session)
        )
        self.assertEqual(
            4,
            DAG.get_num_task_instances(
                test_dag_id, ['fakename', test_task_id], session=session)
        )
        self.assertEqual(
            1,
            DAG.get_num_task_instances(
                test_dag_id, [test_task_id], states=[None], session=session)
        )
        self.assertEqual(
            2,
            DAG.get_num_task_instances(
                test_dag_id, [test_task_id], states=[State.RUNNING], session=session)
        )
        self.assertEqual(
            3,
            DAG.get_num_task_instances(
                test_dag_id, [test_task_id],
                states=[None, State.RUNNING], session=session)
        )
        self.assertEqual(
            4,
            DAG.get_num_task_instances(
                test_dag_id, [test_task_id],
                states=[None, State.QUEUED, State.RUNNING], session=session)
        )
        session.close()

    def test_render_template_field(self):
        """Tests if render_template from a field works"""

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE)

        with dag:
            task = DummyOperator(task_id='op1')

        result = task.render_template('', '{{ foo }}', dict(foo='bar'))
        self.assertEqual(result, 'bar')

    def test_render_template_field_undefined(self):
        """Tests if render_template from a field works"""

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE)

        with dag:
            task = DummyOperator(task_id='op1')

        result = task.render_template('', '{{ foo }}', {})
        self.assertEqual(result, '')

    def test_render_template_field_undefined_strict(self):
        """Tests if render_template from a field works"""

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE,
                  template_undefined=jinja2.StrictUndefined)

        with dag:
            task = DummyOperator(task_id='op1')

        with self.assertRaises(jinja2.UndefinedError):
            task.render_template('', '{{ foo }}', {})

    def test_render_template_list_field(self):
        """Tests if render_template from a list field works"""

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE)

        with dag:
            task = DummyOperator(task_id='op1')

        self.assertListEqual(
            task.render_template('', ['{{ foo }}_1', '{{ foo }}_2'], {'foo': 'bar'}),
            ['bar_1', 'bar_2']
        )

    def test_render_template_tuple_field(self):
        """Tests if render_template from a tuple field works"""

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE)

        with dag:
            task = DummyOperator(task_id='op1')

        # tuple is replaced by a list
        self.assertListEqual(
            task.render_template('', ('{{ foo }}_1', '{{ foo }}_2'), {'foo': 'bar'}),
            ['bar_1', 'bar_2']
        )

    def test_render_template_dict_field(self):
        """Tests if render_template from a dict field works"""

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE)

        with dag:
            task = DummyOperator(task_id='op1')

        self.assertDictEqual(
            task.render_template('', {'key1': '{{ foo }}_1', 'key2': '{{ foo }}_2'}, {'foo': 'bar'}),
            {'key1': 'bar_1', 'key2': 'bar_2'}
        )

    def test_render_template_dict_field_with_templated_keys(self):
        """Tests if render_template from a dict field works as expected:
        dictionary keys are not templated"""

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE)

        with dag:
            task = DummyOperator(task_id='op1')

        self.assertDictEqual(
            task.render_template('', {'key_{{ foo }}_1': 1, 'key_2': '{{ foo }}_2'}, {'foo': 'bar'}),
            {'key_{{ foo }}_1': 1, 'key_2': 'bar_2'}
        )

    def test_render_template_date_field(self):
        """Tests if render_template from a date field works"""

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE)

        with dag:
            task = DummyOperator(task_id='op1')

        self.assertEqual(
            task.render_template('', datetime.date(2018, 12, 6), {'foo': 'bar'}),
            datetime.date(2018, 12, 6)
        )

    def test_render_template_datetime_field(self):
        """Tests if render_template from a datetime field works"""

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE)

        with dag:
            task = DummyOperator(task_id='op1')

        self.assertEqual(
            task.render_template('', datetime.datetime(2018, 12, 6, 10, 55), {'foo': 'bar'}),
            datetime.datetime(2018, 12, 6, 10, 55)
        )

    def test_render_template_UUID_field(self):
        """Tests if render_template from a UUID field works"""

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE)

        with dag:
            task = DummyOperator(task_id='op1')

        random_uuid = uuid.uuid4()
        self.assertIs(
            task.render_template('', random_uuid, {'foo': 'bar'}),
            random_uuid
        )

    def test_render_template_object_field(self):
        """Tests if render_template from an object field works"""

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE)

        with dag:
            task = DummyOperator(task_id='op1')

        test_object = object()
        self.assertIs(
            task.render_template('', test_object, {'foo': 'bar'}),
            test_object
        )

    def test_render_template_field_macro(self):
        """ Tests if render_template from a field works,
            if a custom filter was defined"""

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE,
                  user_defined_macros=dict(foo='bar'))

        with dag:
            task = DummyOperator(task_id='op1')

        result = task.render_template('', '{{ foo }}', dict())
        self.assertEqual(result, 'bar')

    def test_render_template_numeric_field(self):
        """ Tests if render_template from a field works,
            if a custom filter was defined"""

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE,
                  user_defined_macros=dict(foo='bar'))

        with dag:
            task = DummyOperator(task_id='op1')

        result = task.render_template('', 1, dict())
        self.assertEqual(result, 1)

    def test_user_defined_filters(self):
        def jinja_udf(name):
            return 'Hello %s' % name

        dag = models.DAG('test-dag',
                         start_date=DEFAULT_DATE,
                         user_defined_filters=dict(hello=jinja_udf))
        jinja_env = dag.get_template_env()

        self.assertIn('hello', jinja_env.filters)
        self.assertEqual(jinja_env.filters['hello'], jinja_udf)

    def test_render_template_field_filter(self):
        """ Tests if render_template from a field works,
            if a custom filter was defined"""

        def jinja_udf(name):
            return 'Hello %s' % name

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE,
                  user_defined_filters=dict(hello=jinja_udf))

        with dag:
            task = DummyOperator(task_id='op1')

        result = task.render_template('', "{{ 'world' | hello}}", dict())
        self.assertEqual(result, 'Hello world')

    def test_resolve_template_files_value(self):

        with NamedTemporaryFile(suffix='.template') as f:
            f.write('{{ ds }}'.encode('utf8'))
            f.flush()
            template_dir = os.path.dirname(f.name)
            template_file = os.path.basename(f.name)

            dag = DAG('test-dag',
                      start_date=DEFAULT_DATE,
                      template_searchpath=template_dir)

            with dag:
                task = DummyOperator(task_id='op1')

            task.test_field = template_file
            task.template_fields = ('test_field',)
            task.template_ext = ('.template',)
            task.resolve_template_files()

        self.assertEqual(task.test_field, '{{ ds }}')

    def test_resolve_template_files_list(self):

        with NamedTemporaryFile(suffix='.template') as f:
            f = NamedTemporaryFile(suffix='.template')
            f.write('{{ ds }}'.encode('utf8'))
            f.flush()
            template_dir = os.path.dirname(f.name)
            template_file = os.path.basename(f.name)

            dag = DAG('test-dag',
                      start_date=DEFAULT_DATE,
                      template_searchpath=template_dir)

            with dag:
                task = DummyOperator(task_id='op1')

            task.test_field = [template_file, 'some_string']
            task.template_fields = ('test_field',)
            task.template_ext = ('.template',)
            task.resolve_template_files()

        self.assertEqual(task.test_field, ['{{ ds }}', 'some_string'])

    def test_cycle(self):
        # test empty
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        self.assertFalse(dag.test_cycle())

        # test single task
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        with dag:
            opA = DummyOperator(task_id='A')

        self.assertFalse(dag.test_cycle())

        # test no cycle
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        # A -> B -> C
        #      B -> D
        # E -> F
        with dag:
            opA = DummyOperator(task_id='A')
            opB = DummyOperator(task_id='B')
            opC = DummyOperator(task_id='C')
            opD = DummyOperator(task_id='D')
            opE = DummyOperator(task_id='E')
            opF = DummyOperator(task_id='F')
            opA.set_downstream(opB)
            opB.set_downstream(opC)
            opB.set_downstream(opD)
            opE.set_downstream(opF)

        self.assertFalse(dag.test_cycle())

        # test self loop
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        # A -> A
        with dag:
            opA = DummyOperator(task_id='A')
            opA.set_downstream(opA)

        with self.assertRaises(AirflowDagCycleException):
            dag.test_cycle()

        # test downstream self loop
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        # A -> B -> C -> D -> E -> E
        with dag:
            opA = DummyOperator(task_id='A')
            opB = DummyOperator(task_id='B')
            opC = DummyOperator(task_id='C')
            opD = DummyOperator(task_id='D')
            opE = DummyOperator(task_id='E')
            opA.set_downstream(opB)
            opB.set_downstream(opC)
            opC.set_downstream(opD)
            opD.set_downstream(opE)
            opE.set_downstream(opE)

        with self.assertRaises(AirflowDagCycleException):
            dag.test_cycle()

        # large loop
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        # A -> B -> C -> D -> E -> A
        with dag:
            opA = DummyOperator(task_id='A')
            opB = DummyOperator(task_id='B')
            opC = DummyOperator(task_id='C')
            opD = DummyOperator(task_id='D')
            opE = DummyOperator(task_id='E')
            opA.set_downstream(opB)
            opB.set_downstream(opC)
            opC.set_downstream(opD)
            opD.set_downstream(opE)
            opE.set_downstream(opA)

        with self.assertRaises(AirflowDagCycleException):
            dag.test_cycle()

        # test arbitrary loop
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        # E-> A -> B -> F -> A
        #       -> C -> F
        with dag:
            opA = DummyOperator(task_id='A')
            opB = DummyOperator(task_id='B')
            opC = DummyOperator(task_id='C')
            opD = DummyOperator(task_id='D')
            opE = DummyOperator(task_id='E')
            opF = DummyOperator(task_id='F')
            opA.set_downstream(opB)
            opA.set_downstream(opC)
            opE.set_downstream(opA)
            opC.set_downstream(opF)
            opB.set_downstream(opF)
            opF.set_downstream(opA)

        with self.assertRaises(AirflowDagCycleException):
            dag.test_cycle()

    def test_following_previous_schedule(self):
        """
        Make sure DST transitions are properly observed
        """
        local_tz = pendulum.timezone('Europe/Zurich')
        start = local_tz.convert(datetime.datetime(2018, 10, 28, 2, 55),
                                 dst_rule=pendulum.PRE_TRANSITION)
        self.assertEqual(start.isoformat(), "2018-10-28T02:55:00+02:00",
                         "Pre-condition: start date is in DST")

        utc = timezone.convert_to_utc(start)

        dag = DAG('tz_dag', start_date=start, schedule_interval='*/5 * * * *')
        _next = dag.following_schedule(utc)
        next_local = local_tz.convert(_next)

        self.assertEqual(_next.isoformat(), "2018-10-28T01:00:00+00:00")
        self.assertEqual(next_local.isoformat(), "2018-10-28T02:00:00+01:00")

        prev = dag.previous_schedule(utc)
        prev_local = local_tz.convert(prev)

        self.assertEqual(prev_local.isoformat(), "2018-10-28T02:50:00+02:00")

        prev = dag.previous_schedule(_next)
        prev_local = local_tz.convert(prev)

        self.assertEqual(prev_local.isoformat(), "2018-10-28T02:55:00+02:00")
        self.assertEqual(prev, utc)

    def test_following_previous_schedule_daily_dag_CEST_to_CET(self):
        """
        Make sure DST transitions are properly observed
        """
        local_tz = pendulum.timezone('Europe/Zurich')
        start = local_tz.convert(datetime.datetime(2018, 10, 27, 3),
                                 dst_rule=pendulum.PRE_TRANSITION)

        utc = timezone.convert_to_utc(start)

        dag = DAG('tz_dag', start_date=start, schedule_interval='0 3 * * *')

        prev = dag.previous_schedule(utc)
        prev_local = local_tz.convert(prev)

        self.assertEqual(prev_local.isoformat(), "2018-10-26T03:00:00+02:00")
        self.assertEqual(prev.isoformat(), "2018-10-26T01:00:00+00:00")

        _next = dag.following_schedule(utc)
        next_local = local_tz.convert(_next)

        self.assertEqual(next_local.isoformat(), "2018-10-28T03:00:00+01:00")
        self.assertEqual(_next.isoformat(), "2018-10-28T02:00:00+00:00")

        prev = dag.previous_schedule(_next)
        prev_local = local_tz.convert(prev)

        self.assertEqual(prev_local.isoformat(), "2018-10-27T03:00:00+02:00")
        self.assertEqual(prev.isoformat(), "2018-10-27T01:00:00+00:00")

    def test_following_previous_schedule_daily_dag_CET_to_CEST(self):
        """
        Make sure DST transitions are properly observed
        """
        local_tz = pendulum.timezone('Europe/Zurich')
        start = local_tz.convert(datetime.datetime(2018, 3, 25, 2),
                                 dst_rule=pendulum.PRE_TRANSITION)

        utc = timezone.convert_to_utc(start)

        dag = DAG('tz_dag', start_date=start, schedule_interval='0 3 * * *')

        prev = dag.previous_schedule(utc)
        prev_local = local_tz.convert(prev)

        self.assertEqual(prev_local.isoformat(), "2018-03-24T03:00:00+01:00")
        self.assertEqual(prev.isoformat(), "2018-03-24T02:00:00+00:00")

        _next = dag.following_schedule(utc)
        next_local = local_tz.convert(_next)

        self.assertEqual(next_local.isoformat(), "2018-03-25T03:00:00+02:00")
        self.assertEqual(_next.isoformat(), "2018-03-25T01:00:00+00:00")

        prev = dag.previous_schedule(_next)
        prev_local = local_tz.convert(prev)

        self.assertEqual(prev_local.isoformat(), "2018-03-24T03:00:00+01:00")
        self.assertEqual(prev.isoformat(), "2018-03-24T02:00:00+00:00")

    @patch('airflow.models.dag.timezone.utcnow')
    def test_sync_to_db(self, mock_now):
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
        )
        with dag:
            DummyOperator(task_id='task', owner='owner1')
            SubDagOperator(
                task_id='subtask',
                owner='owner2',
                subdag=DAG(
                    'dag.subtask',
                    start_date=DEFAULT_DATE,
                )
            )
        now = datetime.datetime.utcnow().replace(tzinfo=pendulum.timezone('UTC'))
        mock_now.return_value = now
        session = settings.Session()
        dag.sync_to_db(session=session)

        orm_dag = session.query(DagModel).filter(DagModel.dag_id == 'dag').one()
        self.assertEqual(set(orm_dag.owners.split(', ')), {'owner1', 'owner2'})
        self.assertEqual(orm_dag.last_scheduler_run, now)
        self.assertTrue(orm_dag.is_active)
        self.assertIsNone(orm_dag.default_view)
        self.assertEqual(orm_dag.get_default_view(),
                         configuration.conf.get('webserver', 'dag_default_view').lower())
        self.assertEqual(orm_dag.safe_dag_id, 'dag')

        orm_subdag = session.query(DagModel).filter(
            DagModel.dag_id == 'dag.subtask').one()
        self.assertEqual(set(orm_subdag.owners.split(', ')), {'owner1', 'owner2'})
        self.assertEqual(orm_subdag.last_scheduler_run, now)
        self.assertTrue(orm_subdag.is_active)
        self.assertEqual(orm_subdag.safe_dag_id, 'dag__dot__subtask')
        self.assertEqual(orm_subdag.fileloc, orm_dag.fileloc)

    @patch('airflow.models.dag.timezone.utcnow')
    def test_sync_to_db_default_view(self, mock_now):
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_view="graph",
        )
        with dag:
            DummyOperator(task_id='task', owner='owner1')
            SubDagOperator(
                task_id='subtask',
                owner='owner2',
                subdag=DAG(
                    'dag.subtask',
                    start_date=DEFAULT_DATE,
                )
            )
        now = datetime.datetime.utcnow().replace(tzinfo=pendulum.timezone('UTC'))
        mock_now.return_value = now
        session = settings.Session()
        dag.sync_to_db(session=session)

        orm_dag = session.query(DagModel).filter(DagModel.dag_id == 'dag').one()
        self.assertIsNotNone(orm_dag.default_view)
        self.assertEqual(orm_dag.get_default_view(), "graph")

    @patch('airflow.models.dag.DagBag')
    def test_is_paused_subdag(self, mock_dag_bag):
        subdag_id = 'dag.subdag'
        subdag = DAG(
            subdag_id,
            start_date=DEFAULT_DATE,
        )
        with subdag:
            DummyOperator(
                task_id='dummy_task',
            )

        dag_id = 'dag'
        dag = DAG(
            dag_id,
            start_date=DEFAULT_DATE,
        )

        with dag:
            SubDagOperator(
                task_id='subdag',
                subdag=subdag
            )

        mock_dag_bag.return_value.get_dag.return_value = dag

        session = settings.Session()
        dag.sync_to_db(session=session)

        unpaused_dags = session.query(
            DagModel
        ).filter(
            DagModel.dag_id.in_([subdag_id, dag_id]),
        ).filter(
            DagModel.is_paused.is_(False)
        ).count()

        self.assertEqual(2, unpaused_dags)

        DagModel.get_dagmodel(dag.dag_id).set_is_paused(is_paused=True)

        paused_dags = session.query(
            DagModel
        ).filter(
            DagModel.dag_id.in_([subdag_id, dag_id]),
        ).filter(
            DagModel.is_paused.is_(True)
        ).count()

        self.assertEqual(2, paused_dags)

    def test_existing_dag_is_paused_upon_creation(self):
        dag = DAG(
            'dag'
        )
        session = settings.Session()
        dag.sync_to_db(session=session)
        orm_dag = session.query(DagModel).filter(DagModel.dag_id == 'dag').one()
        self.assertFalse(orm_dag.is_paused)
        dag = DAG(
            'dag',
            is_paused_upon_creation=True
        )
        dag.sync_to_db(session=session)
        orm_dag = session.query(DagModel).filter(DagModel.dag_id == 'dag').one()
        # Since the dag existed before, it should not follow the pause flag upon creation
        self.assertFalse(orm_dag.is_paused)

    def test_new_dag_is_paused_upon_creation(self):
        dag = DAG(
            'new_nonexisting_dag',
            is_paused_upon_creation=True
        )
        session = settings.Session()
        dag.sync_to_db(session=session)

        orm_dag = session.query(DagModel).filter(DagModel.dag_id == 'new_nonexisting_dag').one()
        # Since the dag didn't exist before, it should follow the pause flag upon creation
        self.assertTrue(orm_dag.is_paused)

    def test_dag_naive_default_args_start_date_with_timezone(self):
        local_tz = pendulum.timezone('Europe/Zurich')
        default_args = {'start_date': datetime.datetime(2018, 1, 1, tzinfo=local_tz)}

        dag = DAG('DAG', default_args=default_args)
        self.assertEqual(dag.timezone.name, local_tz.name)

        dag = DAG('DAG', default_args=default_args)
        self.assertEqual(dag.timezone.name, local_tz.name)
