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
import io
import logging
import os
import pickle
import re
import unittest
from contextlib import redirect_stdout
from tempfile import NamedTemporaryFile
from unittest import mock
from unittest.mock import patch

import pendulum
from dateutil.relativedelta import relativedelta
from pendulum import utcnow

from airflow import models, settings
from airflow.configuration import conf
from airflow.exceptions import AirflowDagCycleException, AirflowException, DuplicateTaskIdFound
from airflow.jobs.scheduler_job import DagFileProcessor
from airflow.models import DAG, DagModel, DagRun, TaskFail, TaskInstance as TI
from airflow.models.baseoperator import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils import timezone
from airflow.utils.file import list_py_file_paths
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.timezone import datetime as datetime_tz
from airflow.utils.weight_rule import WeightRule
from tests.models import DEFAULT_DATE


class TestDag(unittest.TestCase):

    @staticmethod
    def _clean_up(dag_id: str):
        if os.environ.get('KUBERNETES_VERSION') is not None:
            return
        with create_session() as session:
            session.query(DagRun).filter(
                DagRun.dag_id == dag_id).delete(
                synchronize_session=False)
            session.query(TI).filter(
                TI.dag_id == dag_id).delete(
                synchronize_session=False)
            session.query(TaskFail).filter(
                TaskFail.dag_id == dag_id).delete(
                synchronize_session=False)

    @staticmethod
    def _occur_before(a, b, list_):
        """
        Assert that a occurs before b in the list.
        """
        a_index = -1
        b_index = -1
        for i, e in enumerate(list_):
            if e.task_id == a:
                a_index = i
            if e.task_id == b:
                b_index = i
        return 0 <= a_index < b_index

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

    def test_dag_topological_sort_include_subdag_tasks(self):
        child_dag = DAG(
            'parent_dag.child_dag',
            schedule_interval='@daily',
            start_date=DEFAULT_DATE,
        )

        with child_dag:
            DummyOperator(task_id='a_child')
            DummyOperator(task_id='b_child')

        parent_dag = DAG(
            'parent_dag',
            schedule_interval='@daily',
            start_date=DEFAULT_DATE,
        )

        # a_parent -> child_dag -> (a_child | b_child) -> b_parent
        with parent_dag:
            op1 = DummyOperator(task_id='a_parent')
            op2 = SubDagOperator(task_id='child_dag', subdag=child_dag)
            op3 = DummyOperator(task_id='b_parent')

            op1 >> op2 >> op3

        topological_list = parent_dag.topological_sort(include_subdag_tasks=True)

        self.assertTrue(self._occur_before('a_parent', 'child_dag', topological_list))
        self.assertTrue(self._occur_before('child_dag', 'a_child', topological_list))
        self.assertTrue(self._occur_before('child_dag', 'b_child', topological_list))
        self.assertTrue(self._occur_before('a_child', 'b_parent', topological_list))
        self.assertTrue(self._occur_before('b_child', 'b_parent', topological_list))

    def test_dag_topological_sort1(self):
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

    def test_dag_topological_sort2(self):
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

    def test_dag_topological_sort_dag_without_tasks(self):
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

        An explicit check the `tzinfo` attributes for both are the same is an extra check.
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
            for i, stage in enumerate(pipeline):
                if i == 0:
                    continue
                for current_task in stage:
                    for prev_task in pipeline[i - 1]:
                        current_task.set_upstream(prev_task)

            for task in dag.task_dict.values():
                match = pattern.match(task.task_id)
                task_depth = int(match.group(1))
                # the sum of each stages after this task + itself
                correct_weight = ((depth - (task_depth + 1)) * width + 1) * weight

                calculated_weight = task.priority_weight_total
                self.assertEqual(calculated_weight, correct_weight)

    def test_dag_task_priority_weight_total_using_upstream(self):
        # Same test as above except use 'upstream' for weight calculation
        weight = 3
        width = 5
        depth = 5
        pattern = re.compile('stage(\\d*).(\\d*)')
        with DAG('dag', start_date=DEFAULT_DATE,
                 default_args={'owner': 'owner1'}) as dag:
            pipeline = [
                [DummyOperator(
                    task_id='stage{}.{}'.format(i, j), priority_weight=weight,
                    weight_rule=WeightRule.UPSTREAM)
                    for j in range(0, width)] for i in range(0, depth)
            ]
            for i, stage in enumerate(pipeline):
                if i == 0:
                    continue
                for current_task in stage:
                    for prev_task in pipeline[i - 1]:
                        current_task.set_upstream(prev_task)

            for task in dag.task_dict.values():
                match = pattern.match(task.task_id)
                task_depth = int(match.group(1))
                # the sum of each stages after this task + itself
                correct_weight = (task_depth * width + 1) * weight

                calculated_weight = task.priority_weight_total
                self.assertEqual(calculated_weight, correct_weight)

    def test_dag_task_priority_weight_total_using_absolute(self):
        # Same test as above except use 'absolute' for weight calculation
        weight = 10
        width = 5
        depth = 5
        with DAG('dag', start_date=DEFAULT_DATE,
                 default_args={'owner': 'owner1'}) as dag:
            pipeline = [
                [DummyOperator(
                    task_id='stage{}.{}'.format(i, j), priority_weight=weight,
                    weight_rule=WeightRule.ABSOLUTE)
                    for j in range(0, width)] for i in range(0, depth)
            ]
            for i, stage in enumerate(pipeline):
                if i == 0:
                    continue
                for current_task in stage:
                    for prev_task in pipeline[i - 1]:
                        current_task.set_upstream(prev_task)

            for task in dag.task_dict.values():
                # the sum of each stages after this task + itself
                correct_weight = weight
                calculated_weight = task.priority_weight_total
                self.assertEqual(calculated_weight, correct_weight)

    def test_dag_task_invalid_weight_rule(self):
        # Test if we enter an invalid weight rule
        with DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'}):
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

    def test_user_defined_filters(self):
        def jinja_udf(name):
            return 'Hello %s' % name

        dag = models.DAG('test-dag', start_date=DEFAULT_DATE, user_defined_filters={"hello": jinja_udf})
        jinja_env = dag.get_template_env()

        self.assertIn('hello', jinja_env.filters)
        self.assertEqual(jinja_env.filters['hello'], jinja_udf)

    def test_resolve_template_files_value(self):

        with NamedTemporaryFile(suffix='.template') as f:
            f.write(b'{{ ds }}')
            f.flush()
            template_dir = os.path.dirname(f.name)
            template_file = os.path.basename(f.name)

            with DAG('test-dag', start_date=DEFAULT_DATE, template_searchpath=template_dir):
                task = DummyOperator(task_id='op1')

            task.test_field = template_file
            task.template_fields = ('test_field',)
            task.template_ext = ('.template',)
            task.resolve_template_files()

        self.assertEqual(task.test_field, '{{ ds }}')

    def test_resolve_template_files_list(self):

        with NamedTemporaryFile(suffix='.template') as f:
            f.write(b'{{ ds }}')
            f.flush()
            template_dir = os.path.dirname(f.name)
            template_file = os.path.basename(f.name)

            with DAG('test-dag', start_date=DEFAULT_DATE, template_searchpath=template_dir):
                task = DummyOperator(task_id='op1')

            task.test_field = [template_file, 'some_string']
            task.template_fields = ('test_field',)
            task.template_ext = ('.template',)
            task.resolve_template_files()

        self.assertEqual(task.test_field, ['{{ ds }}', 'some_string'])

    def test_cycle_empty(self):
        # test empty
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        self.assertFalse(dag.test_cycle())

    def test_cycle_single_task(self):
        # test single task
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        with dag:
            DummyOperator(task_id='A')

        self.assertFalse(dag.test_cycle())

    def test_cycle_no_cycle(self):
        # test no cycle
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        # A -> B -> C
        #      B -> D
        # E -> F
        with dag:
            op1 = DummyOperator(task_id='A')
            op2 = DummyOperator(task_id='B')
            op3 = DummyOperator(task_id='C')
            op4 = DummyOperator(task_id='D')
            op5 = DummyOperator(task_id='E')
            op6 = DummyOperator(task_id='F')
            op1.set_downstream(op2)
            op2.set_downstream(op3)
            op2.set_downstream(op4)
            op5.set_downstream(op6)

        self.assertFalse(dag.test_cycle())

    def test_cycle_loop(self):
        # test self loop
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        # A -> A
        with dag:
            op1 = DummyOperator(task_id='A')
            op1.set_downstream(op1)

        with self.assertRaises(AirflowDagCycleException):
            dag.test_cycle()

    def test_cycle_downstream_loop(self):
        # test downstream self loop
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        # A -> B -> C -> D -> E -> E
        with dag:
            op1 = DummyOperator(task_id='A')
            op2 = DummyOperator(task_id='B')
            op3 = DummyOperator(task_id='C')
            op4 = DummyOperator(task_id='D')
            op5 = DummyOperator(task_id='E')
            op1.set_downstream(op2)
            op2.set_downstream(op3)
            op3.set_downstream(op4)
            op4.set_downstream(op5)
            op5.set_downstream(op5)

        with self.assertRaises(AirflowDagCycleException):
            dag.test_cycle()

    def test_cycle_large_loop(self):
        # large loop
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        # A -> B -> C -> D -> E -> A
        with dag:
            op1 = DummyOperator(task_id='A')
            op2 = DummyOperator(task_id='B')
            op3 = DummyOperator(task_id='C')
            op4 = DummyOperator(task_id='D')
            op5 = DummyOperator(task_id='E')
            op1.set_downstream(op2)
            op2.set_downstream(op3)
            op3.set_downstream(op4)
            op4.set_downstream(op5)
            op5.set_downstream(op1)

        with self.assertRaises(AirflowDagCycleException):
            dag.test_cycle()

    def test_cycle_arbitrary_loop(self):
        # test arbitrary loop
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        # E-> A -> B -> F -> A
        #       -> C -> F
        with dag:
            op1 = DummyOperator(task_id='A')
            op2 = DummyOperator(task_id='B')
            op3 = DummyOperator(task_id='C')
            op4 = DummyOperator(task_id='E')
            op5 = DummyOperator(task_id='F')
            op1.set_downstream(op2)
            op1.set_downstream(op3)
            op4.set_downstream(op1)
            op3.set_downstream(op5)
            op2.set_downstream(op5)
            op5.set_downstream(op1)

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

    def test_following_previous_schedule_daily_dag_cest_to_cet(self):
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

    def test_following_previous_schedule_daily_dag_cet_to_cest(self):
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
                         conf.get('webserver', 'dag_default_view').lower())
        self.assertEqual(orm_dag.safe_dag_id, 'dag')

        orm_subdag = session.query(DagModel).filter(
            DagModel.dag_id == 'dag.subtask').one()
        self.assertEqual(set(orm_subdag.owners.split(', ')), {'owner1', 'owner2'})
        self.assertEqual(orm_subdag.last_scheduler_run, now)
        self.assertTrue(orm_subdag.is_active)
        self.assertEqual(orm_subdag.safe_dag_id, 'dag__dot__subtask')
        self.assertEqual(orm_subdag.fileloc, orm_dag.fileloc)
        session.close()

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
        session.close()

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
        session.close()

    def test_existing_dag_is_paused_upon_creation(self):
        dag = DAG(
            'dag_paused'
        )
        dag.sync_to_db()
        self.assertFalse(dag.is_paused)

        dag = DAG(
            'dag_paused',
            is_paused_upon_creation=True
        )
        dag.sync_to_db()
        # Since the dag existed before, it should not follow the pause flag upon creation
        self.assertFalse(dag.is_paused)

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
        session.close()

    def test_dag_is_deactivated_upon_dagfile_deletion(self):
        dag_id = 'old_existing_dag'
        dag_fileloc = "/usr/local/airflow/dags/non_existing_path.py"
        dag = DAG(
            dag_id,
            is_paused_upon_creation=True,
        )
        dag.fileloc = dag_fileloc
        session = settings.Session()
        dag.sync_to_db(session=session)

        orm_dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).one()

        self.assertTrue(orm_dag.is_active)
        self.assertEqual(orm_dag.fileloc, dag_fileloc)

        DagModel.deactivate_deleted_dags(list_py_file_paths(settings.DAGS_FOLDER))

        orm_dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).one()
        self.assertFalse(orm_dag.is_active)

        # CleanUp
        session.execute(DagModel.__table__.delete().where(DagModel.dag_id == dag_id))
        session.close()

    def test_dag_naive_default_args_start_date_with_timezone(self):
        local_tz = pendulum.timezone('Europe/Zurich')
        default_args = {'start_date': datetime.datetime(2018, 1, 1, tzinfo=local_tz)}

        dag = DAG('DAG', default_args=default_args)
        self.assertEqual(dag.timezone.name, local_tz.name)

        dag = DAG('DAG', default_args=default_args)
        self.assertEqual(dag.timezone.name, local_tz.name)

    def test_roots(self):
        """Verify if dag.roots returns the root tasks of a DAG."""
        with DAG("test_dag", start_date=DEFAULT_DATE) as dag:
            op1 = DummyOperator(task_id="t1")
            op2 = DummyOperator(task_id="t2")
            op3 = DummyOperator(task_id="t3")
            op4 = DummyOperator(task_id="t4")
            op5 = DummyOperator(task_id="t5")
            [op1, op2] >> op3 >> [op4, op5]

            self.assertCountEqual(dag.roots, [op1, op2])

    def test_leaves(self):
        """Verify if dag.leaves returns the leaf tasks of a DAG."""
        with DAG("test_dag", start_date=DEFAULT_DATE) as dag:
            op1 = DummyOperator(task_id="t1")
            op2 = DummyOperator(task_id="t2")
            op3 = DummyOperator(task_id="t3")
            op4 = DummyOperator(task_id="t4")
            op5 = DummyOperator(task_id="t5")
            [op1, op2] >> op3 >> [op4, op5]

            self.assertCountEqual(dag.leaves, [op4, op5])

    def test_tree_view(self):
        """Verify correctness of dag.tree_view()."""
        with DAG("test_dag", start_date=DEFAULT_DATE) as dag:
            op1 = DummyOperator(task_id="t1")
            op2 = DummyOperator(task_id="t2")
            op3 = DummyOperator(task_id="t3")
            op1 >> op2 >> op3

            with redirect_stdout(io.StringIO()) as stdout:
                dag.tree_view()
                stdout = stdout.getvalue()

            stdout_lines = stdout.split("\n")
            self.assertIn('t1', stdout_lines[0])
            self.assertIn('t2', stdout_lines[1])
            self.assertIn('t3', stdout_lines[2])

    def test_duplicate_task_ids_not_allowed_with_dag_context_manager(self):
        """Verify tasks with Duplicate task_id raises error"""
        with self.assertRaisesRegex(
            DuplicateTaskIdFound, "Task id 't1' has already been added to the DAG"
        ):
            with DAG("test_dag", start_date=DEFAULT_DATE) as dag:
                op1 = DummyOperator(task_id="t1")
                op2 = BashOperator(task_id="t1", bash_command="sleep 1")
                op1 >> op2

        self.assertEqual(dag.task_dict, {op1.task_id: op1})

        # Also verify that DAGs with duplicate task_ids don't raise errors
        with DAG("test_dag_1", start_date=DEFAULT_DATE) as dag1:
            op3 = DummyOperator(task_id="t3")
            op4 = BashOperator(task_id="t4", bash_command="sleep 1")
            op3 >> op4

        self.assertEqual(dag1.task_dict, {op3.task_id: op3, op4.task_id: op4})

    def test_duplicate_task_ids_not_allowed_without_dag_context_manager(self):
        """Verify tasks with Duplicate task_id raises error"""
        with self.assertRaisesRegex(
            DuplicateTaskIdFound, "Task id 't1' has already been added to the DAG"
        ):
            dag = DAG("test_dag", start_date=DEFAULT_DATE)
            op1 = DummyOperator(task_id="t1", dag=dag)
            op2 = BashOperator(task_id="t1", bash_command="sleep 1", dag=dag)
            op1 >> op2

        self.assertEqual(dag.task_dict, {op1.task_id: op1})

        # Also verify that DAGs with duplicate task_ids don't raise errors
        dag1 = DAG("test_dag_1", start_date=DEFAULT_DATE)
        op3 = DummyOperator(task_id="t3", dag=dag1)
        op4 = DummyOperator(task_id="t4", dag=dag1)
        op3 >> op4

        self.assertEqual(dag1.task_dict, {op3.task_id: op3, op4.task_id: op4})

    def test_duplicate_task_ids_for_same_task_is_allowed(self):
        """Verify that same tasks with Duplicate task_id do not raise error"""
        with DAG("test_dag", start_date=DEFAULT_DATE) as dag:
            op1 = op2 = DummyOperator(task_id="t1")
            op3 = DummyOperator(task_id="t3")
            op1 >> op3
            op2 >> op3

        self.assertEqual(op1, op2)
        self.assertEqual(dag.task_dict, {op1.task_id: op1, op3.task_id: op3})
        self.assertEqual(dag.task_dict, {op2.task_id: op2, op3.task_id: op3})

    def test_sub_dag_updates_all_references_while_deepcopy(self):
        with DAG("test_dag", start_date=DEFAULT_DATE) as dag:
            op1 = DummyOperator(task_id='t1')
            op2 = DummyOperator(task_id='t2')
            op3 = DummyOperator(task_id='t3')
            op1 >> op2
            op2 >> op3

        sub_dag = dag.sub_dag('t2', include_upstream=True, include_downstream=False)
        self.assertEqual(id(sub_dag.task_dict['t1'].downstream_list[0].dag), id(sub_dag))

    def test_schedule_dag_no_previous_runs(self):
        """
        Tests scheduling a dag with no previous runs
        """
        dag_id = "test_schedule_dag_no_previous_runs"
        dag = DAG(dag_id=dag_id)
        dag.add_task(BaseOperator(
            task_id="faketastic",
            owner='Also fake',
            start_date=datetime_tz(2015, 1, 2, 0, 0)))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag_run = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dag_run)
        self.assertEqual(dag.dag_id, dag_run.dag_id)
        self.assertIsNotNone(dag_run.run_id)
        self.assertNotEqual('', dag_run.run_id)
        self.assertEqual(
            datetime_tz(2015, 1, 2, 0, 0),
            dag_run.execution_date,
            msg='dag_run.execution_date did not match expectation: {0}'
            .format(dag_run.execution_date)
        )
        self.assertEqual(State.RUNNING, dag_run.state)
        self.assertFalse(dag_run.external_trigger)
        dag.clear()
        self._clean_up(dag_id)

    def test_schedule_dag_relativedelta(self):
        """
        Tests scheduling a dag with a relativedelta schedule_interval
        """
        dag_id = "test_schedule_dag_relativedelta"
        delta = relativedelta(hours=+1)
        dag = DAG(dag_id=dag_id,
                  schedule_interval=delta)
        dag.add_task(BaseOperator(
            task_id="faketastic",
            owner='Also fake',
            start_date=datetime_tz(2015, 1, 2, 0, 0)))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag_run = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dag_run)
        self.assertEqual(dag.dag_id, dag_run.dag_id)
        self.assertIsNotNone(dag_run.run_id)
        self.assertNotEqual('', dag_run.run_id)
        self.assertEqual(
            datetime_tz(2015, 1, 2, 0, 0),
            dag_run.execution_date,
            msg='dag_run.execution_date did not match expectation: {0}'
            .format(dag_run.execution_date)
        )
        self.assertEqual(State.RUNNING, dag_run.state)
        self.assertFalse(dag_run.external_trigger)
        dag_run2 = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dag_run2)
        self.assertEqual(dag.dag_id, dag_run2.dag_id)
        self.assertIsNotNone(dag_run2.run_id)
        self.assertNotEqual('', dag_run2.run_id)
        self.assertEqual(
            datetime_tz(2015, 1, 2, 0, 0) + delta,
            dag_run2.execution_date,
            msg='dag_run2.execution_date did not match expectation: {0}'
            .format(dag_run2.execution_date)
        )
        self.assertEqual(State.RUNNING, dag_run2.state)
        self.assertFalse(dag_run2.external_trigger)
        dag.clear()
        self._clean_up(dag_id)

    def test_schedule_dag_fake_scheduled_previous(self):
        """
        Test scheduling a dag where there is a prior DagRun
        which has the same run_id as the next run should have
        """
        delta = datetime.timedelta(hours=1)
        dag_id = "test_schedule_dag_fake_scheduled_previous"
        dag = DAG(dag_id=dag_id,
                  schedule_interval=delta,
                  start_date=DEFAULT_DATE)
        dag.add_task(BaseOperator(
            task_id="faketastic",
            owner='Also fake',
            start_date=DEFAULT_DATE))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag.create_dagrun(run_id=DagRun.id_for_date(DEFAULT_DATE),
                          execution_date=DEFAULT_DATE,
                          state=State.SUCCESS,
                          external_trigger=True)
        dag_run = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dag_run)
        self.assertEqual(dag.dag_id, dag_run.dag_id)
        self.assertIsNotNone(dag_run.run_id)
        self.assertNotEqual('', dag_run.run_id)
        self.assertEqual(
            DEFAULT_DATE + delta,
            dag_run.execution_date,
            msg='dag_run.execution_date did not match expectation: {0}'
            .format(dag_run.execution_date)
        )
        self.assertEqual(State.RUNNING, dag_run.state)
        self.assertFalse(dag_run.external_trigger)
        self._clean_up(dag_id)

    def test_schedule_dag_once(self):
        """
        Tests scheduling a dag scheduled for @once - should be scheduled the first time
        it is called, and not scheduled the second.
        """
        dag_id = "test_schedule_dag_once"
        dag = DAG(dag_id=dag_id)
        dag.schedule_interval = '@once'
        dag.add_task(BaseOperator(
            task_id="faketastic",
            owner='Also fake',
            start_date=datetime_tz(2015, 1, 2, 0, 0)))
        dag_run = DagFileProcessor(dag_ids=[], log=mock.MagicMock()).create_dag_run(dag)
        dag_run2 = DagFileProcessor(dag_ids=[], log=mock.MagicMock()).create_dag_run(dag)

        self.assertIsNotNone(dag_run)
        self.assertIsNone(dag_run2)
        dag.clear()
        self._clean_up(dag_id)

    def test_fractional_seconds(self):
        """
        Tests if fractional seconds are stored in the database
        """
        dag_id = "test_fractional_seconds"
        dag = DAG(dag_id=dag_id)
        dag.schedule_interval = '@once'
        dag.add_task(BaseOperator(
            task_id="faketastic",
            owner='Also fake',
            start_date=datetime_tz(2015, 1, 2, 0, 0)))

        start_date = timezone.utcnow()

        run = dag.create_dagrun(
            run_id='test_' + start_date.isoformat(),
            execution_date=start_date,
            start_date=start_date,
            state=State.RUNNING,
            external_trigger=False
        )

        run.refresh_from_db()

        self.assertEqual(start_date, run.execution_date,
                         "dag run execution_date loses precision")
        self.assertEqual(start_date, run.start_date,
                         "dag run start_date loses precision ")
        self._clean_up(dag_id)

    def test_schedule_dag_start_end_dates(self):
        """
        Tests that an attempt to schedule a task after the Dag's end_date
        does not succeed.
        """
        delta = datetime.timedelta(hours=1)
        runs = 3
        start_date = DEFAULT_DATE
        end_date = start_date + (runs - 1) * delta
        dag_id = "test_schedule_dag_start_end_dates"
        dag = DAG(dag_id=dag_id,
                  start_date=start_date,
                  end_date=end_date,
                  schedule_interval=delta)
        dag.add_task(BaseOperator(task_id='faketastic', owner='Also fake'))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        # Create and schedule the dag runs
        dag_runs = []
        for _ in range(runs):
            dag_runs.append(dag_file_processor.create_dag_run(dag))

        additional_dag_run = dag_file_processor.create_dag_run(dag)

        for dag_run in dag_runs:
            self.assertIsNotNone(dag_run)

        self.assertIsNone(additional_dag_run)
        self._clean_up(dag_id)

    def test_schedule_dag_no_end_date_up_to_today_only(self):
        """
        Tests that a Dag created without an end_date can only be scheduled up
        to and including the current datetime.

        For example, if today is 2016-01-01 and we are scheduling from a
        start_date of 2015-01-01, only jobs up to, but not including
        2016-01-01 should be scheduled.
        """
        session = settings.Session()
        delta = datetime.timedelta(days=1)
        now = utcnow()
        start_date = now.subtract(weeks=1)

        runs = (now - start_date).days
        dag_id = "test_schedule_dag_no_end_date_up_to_today_only"
        dag = DAG(dag_id=dag_id,
                  start_date=start_date,
                  schedule_interval=delta)
        dag.add_task(BaseOperator(task_id='faketastic', owner='Also fake'))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag_runs = []
        for _ in range(runs):
            dag_run = dag_file_processor.create_dag_run(dag)
            dag_runs.append(dag_run)

            # Mark the DagRun as complete
            dag_run.state = State.SUCCESS
            session.merge(dag_run)
            session.commit()

        # Attempt to schedule an additional dag run (for 2016-01-01)
        additional_dag_run = dag_file_processor.create_dag_run(dag)

        for dag_run in dag_runs:
            self.assertIsNotNone(dag_run)

        self.assertIsNone(additional_dag_run)
        self._clean_up(dag_id)

    def test_pickling(self):
        test_dag_id = 'test_pickling'
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG(test_dag_id, default_args=args)
        dag_pickle = dag.pickle()
        self.assertEqual(dag_pickle.pickle.dag_id, dag.dag_id)

    def test_rich_comparison_ops(self):
        test_dag_id = 'test_rich_comparison_ops'

        class DAGsubclass(DAG):
            pass

        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG(test_dag_id, default_args=args)

        dag_eq = DAG(test_dag_id, default_args=args)

        dag_diff_load_time = DAG(test_dag_id, default_args=args)
        dag_diff_name = DAG(test_dag_id + '_neq', default_args=args)

        dag_subclass = DAGsubclass(test_dag_id, default_args=args)
        dag_subclass_diff_name = DAGsubclass(
            test_dag_id + '2', default_args=args)

        for dag_ in [dag_eq, dag_diff_name, dag_subclass, dag_subclass_diff_name]:
            dag_.last_loaded = dag.last_loaded

        # test identity equality
        self.assertEqual(dag, dag)

        # test dag (in)equality based on _comps
        self.assertEqual(dag_eq, dag)
        self.assertNotEqual(dag_diff_name, dag)
        self.assertNotEqual(dag_diff_load_time, dag)

        # test dag inequality based on type even if _comps happen to match
        self.assertNotEqual(dag_subclass, dag)

        # a dag should equal an unpickled version of itself
        dump = pickle.dumps(dag)
        self.assertEqual(pickle.loads(dump), dag)

        # dags are ordered based on dag_id no matter what the type is
        self.assertLess(dag, dag_diff_name)
        self.assertGreater(dag, dag_diff_load_time)
        self.assertLess(dag, dag_subclass_diff_name)

        # greater than should have been created automatically by functools
        self.assertGreater(dag_diff_name, dag)

        # hashes are non-random and match equality
        self.assertEqual(hash(dag), hash(dag))
        self.assertEqual(hash(dag_eq), hash(dag))
        self.assertNotEqual(hash(dag_diff_name), hash(dag))
        self.assertNotEqual(hash(dag_subclass), hash(dag))
