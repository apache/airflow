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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import logging
import os
import pendulum
import unittest
import time
import six
import re
import urllib
import textwrap
import inspect

from airflow import configuration, models, settings, AirflowException
from airflow.exceptions import AirflowDagCycleException, AirflowSkipException
from airflow.jobs import BackfillJob
from airflow.models import DAG, TaskInstance as TI
from airflow.models import DagRun
from airflow.models import State as ST
from airflow.models import DagModel, DagRun, DagStat
from airflow.models import clear_task_instances
from airflow.models import XCom
from airflow.models import Connection
from airflow.jobs import LocalTaskJob
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils import timezone
from airflow.utils.weight_rule import WeightRule
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule
from mock import patch, ANY
from parameterized import parameterized
from tempfile import mkdtemp, NamedTemporaryFile

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
TEST_DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'dags')


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

        self.assertEquals(tuple(), dag.topological_sort())

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
                self.assertEquals(calculated_weight, correct_weight)

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
                self.assertEquals(calculated_weight, correct_weight)

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
                self.assertEquals(calculated_weight, correct_weight)

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

        self.assertEqual(0, DAG.get_num_task_instances(test_dag_id, ['fakename'],
            session=session))
        self.assertEqual(4, DAG.get_num_task_instances(test_dag_id, [test_task_id],
            session=session))
        self.assertEqual(4, DAG.get_num_task_instances(test_dag_id,
            ['fakename', test_task_id], session=session))
        self.assertEqual(1, DAG.get_num_task_instances(test_dag_id, [test_task_id],
            states=[None], session=session))
        self.assertEqual(2, DAG.get_num_task_instances(test_dag_id, [test_task_id],
            states=[State.RUNNING], session=session))
        self.assertEqual(3, DAG.get_num_task_instances(test_dag_id, [test_task_id],
            states=[None, State.RUNNING], session=session))
        self.assertEqual(4, DAG.get_num_task_instances(test_dag_id, [test_task_id],
            states=[None, State.QUEUED, State.RUNNING], session=session))
        session.close()

    def test_render_template_field(self):
        """Tests if render_template from a field works"""

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE)

        with dag:
            task = DummyOperator(task_id='op1')

        result = task.render_template('', '{{ foo }}', dict(foo='bar'))
        self.assertEqual(result, 'bar')

    def test_render_template_field_macro(self):
        """ Tests if render_template from a field works,
            if a custom filter was defined"""

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE,
                  user_defined_macros = dict(foo='bar'))

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
            return 'Hello %s' %name

        dag = DAG('test-dag',
                  start_date=DEFAULT_DATE,
                  user_defined_filters = dict(hello=jinja_udf))

        with dag:
            task = DummyOperator(task_id='op1')

        result = task.render_template('', "{{ 'world' | hello}}", dict())
        self.assertEqual(result, 'Hello world')

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


class DagStatTest(unittest.TestCase):
    def test_dagstats_crud(self):
        DagStat.create(dag_id='test_dagstats_crud')

        session = settings.Session()
        qry = session.query(DagStat).filter(DagStat.dag_id == 'test_dagstats_crud')
        self.assertEqual(len(qry.all()), len(State.dag_states))

        DagStat.set_dirty(dag_id='test_dagstats_crud')
        res = qry.all()

        for stat in res:
            self.assertTrue(stat.dirty)

        # create missing
        DagStat.set_dirty(dag_id='test_dagstats_crud_2')
        qry2 = session.query(DagStat).filter(DagStat.dag_id == 'test_dagstats_crud_2')
        self.assertEqual(len(qry2.all()), len(State.dag_states))

        dag = DAG(
            'test_dagstats_crud',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        with dag:
            op1 = DummyOperator(task_id='A')

        now = timezone.utcnow()
        dr = dag.create_dagrun(
            run_id='manual__' + now.isoformat(),
            execution_date=now,
            start_date=now,
            state=State.FAILED,
            external_trigger=False,
        )

        DagStat.update(dag_ids=['test_dagstats_crud'])
        res = qry.all()
        for stat in res:
            if stat.state == State.FAILED:
                self.assertEqual(stat.count, 1)
            else:
                self.assertEqual(stat.count, 0)

        DagStat.update()
        res = qry2.all()
        for stat in res:
            self.assertFalse(stat.dirty)


class DagRunTest(unittest.TestCase):

    def create_dag_run(self, dag,
                       state=State.RUNNING,
                       task_states=None,
                       execution_date=None,
                       is_backfill=False,
                       ):
        now = timezone.utcnow()
        if execution_date is None:
            execution_date = now
        if is_backfill:
            run_id = BackfillJob.ID_PREFIX + now.isoformat()
        else:
            run_id = 'manual__' + now.isoformat()
        dag_run = dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            start_date=now,
            state=state,
            external_trigger=False,
        )

        if task_states is not None:
            session = settings.Session()
            for task_id, state in task_states.items():
                ti = dag_run.get_task_instance(task_id)
                ti.set_state(state, session)
            session.close()

        return dag_run

    def test_clear_task_instances_for_backfill_dagrun(self):
        now = timezone.utcnow()
        session = settings.Session()
        dag_id = 'test_clear_task_instances_for_backfill_dagrun'
        dag = DAG(dag_id=dag_id, start_date=now)
        self.create_dag_run(dag, execution_date=now, is_backfill=True)

        task0 = DummyOperator(task_id='backfill_task_0', owner='test', dag=dag)
        ti0 = TI(task=task0, execution_date=now)
        ti0.run()

        qry = session.query(TI).filter(
            TI.dag_id == dag.dag_id).all()
        clear_task_instances(qry, session)
        session.commit()
        ti0.refresh_from_db()
        dr0 = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.execution_date == now
        ).first()
        self.assertEquals(dr0.state, State.RUNNING)

    def test_id_for_date(self):
        run_id = models.DagRun.id_for_date(
            timezone.datetime(2015, 1, 2, 3, 4, 5, 6))
        self.assertEqual(
            'scheduled__2015-01-02T03:04:05', run_id,
            'Generated run_id did not match expectations: {0}'.format(run_id))

    def test_dagrun_find(self):
        session = settings.Session()
        now = timezone.utcnow()

        dag_id1 = "test_dagrun_find_externally_triggered"
        dag_run = models.DagRun(
            dag_id=dag_id1,
            run_id='manual__' + now.isoformat(),
            execution_date=now,
            start_date=now,
            state=State.RUNNING,
            external_trigger=True,
        )
        session.add(dag_run)

        dag_id2 = "test_dagrun_find_not_externally_triggered"
        dag_run = models.DagRun(
            dag_id=dag_id2,
            run_id='manual__' + now.isoformat(),
            execution_date=now,
            start_date=now,
            state=State.RUNNING,
            external_trigger=False,
        )
        session.add(dag_run)

        session.commit()

        self.assertEqual(1, len(models.DagRun.find(dag_id=dag_id1, external_trigger=True)))
        self.assertEqual(0, len(models.DagRun.find(dag_id=dag_id1, external_trigger=False)))
        self.assertEqual(0, len(models.DagRun.find(dag_id=dag_id2, external_trigger=True)))
        self.assertEqual(1, len(models.DagRun.find(dag_id=dag_id2, external_trigger=False)))

    def test_dagrun_success_when_all_skipped(self):
        """
        Tests that a DAG run succeeds when all tasks are skipped
        """
        dag = DAG(
            dag_id='test_dagrun_success_when_all_skipped',
            start_date=timezone.datetime(2017, 1, 1)
        )
        dag_task1 = ShortCircuitOperator(
            task_id='test_short_circuit_false',
            dag=dag,
            python_callable=lambda: False)
        dag_task2 = DummyOperator(
            task_id='test_state_skipped1',
            dag=dag)
        dag_task3 = DummyOperator(
            task_id='test_state_skipped2',
            dag=dag)
        dag_task1.set_downstream(dag_task2)
        dag_task2.set_downstream(dag_task3)

        initial_task_states = {
            'test_short_circuit_false': State.SUCCESS,
            'test_state_skipped1': State.SKIPPED,
            'test_state_skipped2': State.SKIPPED,
        }

        dag_run = self.create_dag_run(dag=dag,
                                      state=State.RUNNING,
                                      task_states=initial_task_states)
        updated_dag_state = dag_run.update_state()
        self.assertEqual(State.SUCCESS, updated_dag_state)

    def test_dagrun_success_conditions(self):
        session = settings.Session()

        dag = DAG(
            'test_dagrun_success_conditions',
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

        dag.clear()

        now = timezone.utcnow()
        dr = dag.create_dagrun(run_id='test_dagrun_success_conditions',
                               state=State.RUNNING,
                               execution_date=now,
                               start_date=now)

        # op1 = root
        ti_op1 = dr.get_task_instance(task_id=op1.task_id)
        ti_op1.set_state(state=State.SUCCESS, session=session)

        ti_op2 = dr.get_task_instance(task_id=op2.task_id)
        ti_op3 = dr.get_task_instance(task_id=op3.task_id)
        ti_op4 = dr.get_task_instance(task_id=op4.task_id)

        # root is successful, but unfinished tasks
        state = dr.update_state()
        self.assertEqual(State.RUNNING, state)

        # one has failed, but root is successful
        ti_op2.set_state(state=State.FAILED, session=session)
        ti_op3.set_state(state=State.SUCCESS, session=session)
        ti_op4.set_state(state=State.SUCCESS, session=session)
        state = dr.update_state()
        self.assertEqual(State.SUCCESS, state)

    def test_dagrun_deadlock(self):
        session = settings.Session()
        dag = DAG(
            'text_dagrun_deadlock',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        with dag:
            op1 = DummyOperator(task_id='A')
            op2 = DummyOperator(task_id='B')
            op2.trigger_rule = TriggerRule.ONE_FAILED
            op2.set_upstream(op1)

        dag.clear()
        now = timezone.utcnow()
        dr = dag.create_dagrun(run_id='test_dagrun_deadlock',
                               state=State.RUNNING,
                               execution_date=now,
                               start_date=now)

        ti_op1 = dr.get_task_instance(task_id=op1.task_id)
        ti_op1.set_state(state=State.SUCCESS, session=session)
        ti_op2 = dr.get_task_instance(task_id=op2.task_id)
        ti_op2.set_state(state=State.NONE, session=session)

        dr.update_state()
        self.assertEqual(dr.state, State.RUNNING)

        ti_op2.set_state(state=State.NONE, session=session)
        op2.trigger_rule = 'invalid'
        dr.update_state()
        self.assertEqual(dr.state, State.FAILED)

    def test_dagrun_no_deadlock_with_shutdown(self):
        session = settings.Session()
        dag = DAG('test_dagrun_no_deadlock_with_shutdown',
                  start_date=DEFAULT_DATE)
        with dag:
            op1 = DummyOperator(task_id='upstream_task')
            op2 = DummyOperator(task_id='downstream_task')
            op2.set_upstream(op1)

        dr = dag.create_dagrun(run_id='test_dagrun_no_deadlock_with_shutdown',
                               state=State.RUNNING,
                               execution_date=DEFAULT_DATE,
                               start_date=DEFAULT_DATE)
        upstream_ti = dr.get_task_instance(task_id='upstream_task')
        upstream_ti.set_state(State.SHUTDOWN, session=session)

        dr.update_state()
        self.assertEqual(dr.state, State.RUNNING)

    def test_dagrun_no_deadlock_with_depends_on_past(self):
        session = settings.Session()
        dag = DAG('test_dagrun_no_deadlock',
                  start_date=DEFAULT_DATE)
        with dag:
            op1 = DummyOperator(task_id='dop', depends_on_past=True)
            op2 = DummyOperator(task_id='tc', task_concurrency=1)

        dag.clear()
        dr = dag.create_dagrun(run_id='test_dagrun_no_deadlock_1',
                               state=State.RUNNING,
                               execution_date=DEFAULT_DATE,
                               start_date=DEFAULT_DATE)
        dr2 = dag.create_dagrun(run_id='test_dagrun_no_deadlock_2',
                                state=State.RUNNING,
                                execution_date=DEFAULT_DATE + datetime.timedelta(days=1),
                                start_date=DEFAULT_DATE + datetime.timedelta(days=1))
        ti1_op1 = dr.get_task_instance(task_id='dop')
        ti2_op1 = dr2.get_task_instance(task_id='dop')
        ti2_op1 = dr.get_task_instance(task_id='tc')
        ti2_op2 = dr.get_task_instance(task_id='tc')
        ti1_op1.set_state(state=State.RUNNING, session=session)
        dr.update_state()
        dr2.update_state()
        self.assertEqual(dr.state, State.RUNNING)
        self.assertEqual(dr2.state, State.RUNNING)

        ti2_op1.set_state(state=State.RUNNING, session=session)
        dr.update_state()
        dr2.update_state()
        self.assertEqual(dr.state, State.RUNNING)
        self.assertEqual(dr2.state, State.RUNNING)

    def test_dagrun_success_callback(self):
        def on_success_callable(context):
            self.assertEqual(
                context['dag_run'].dag_id,
                'test_dagrun_success_callback'
            )

        dag = DAG(
            dag_id='test_dagrun_success_callback',
            start_date=datetime.datetime(2017, 1, 1),
            on_success_callback=on_success_callable,
        )
        dag_task1 = DummyOperator(
            task_id='test_state_succeeded1',
            dag=dag)
        dag_task2 = DummyOperator(
            task_id='test_state_succeeded2',
            dag=dag)
        dag_task1.set_downstream(dag_task2)

        initial_task_states = {
            'test_state_succeeded1': State.SUCCESS,
            'test_state_succeeded2': State.SUCCESS,
        }

        dag_run = self.create_dag_run(dag=dag,
                                      state=State.RUNNING,
                                      task_states=initial_task_states)
        updated_dag_state = dag_run.update_state()
        self.assertEqual(State.SUCCESS, updated_dag_state)

    def test_dagrun_failure_callback(self):
        def on_failure_callable(context):
            self.assertEqual(
                context['dag_run'].dag_id,
                'test_dagrun_failure_callback'
            )

        dag = DAG(
            dag_id='test_dagrun_failure_callback',
            start_date=datetime.datetime(2017, 1, 1),
            on_failure_callback=on_failure_callable,
        )
        dag_task1 = DummyOperator(
            task_id='test_state_succeeded1',
            dag=dag)
        dag_task2 = DummyOperator(
            task_id='test_state_failed2',
            dag=dag)

        initial_task_states = {
            'test_state_succeeded1': State.SUCCESS,
            'test_state_failed2': State.FAILED,
        }
        dag_task1.set_downstream(dag_task2)

        dag_run = self.create_dag_run(dag=dag,
                                      state=State.RUNNING,
                                      task_states=initial_task_states)
        updated_dag_state = dag_run.update_state()
        self.assertEqual(State.FAILED, updated_dag_state)

    def test_dagrun_set_state_end_date(self):
        session = settings.Session()

        dag = DAG(
            'test_dagrun_set_state_end_date',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        dag.clear()

        now = timezone.utcnow()
        dr = dag.create_dagrun(run_id='test_dagrun_set_state_end_date',
                               state=State.RUNNING,
                               execution_date=now,
                               start_date=now)

        # Initial end_date should be NULL
        # State.SUCCESS and State.FAILED are all ending state and should set end_date
        # State.RUNNING set end_date back to NULL
        session.add(dr)
        session.commit()
        self.assertIsNone(dr.end_date)

        dr.set_state(State.SUCCESS)
        session.merge(dr)
        session.commit()

        dr_database = session.query(DagRun).filter(
            DagRun.run_id == 'test_dagrun_set_state_end_date'
        ).one()
        self.assertIsNotNone(dr_database.end_date)
        self.assertEqual(dr.end_date, dr_database.end_date)

        dr.set_state(State.RUNNING)
        session.merge(dr)
        session.commit()

        dr_database = session.query(DagRun).filter(
            DagRun.run_id == 'test_dagrun_set_state_end_date'
        ).one()

        self.assertIsNone(dr_database.end_date)

        dr.set_state(State.FAILED)
        session.merge(dr)
        session.commit()
        dr_database = session.query(DagRun).filter(
            DagRun.run_id == 'test_dagrun_set_state_end_date'
        ).one()

        self.assertIsNotNone(dr_database.end_date)
        self.assertEqual(dr.end_date, dr_database.end_date)

    def test_dagrun_update_state_end_date(self):
        session = settings.Session()

        dag = DAG(
            'test_dagrun_update_state_end_date',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        # A -> B
        with dag:
            op1 = DummyOperator(task_id='A')
            op2 = DummyOperator(task_id='B')
            op1.set_upstream(op2)

        dag.clear()

        now = timezone.utcnow()
        dr = dag.create_dagrun(run_id='test_dagrun_update_state_end_date',
                               state=State.RUNNING,
                               execution_date=now,
                               start_date=now)

        # Initial end_date should be NULL
        # State.SUCCESS and State.FAILED are all ending state and should set end_date
        # State.RUNNING set end_date back to NULL
        session.merge(dr)
        session.commit()
        self.assertIsNone(dr.end_date)

        ti_op1 = dr.get_task_instance(task_id=op1.task_id)
        ti_op1.set_state(state=State.SUCCESS, session=session)
        ti_op2 = dr.get_task_instance(task_id=op2.task_id)
        ti_op2.set_state(state=State.SUCCESS, session=session)

        dr.update_state()

        dr_database = session.query(DagRun).filter(
            DagRun.run_id == 'test_dagrun_update_state_end_date'
        ).one()
        self.assertIsNotNone(dr_database.end_date)
        self.assertEqual(dr.end_date, dr_database.end_date)

        ti_op1.set_state(state=State.RUNNING, session=session)
        ti_op2.set_state(state=State.RUNNING, session=session)
        dr.update_state()

        dr_database = session.query(DagRun).filter(
            DagRun.run_id == 'test_dagrun_update_state_end_date'
        ).one()

        self.assertEqual(dr._state, State.RUNNING)
        self.assertIsNone(dr.end_date)
        self.assertIsNone(dr_database.end_date)

        ti_op1.set_state(state=State.FAILED, session=session)
        ti_op2.set_state(state=State.FAILED, session=session)
        dr.update_state()

        dr_database = session.query(DagRun).filter(
            DagRun.run_id == 'test_dagrun_update_state_end_date'
        ).one()

        self.assertIsNotNone(dr_database.end_date)
        self.assertEqual(dr.end_date, dr_database.end_date)

    def test_get_task_instance_on_empty_dagrun(self):
        """
        Make sure that a proper value is returned when a dagrun has no task instances
        """
        dag = DAG(
            dag_id='test_get_task_instance_on_empty_dagrun',
            start_date=timezone.datetime(2017, 1, 1)
        )
        dag_task1 = ShortCircuitOperator(
            task_id='test_short_circuit_false',
            dag=dag,
            python_callable=lambda: False)

        session = settings.Session()

        now = timezone.utcnow()

        # Don't use create_dagrun since it will create the task instances too which we
        # don't want
        dag_run = models.DagRun(
            dag_id=dag.dag_id,
            run_id='manual__' + now.isoformat(),
            execution_date=now,
            start_date=now,
            state=State.RUNNING,
            external_trigger=False,
        )
        session.add(dag_run)
        session.commit()

        ti = dag_run.get_task_instance('test_short_circuit_false')
        self.assertEqual(None, ti)

    def test_get_latest_runs(self):
        session = settings.Session()
        dag = DAG(
            dag_id='test_latest_runs_1',
            start_date=DEFAULT_DATE)
        dag_1_run_1 = self.create_dag_run(dag,
                execution_date=timezone.datetime(2015, 1, 1))
        dag_1_run_2 = self.create_dag_run(dag,
                execution_date=timezone.datetime(2015, 1, 2))
        dagruns = models.DagRun.get_latest_runs(session)
        session.close()
        for dagrun in dagruns:
            if dagrun.dag_id == 'test_latest_runs_1':
                self.assertEqual(dagrun.execution_date, timezone.datetime(2015, 1, 2))

    def test_is_backfill(self):
        dag = DAG(dag_id='test_is_backfill', start_date=DEFAULT_DATE)

        dagrun = self.create_dag_run(dag, execution_date=DEFAULT_DATE)
        dagrun.run_id = BackfillJob.ID_PREFIX + '_sfddsffds'

        dagrun2 = self.create_dag_run(
            dag, execution_date=DEFAULT_DATE + datetime.timedelta(days=1))

        dagrun3 = self.create_dag_run(
            dag, execution_date=DEFAULT_DATE + datetime.timedelta(days=2))
        dagrun3.run_id = None

        self.assertTrue(dagrun.is_backfill)
        self.assertFalse(dagrun2.is_backfill)
        self.assertFalse(dagrun3.is_backfill)

    def test_removed_task_instances_can_be_restored(self):
        def with_all_tasks_removed(dag):
            return DAG(dag_id=dag.dag_id, start_date=dag.start_date)

        dag = DAG('test_task_restoration', start_date=DEFAULT_DATE)
        dag.add_task(DummyOperator(task_id='flaky_task', owner='test'))

        dagrun = self.create_dag_run(dag)
        flaky_ti = dagrun.get_task_instances()[0]
        self.assertEquals('flaky_task', flaky_ti.task_id)
        self.assertEquals(State.NONE, flaky_ti.state)

        dagrun.dag = with_all_tasks_removed(dag)

        dagrun.verify_integrity()
        flaky_ti.refresh_from_db()
        self.assertEquals(State.NONE, flaky_ti.state)

        dagrun.dag.add_task(DummyOperator(task_id='flaky_task', owner='test'))

        dagrun.verify_integrity()
        flaky_ti.refresh_from_db()
        self.assertEquals(State.NONE, flaky_ti.state)


class DagBagTest(unittest.TestCase):

    def test_get_existing_dag(self):
        """
        test that were're able to parse some example DAGs and retrieve them
        """
        dagbag = models.DagBag(include_examples=True)

        some_expected_dag_ids = ["example_bash_operator",
                                 "example_branch_operator"]

        for dag_id in some_expected_dag_ids:
            dag = dagbag.get_dag(dag_id)

            self.assertIsNotNone(dag)
            self.assertEqual(dag_id, dag.dag_id)

        self.assertGreaterEqual(dagbag.size(), 7)

    def test_get_non_existing_dag(self):
        """
        test that retrieving a non existing dag id returns None without crashing
        """
        dagbag = models.DagBag(include_examples=True)

        non_existing_dag_id = "non_existing_dag_id"
        self.assertIsNone(dagbag.get_dag(non_existing_dag_id))

    def test_process_file_that_contains_multi_bytes_char(self):
        """
        test that we're able to parse file that contains multi-byte char
        """
        f = NamedTemporaryFile()
        f.write('\u3042'.encode('utf8'))  # write multi-byte char (hiragana)
        f.flush()

        dagbag = models.DagBag(include_examples=True)
        self.assertEqual([], dagbag.process_file(f.name))

    def test_zip_skip_log(self):
        """
        test the loading of a DAG from within a zip file that skips another file because
        it doesn't have "airflow" and "DAG"
        """
        from mock import Mock
        with patch('airflow.models.DagBag.log') as log_mock:
            log_mock.info = Mock()
            test_zip_path = os.path.join(TEST_DAGS_FOLDER, "test_zip.zip")
            dagbag = models.DagBag(dag_folder=test_zip_path, include_examples=False)

            self.assertTrue(dagbag.has_logged)
            log_mock.info.assert_any_call("File %s assumed to contain no DAGs. Skipping.",
                                          test_zip_path)

    def test_zip(self):
        """
        test the loading of a DAG within a zip file that includes dependencies
        """
        dagbag = models.DagBag()
        dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, "test_zip.zip"))
        self.assertTrue(dagbag.get_dag("test_zip_dag"))

    def test_process_file_cron_validity_check(self):
        """
        test if an invalid cron expression
        as schedule interval can be identified
        """
        invalid_dag_files = ["test_invalid_cron.py", "test_zip_invalid_cron.zip"]
        dagbag = models.DagBag(dag_folder=mkdtemp())

        self.assertEqual(len(dagbag.import_errors), 0)
        for d in invalid_dag_files:
            dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, d))
        self.assertEqual(len(dagbag.import_errors), len(invalid_dag_files))

    @patch.object(DagModel,'get_current')
    def test_get_dag_without_refresh(self, mock_dagmodel):
        """
        Test that, once a DAG is loaded, it doesn't get refreshed again if it
        hasn't been expired.
        """
        dag_id = 'example_bash_operator'

        mock_dagmodel.return_value = DagModel()
        mock_dagmodel.return_value.last_expired = None
        mock_dagmodel.return_value.fileloc = 'foo'

        class TestDagBag(models.DagBag):
            process_file_calls = 0
            def process_file(self, filepath, only_if_updated=True, safe_mode=True):
                if 'example_bash_operator.py' == os.path.basename(filepath):
                    TestDagBag.process_file_calls += 1
                super(TestDagBag, self).process_file(filepath, only_if_updated, safe_mode)

        dagbag = TestDagBag(include_examples=True)
        processed_files = dagbag.process_file_calls

        # Should not call process_file agani, since it's already loaded during init.
        self.assertEqual(1, dagbag.process_file_calls)
        self.assertIsNotNone(dagbag.get_dag(dag_id))
        self.assertEqual(1, dagbag.process_file_calls)

    def test_get_dag_fileloc(self):
        """
        Test that fileloc is correctly set when we load example DAGs,
        specifically SubDAGs.
        """
        dagbag = models.DagBag(include_examples=True)

        expected = {
            'example_bash_operator': 'example_bash_operator.py',
            'example_subdag_operator': 'example_subdag_operator.py',
            'example_subdag_operator.section-1': 'subdags/subdag.py'
        }

        for dag_id, path in expected.items():
            dag = dagbag.get_dag(dag_id)
            self.assertTrue(
                dag.fileloc.endswith('airflow/example_dags/' + path))

    def process_dag(self, create_dag):
        """
        Helper method to process a file generated from the input create_dag function.
        """
        # write source to file
        source = textwrap.dedent(''.join(
            inspect.getsource(create_dag).splitlines(True)[1:-1]))
        f = NamedTemporaryFile()
        f.write(source.encode('utf8'))
        f.flush()

        dagbag = models.DagBag(include_examples=False)
        found_dags = dagbag.process_file(f.name)
        return dagbag, found_dags, f.name

    def validate_dags(self, expected_parent_dag, actual_found_dags, actual_dagbag,
                      should_be_found=True):
        expected_dag_ids = list(map(lambda dag: dag.dag_id, expected_parent_dag.subdags))
        expected_dag_ids.append(expected_parent_dag.dag_id)

        actual_found_dag_ids = list(map(lambda dag: dag.dag_id, actual_found_dags))

        for dag_id in expected_dag_ids:
            actual_dagbag.log.info('validating %s' % dag_id)
            self.assertEquals(
                dag_id in actual_found_dag_ids, should_be_found,
                'dag "%s" should %shave been found after processing dag "%s"' %
                (dag_id, '' if should_be_found else 'not ', expected_parent_dag.dag_id)
            )
            self.assertEquals(
                dag_id in actual_dagbag.dags, should_be_found,
                'dag "%s" should %sbe in dagbag.dags after processing dag "%s"' %
                (dag_id, '' if should_be_found else 'not ', expected_parent_dag.dag_id)
            )

    def test_load_subdags(self):
        # Define Dag to load
        def standard_subdag():
            from airflow.models import DAG
            from airflow.operators.dummy_operator import DummyOperator
            from airflow.operators.subdag_operator import SubDagOperator
            import datetime
            DAG_NAME = 'master'
            DEFAULT_ARGS = {
                'owner': 'owner1',
                'start_date': datetime.datetime(2016, 1, 1)
            }
            dag = DAG(
                DAG_NAME,
                default_args=DEFAULT_ARGS)

            # master:
            #     A -> opSubDag_0
            #          master.opsubdag_0:
            #              -> subdag_0.task
            #     A -> opSubDag_1
            #          master.opsubdag_1:
            #              -> subdag_1.task

            with dag:
                def subdag_0():
                    subdag_0 = DAG('master.opSubdag_0', default_args=DEFAULT_ARGS)
                    DummyOperator(task_id='subdag_0.task', dag=subdag_0)
                    return subdag_0

                def subdag_1():
                    subdag_1 = DAG('master.opSubdag_1', default_args=DEFAULT_ARGS)
                    DummyOperator(task_id='subdag_1.task', dag=subdag_1)
                    return subdag_1

                opSubdag_0 = SubDagOperator(
                    task_id='opSubdag_0', dag=dag, subdag=subdag_0())
                opSubdag_1 = SubDagOperator(
                    task_id='opSubdag_1', dag=dag, subdag=subdag_1())

                opA = DummyOperator(task_id='A')
                opA.set_downstream(opSubdag_0)
                opA.set_downstream(opSubdag_1)
            return dag

        testDag = standard_subdag()
        # sanity check to make sure DAG.subdag is still functioning properly
        self.assertEqual(len(testDag.subdags), 2)

        # Perform processing dag
        dagbag, found_dags, _ = self.process_dag(standard_subdag)

        # Validate correctness
        # all dags from testDag should be listed
        self.validate_dags(testDag, found_dags, dagbag)

        # Define Dag to load
        def nested_subdags():
            from airflow.models import DAG
            from airflow.operators.dummy_operator import DummyOperator
            from airflow.operators.subdag_operator import SubDagOperator
            import datetime
            DAG_NAME = 'master'
            DEFAULT_ARGS = {
                'owner': 'owner1',
                'start_date': datetime.datetime(2016, 1, 1)
            }
            dag = DAG(
                DAG_NAME,
                default_args=DEFAULT_ARGS)

            # master:
            #     A -> opSubdag_0
            #          master.opSubdag_0:
            #              -> opSubDag_A
            #                 master.opSubdag_0.opSubdag_A:
            #                     -> subdag_A.task
            #              -> opSubdag_B
            #                 master.opSubdag_0.opSubdag_B:
            #                     -> subdag_B.task
            #     A -> opSubdag_1
            #          master.opSubdag_1:
            #              -> opSubdag_C
            #                 master.opSubdag_1.opSubdag_C:
            #                     -> subdag_C.task
            #              -> opSubDag_D
            #                 master.opSubdag_1.opSubdag_D:
            #                     -> subdag_D.task

            with dag:
                def subdag_A():
                    subdag_A = DAG(
                        'master.opSubdag_0.opSubdag_A', default_args=DEFAULT_ARGS)
                    DummyOperator(task_id='subdag_A.task', dag=subdag_A)
                    return subdag_A

                def subdag_B():
                    subdag_B = DAG(
                        'master.opSubdag_0.opSubdag_B', default_args=DEFAULT_ARGS)
                    DummyOperator(task_id='subdag_B.task', dag=subdag_B)
                    return subdag_B

                def subdag_C():
                    subdag_C = DAG(
                        'master.opSubdag_1.opSubdag_C', default_args=DEFAULT_ARGS)
                    DummyOperator(task_id='subdag_C.task', dag=subdag_C)
                    return subdag_C

                def subdag_D():
                    subdag_D = DAG(
                        'master.opSubdag_1.opSubdag_D', default_args=DEFAULT_ARGS)
                    DummyOperator(task_id='subdag_D.task', dag=subdag_D)
                    return subdag_D

                def subdag_0():
                    subdag_0 = DAG('master.opSubdag_0', default_args=DEFAULT_ARGS)
                    SubDagOperator(task_id='opSubdag_A', dag=subdag_0, subdag=subdag_A())
                    SubDagOperator(task_id='opSubdag_B', dag=subdag_0, subdag=subdag_B())
                    return subdag_0

                def subdag_1():
                    subdag_1 = DAG('master.opSubdag_1', default_args=DEFAULT_ARGS)
                    SubDagOperator(task_id='opSubdag_C', dag=subdag_1, subdag=subdag_C())
                    SubDagOperator(task_id='opSubdag_D', dag=subdag_1, subdag=subdag_D())
                    return subdag_1

                opSubdag_0 = SubDagOperator(
                    task_id='opSubdag_0', dag=dag, subdag=subdag_0())
                opSubdag_1 = SubDagOperator(
                    task_id='opSubdag_1', dag=dag, subdag=subdag_1())

                opA = DummyOperator(task_id='A')
                opA.set_downstream(opSubdag_0)
                opA.set_downstream(opSubdag_1)

            return dag

        testDag = nested_subdags()
        # sanity check to make sure DAG.subdag is still functioning properly
        self.assertEqual(len(testDag.subdags), 6)

        # Perform processing dag
        dagbag, found_dags, _ = self.process_dag(nested_subdags)

        # Validate correctness
        # all dags from testDag should be listed
        self.validate_dags(testDag, found_dags, dagbag)

    def test_skip_cycle_dags(self):
        """
        Don't crash when loading an invalid (contains a cycle) DAG file.
        Don't load the dag into the DagBag either
        """
        # Define Dag to load
        def basic_cycle():
            from airflow.models import DAG
            from airflow.operators.dummy_operator import DummyOperator
            import datetime
            DAG_NAME = 'cycle_dag'
            DEFAULT_ARGS = {
                'owner': 'owner1',
                'start_date': datetime.datetime(2016, 1, 1)
            }
            dag = DAG(
                DAG_NAME,
                default_args=DEFAULT_ARGS)

            # A -> A
            with dag:
                opA = DummyOperator(task_id='A')
                opA.set_downstream(opA)

            return dag

        testDag = basic_cycle()
        # sanity check to make sure DAG.subdag is still functioning properly
        self.assertEqual(len(testDag.subdags), 0)

        # Perform processing dag
        dagbag, found_dags, file_path = self.process_dag(basic_cycle)

        # #Validate correctness
        # None of the dags should be found
        self.validate_dags(testDag, found_dags, dagbag, should_be_found=False)
        self.assertIn(file_path, dagbag.import_errors)

        # Define Dag to load
        def nested_subdag_cycle():
            from airflow.models import DAG
            from airflow.operators.dummy_operator import DummyOperator
            from airflow.operators.subdag_operator import SubDagOperator
            import datetime
            DAG_NAME = 'nested_cycle'
            DEFAULT_ARGS = {
                'owner': 'owner1',
                'start_date': datetime.datetime(2016, 1, 1)
            }
            dag = DAG(
                DAG_NAME,
                default_args=DEFAULT_ARGS)

            # cycle:
            #     A -> opSubdag_0
            #          cycle.opSubdag_0:
            #              -> opSubDag_A
            #                 cycle.opSubdag_0.opSubdag_A:
            #                     -> subdag_A.task
            #              -> opSubdag_B
            #                 cycle.opSubdag_0.opSubdag_B:
            #                     -> subdag_B.task
            #     A -> opSubdag_1
            #          cycle.opSubdag_1:
            #              -> opSubdag_C
            #                 cycle.opSubdag_1.opSubdag_C:
            #                     -> subdag_C.task -> subdag_C.task  >Invalid Loop<
            #              -> opSubDag_D
            #                 cycle.opSubdag_1.opSubdag_D:
            #                     -> subdag_D.task

            with dag:
                def subdag_A():
                    subdag_A = DAG(
                        'nested_cycle.opSubdag_0.opSubdag_A', default_args=DEFAULT_ARGS)
                    DummyOperator(task_id='subdag_A.task', dag=subdag_A)
                    return subdag_A

                def subdag_B():
                    subdag_B = DAG(
                        'nested_cycle.opSubdag_0.opSubdag_B', default_args=DEFAULT_ARGS)
                    DummyOperator(task_id='subdag_B.task', dag=subdag_B)
                    return subdag_B

                def subdag_C():
                    subdag_C = DAG(
                        'nested_cycle.opSubdag_1.opSubdag_C', default_args=DEFAULT_ARGS)
                    opSubdag_C_task = DummyOperator(
                        task_id='subdag_C.task', dag=subdag_C)
                    # introduce a loop in opSubdag_C
                    opSubdag_C_task.set_downstream(opSubdag_C_task)
                    return subdag_C

                def subdag_D():
                    subdag_D = DAG(
                        'nested_cycle.opSubdag_1.opSubdag_D', default_args=DEFAULT_ARGS)
                    DummyOperator(task_id='subdag_D.task', dag=subdag_D)
                    return subdag_D

                def subdag_0():
                    subdag_0 = DAG('nested_cycle.opSubdag_0', default_args=DEFAULT_ARGS)
                    SubDagOperator(task_id='opSubdag_A', dag=subdag_0, subdag=subdag_A())
                    SubDagOperator(task_id='opSubdag_B', dag=subdag_0, subdag=subdag_B())
                    return subdag_0

                def subdag_1():
                    subdag_1 = DAG('nested_cycle.opSubdag_1', default_args=DEFAULT_ARGS)
                    SubDagOperator(task_id='opSubdag_C', dag=subdag_1, subdag=subdag_C())
                    SubDagOperator(task_id='opSubdag_D', dag=subdag_1, subdag=subdag_D())
                    return subdag_1

                opSubdag_0 = SubDagOperator(
                    task_id='opSubdag_0', dag=dag, subdag=subdag_0())
                opSubdag_1 = SubDagOperator(
                    task_id='opSubdag_1', dag=dag, subdag=subdag_1())

                opA = DummyOperator(task_id='A')
                opA.set_downstream(opSubdag_0)
                opA.set_downstream(opSubdag_1)

            return dag

        testDag = nested_subdag_cycle()
        # sanity check to make sure DAG.subdag is still functioning properly
        self.assertEqual(len(testDag.subdags), 6)

        # Perform processing dag
        dagbag, found_dags, file_path = self.process_dag(nested_subdag_cycle)

        # Validate correctness
        # None of the dags should be found
        self.validate_dags(testDag, found_dags, dagbag, should_be_found=False)
        self.assertIn(file_path, dagbag.import_errors)

    def test_process_file_with_none(self):
        """
        test that process_file can handle Nones
        """
        dagbag = models.DagBag(include_examples=True)

        self.assertEqual([], dagbag.process_file(None))

    @patch.object(TI, 'handle_failure')
    def test_kill_zombies(self, mock_ti):
        """
        Test that kill zombies call TIs failure handler with proper context
        """
        dagbag = models.DagBag()
        session = settings.Session
        dag = dagbag.get_dag('example_branch_operator')
        task = dag.get_task(task_id='run_this_first')

        ti = TI(task, datetime.datetime.now() - datetime.timedelta(1), 'running')
        lj = LocalTaskJob(ti)
        lj.state = State.SHUTDOWN

        session.add(lj)
        session.commit()

        ti.job_id = lj.id

        session.add(ti)
        session.commit()

        dagbag.kill_zombies()
        mock_ti.assert_called_with(ANY,
                                   configuration.getboolean('core', 'unit_test_mode'),
                                   ANY)


class TaskInstanceTest(unittest.TestCase):

    def test_set_task_dates(self):
        """
        Test that tasks properly take start/end dates from DAGs
        """
        dag = DAG('dag', start_date=DEFAULT_DATE, end_date=DEFAULT_DATE + datetime.timedelta(days=10))

        op1 = DummyOperator(task_id='op_1', owner='test')

        self.assertTrue(op1.start_date is None and op1.end_date is None)

        # dag should assign its dates to op1 because op1 has no dates
        dag.add_task(op1)
        self.assertTrue(
            op1.start_date == dag.start_date and op1.end_date == dag.end_date)

        op2 = DummyOperator(
            task_id='op_2',
            owner='test',
            start_date=DEFAULT_DATE - datetime.timedelta(days=1),
            end_date=DEFAULT_DATE + datetime.timedelta(days=11))

        # dag should assign its dates to op2 because they are more restrictive
        dag.add_task(op2)
        self.assertTrue(
            op2.start_date == dag.start_date and op2.end_date == dag.end_date)

        op3 = DummyOperator(
            task_id='op_3',
            owner='test',
            start_date=DEFAULT_DATE + datetime.timedelta(days=1),
            end_date=DEFAULT_DATE + datetime.timedelta(days=9))
        # op3 should keep its dates because they are more restrictive
        dag.add_task(op3)
        self.assertTrue(
            op3.start_date == DEFAULT_DATE + datetime.timedelta(days=1))
        self.assertTrue(
            op3.end_date == DEFAULT_DATE + datetime.timedelta(days=9))

    def test_timezone_awareness(self):
        NAIVE_DATETIME = DEFAULT_DATE.replace(tzinfo=None)

        # check ti without dag (just for bw compat)
        op_no_dag = DummyOperator(task_id='op_no_dag')
        ti = TI(task=op_no_dag, execution_date=NAIVE_DATETIME)

        self.assertEquals(ti.execution_date, DEFAULT_DATE)

        # check with dag without localized execution_date
        dag = DAG('dag', start_date=DEFAULT_DATE)
        op1 = DummyOperator(task_id='op_1')
        dag.add_task(op1)
        ti = TI(task=op1, execution_date=NAIVE_DATETIME)

        self.assertEquals(ti.execution_date, DEFAULT_DATE)

        # with dag and localized execution_date
        tz = pendulum.timezone("Europe/Amsterdam")
        execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tz)
        utc_date = timezone.convert_to_utc(execution_date)
        ti = TI(task=op1, execution_date=execution_date)
        self.assertEquals(ti.execution_date, utc_date)

    def test_task_naive_datetime(self):
        NAIVE_DATETIME = DEFAULT_DATE.replace(tzinfo=None)

        op_no_dag = DummyOperator(task_id='test_task_naive_datetime',
                                  start_date=NAIVE_DATETIME,
                                  end_date=NAIVE_DATETIME)

        self.assertTrue(op_no_dag.start_date.tzinfo)
        self.assertTrue(op_no_dag.end_date.tzinfo)

    def test_set_dag(self):
        """
        Test assigning Operators to Dags, including deferred assignment
        """
        dag = DAG('dag', start_date=DEFAULT_DATE)
        dag2 = DAG('dag2', start_date=DEFAULT_DATE)
        op = DummyOperator(task_id='op_1', owner='test')

        # no dag assigned
        self.assertFalse(op.has_dag())
        self.assertRaises(AirflowException, getattr, op, 'dag')

        # no improper assignment
        with self.assertRaises(TypeError):
            op.dag = 1

        op.dag = dag

        # no reassignment
        with self.assertRaises(AirflowException):
            op.dag = dag2

        # but assigning the same dag is ok
        op.dag = dag

        self.assertIs(op.dag, dag)
        self.assertIn(op, dag.tasks)

    def test_infer_dag(self):
        dag = DAG('dag', start_date=DEFAULT_DATE)
        dag2 = DAG('dag2', start_date=DEFAULT_DATE)

        op1 = DummyOperator(task_id='test_op_1', owner='test')
        op2 = DummyOperator(task_id='test_op_2', owner='test')
        op3 = DummyOperator(task_id='test_op_3', owner='test', dag=dag)
        op4 = DummyOperator(task_id='test_op_4', owner='test', dag=dag2)

        # double check dags
        self.assertEqual(
            [i.has_dag() for i in [op1, op2, op3, op4]],
            [False, False, True, True])

        # can't combine operators with no dags
        self.assertRaises(AirflowException, op1.set_downstream, op2)

        # op2 should infer dag from op1
        op1.dag = dag
        op1.set_downstream(op2)
        self.assertIs(op2.dag, dag)

        # can't assign across multiple DAGs
        self.assertRaises(AirflowException, op1.set_downstream, op4)
        self.assertRaises(AirflowException, op1.set_downstream, [op3, op4])

    def test_bitshift_compose_operators(self):
        dag = DAG('dag', start_date=DEFAULT_DATE)
        op1 = DummyOperator(task_id='test_op_1', owner='test')
        op2 = DummyOperator(task_id='test_op_2', owner='test')
        op3 = DummyOperator(task_id='test_op_3', owner='test')
        op4 = DummyOperator(task_id='test_op_4', owner='test')
        op5 = DummyOperator(task_id='test_op_5', owner='test')

        # can't compose operators without dags
        with self.assertRaises(AirflowException):
            op1 >> op2

        dag >> op1 >> op2 << op3

        # make sure dag assignment carries through
        # using __rrshift__
        self.assertIs(op1.dag, dag)
        self.assertIs(op2.dag, dag)
        self.assertIs(op3.dag, dag)

        # op2 should be downstream of both
        self.assertIn(op2, op1.downstream_list)
        self.assertIn(op2, op3.downstream_list)

        # test dag assignment with __rlshift__
        dag << op4
        self.assertIs(op4.dag, dag)

        # dag assignment with __rrshift__
        dag >> op5
        self.assertIs(op5.dag, dag)

    @patch.object(DAG, 'concurrency_reached')
    def test_requeue_over_concurrency(self, mock_concurrency_reached):
        mock_concurrency_reached.return_value = True

        dag = DAG(dag_id='test_requeue_over_concurrency', start_date=DEFAULT_DATE,
                  max_active_runs=1, concurrency=2)
        task = DummyOperator(task_id='test_requeue_over_concurrency_op', dag=dag)

        ti = TI(task=task, execution_date=timezone.utcnow())
        ti.run()
        self.assertEqual(ti.state, models.State.NONE)


    @patch.object(TI, 'pool_full')
    def test_run_pooling_task(self, mock_pool_full):
        """
        test that running task update task state as  without running task.
        (no dependency check in ti_deps anymore, so also -> SUCCESS)
        """
        # Mock the pool out with a full pool because the pool doesn't actually exist
        mock_pool_full.return_value = True

        dag = models.DAG(dag_id='test_run_pooling_task')
        task = DummyOperator(task_id='test_run_pooling_task_op', dag=dag,
                             pool='test_run_pooling_task_pool', owner='airflow',
                             start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=timezone.utcnow())
        ti.run()
        self.assertEqual(ti.state, models.State.SUCCESS)

    @patch.object(TI, 'pool_full')
    def test_run_pooling_task_with_mark_success(self, mock_pool_full):
        """
        test that running task with mark_success param update task state as SUCCESS
        without running task.
        """
        # Mock the pool out with a full pool because the pool doesn't actually exist
        mock_pool_full.return_value = True

        dag = models.DAG(dag_id='test_run_pooling_task_with_mark_success')
        task = DummyOperator(
            task_id='test_run_pooling_task_with_mark_success_op',
            dag=dag,
            pool='test_run_pooling_task_with_mark_success_pool',
            owner='airflow',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=timezone.utcnow())
        ti.run(mark_success=True)
        self.assertEqual(ti.state, models.State.SUCCESS)

    def test_run_pooling_task_with_skip(self):
        """
        test that running task which returns AirflowSkipOperator will end
        up in a SKIPPED state.
        """

        def raise_skip_exception():
            raise AirflowSkipException

        dag = models.DAG(dag_id='test_run_pooling_task_with_skip')
        task = PythonOperator(
            task_id='test_run_pooling_task_with_skip',
            dag=dag,
            python_callable=raise_skip_exception,
            owner='airflow',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=timezone.utcnow())
        ti.run()
        self.assertEqual(models.State.SKIPPED, ti.state)

    def test_retry_delay(self):
        """
        Test that retry delays are respected
        """
        dag = models.DAG(dag_id='test_retry_handling')
        task = BashOperator(
            task_id='test_retry_handling_op',
            bash_command='exit 1',
            retries=1,
            retry_delay=datetime.timedelta(seconds=3),
            dag=dag,
            owner='airflow',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))

        def run_with_error(ti):
            try:
                ti.run()
            except AirflowException:
                pass

        ti = TI(
            task=task, execution_date=timezone.utcnow())

        self.assertEqual(ti.try_number, 1)
        # first run -- up for retry
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti.try_number, 2)

        # second run -- still up for retry because retry_delay hasn't expired
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)

        # third run -- failed
        time.sleep(3)
        run_with_error(ti)
        self.assertEqual(ti.state, State.FAILED)

    @patch.object(TI, 'pool_full')
    def test_retry_handling(self, mock_pool_full):
        """
        Test that task retries are handled properly
        """
        # Mock the pool with a pool with slots open since the pool doesn't actually exist
        mock_pool_full.return_value = False

        dag = models.DAG(dag_id='test_retry_handling')
        task = BashOperator(
            task_id='test_retry_handling_op',
            bash_command='exit 1',
            retries=1,
            retry_delay=datetime.timedelta(seconds=0),
            dag=dag,
            owner='airflow',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))

        def run_with_error(ti):
            try:
                ti.run()
            except AirflowException:
                pass

        ti = TI(
            task=task, execution_date=timezone.utcnow())
        self.assertEqual(ti.try_number, 1)

        # first run -- up for retry
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti._try_number, 1)
        self.assertEqual(ti.try_number, 2)

        # second run -- fail
        run_with_error(ti)
        self.assertEqual(ti.state, State.FAILED)
        self.assertEqual(ti._try_number, 2)
        self.assertEqual(ti.try_number, 3)

        # Clear the TI state since you can't run a task with a FAILED state without
        # clearing it first
        dag.clear()

        # third run -- up for retry
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti._try_number, 3)
        self.assertEqual(ti.try_number, 4)

        # fourth run -- fail
        run_with_error(ti)
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.FAILED)
        self.assertEqual(ti._try_number, 4)
        self.assertEqual(ti.try_number, 5)

    def test_next_retry_datetime(self):
        delay = datetime.timedelta(seconds=30)
        max_delay = datetime.timedelta(minutes=60)

        dag = models.DAG(dag_id='fail_dag')
        task = BashOperator(
            task_id='task_with_exp_backoff_and_max_delay',
            bash_command='exit 1',
            retries=3,
            retry_delay=delay,
            retry_exponential_backoff=True,
            max_retry_delay=max_delay,
            dag=dag,
            owner='airflow',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=DEFAULT_DATE)
        ti.end_date = pendulum.instance(timezone.utcnow())

        dt = ti.next_retry_datetime()
        # between 30 * 2^0.5 and 30 * 2^1 (15 and 30)
        period = ti.end_date.add(seconds=30) - ti.end_date.add(seconds=15)
        self.assertTrue(dt in period)

        ti.try_number = 3
        dt = ti.next_retry_datetime()
        # between 30 * 2^2 and 30 * 2^3 (120 and 240)
        period = ti.end_date.add(seconds=240) - ti.end_date.add(seconds=120)
        self.assertTrue(dt in period)

        ti.try_number = 5
        dt = ti.next_retry_datetime()
        # between 30 * 2^4 and 30 * 2^5 (480 and 960)
        period = ti.end_date.add(seconds=960) - ti.end_date.add(seconds=480)
        self.assertTrue(dt in period)

        ti.try_number = 9
        dt = ti.next_retry_datetime()
        self.assertEqual(dt, ti.end_date+max_delay)

        ti.try_number = 50
        dt = ti.next_retry_datetime()
        self.assertEqual(dt, ti.end_date+max_delay)

    def test_depends_on_past(self):
        dagbag = models.DagBag()
        dag = dagbag.get_dag('test_depends_on_past')
        dag.clear()
        task = dag.tasks[0]
        run_date = task.start_date + datetime.timedelta(days=5)
        ti = TI(task, run_date)

        # depends_on_past prevents the run
        task.run(start_date=run_date, end_date=run_date)
        ti.refresh_from_db()
        self.assertIs(ti.state, None)

        # ignore first depends_on_past to allow the run
        task.run(
            start_date=run_date,
            end_date=run_date,
            ignore_first_depends_on_past=True)
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.SUCCESS)

    # Parameterized tests to check for the correct firing
    # of the trigger_rule under various circumstances
    # Numeric fields are in order:
    #   successes, skipped, failed, upstream_failed, done
    @parameterized.expand([

        #
        # Tests for all_success
        #
        ['all_success', 5, 0, 0, 0, 0, True, None, True],
        ['all_success', 2, 0, 0, 0, 0, True, None, False],
        ['all_success', 2, 0, 1, 0, 0, True, ST.UPSTREAM_FAILED, False],
        ['all_success', 2, 1, 0, 0, 0, True, ST.SKIPPED, False],
        #
        # Tests for one_success
        #
        ['one_success', 5, 0, 0, 0, 5, True, None, True],
        ['one_success', 2, 0, 0, 0, 2, True, None, True],
        ['one_success', 2, 0, 1, 0, 3, True, None, True],
        ['one_success', 2, 1, 0, 0, 3, True, None, True],
        #
        # Tests for all_failed
        #
        ['all_failed', 5, 0, 0, 0, 5, True, ST.SKIPPED, False],
        ['all_failed', 0, 0, 5, 0, 5, True, None, True],
        ['all_failed', 2, 0, 0, 0, 2, True, ST.SKIPPED, False],
        ['all_failed', 2, 0, 1, 0, 3, True, ST.SKIPPED, False],
        ['all_failed', 2, 1, 0, 0, 3, True, ST.SKIPPED, False],
        #
        # Tests for one_failed
        #
        ['one_failed', 5, 0, 0, 0, 0, True, None, False],
        ['one_failed', 2, 0, 0, 0, 0, True, None, False],
        ['one_failed', 2, 0, 1, 0, 0, True, None, True],
        ['one_failed', 2, 1, 0, 0, 3, True, None, False],
        ['one_failed', 2, 3, 0, 0, 5, True, ST.SKIPPED, False],
        #
        # Tests for done
        #
        ['all_done', 5, 0, 0, 0, 5, True, None, True],
        ['all_done', 2, 0, 0, 0, 2, True, None, False],
        ['all_done', 2, 0, 1, 0, 3, True, None, False],
        ['all_done', 2, 1, 0, 0, 3, True, None, False]
    ])
    def test_check_task_dependencies(self, trigger_rule, successes, skipped,
                                     failed, upstream_failed, done,
                                     flag_upstream_failed,
                                     expect_state, expect_completed):
        start_date = timezone.datetime(2016, 2, 1, 0, 0, 0)
        dag = models.DAG('test-dag', start_date=start_date)
        downstream = DummyOperator(task_id='downstream',
                                   dag=dag, owner='airflow',
                                   trigger_rule=trigger_rule)
        for i in range(5):
            task = DummyOperator(task_id='runme_{}'.format(i),
                                 dag=dag, owner='airflow')
            task.set_downstream(downstream)
        run_date = task.start_date + datetime.timedelta(days=5)

        ti = TI(downstream, run_date)
        dep_results = TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=successes,
            skipped=skipped,
            failed=failed,
            upstream_failed=upstream_failed,
            done=done,
            flag_upstream_failed=flag_upstream_failed)
        completed = all([dep.passed for dep in dep_results])

        self.assertEqual(completed, expect_completed)
        self.assertEqual(ti.state, expect_state)

    def test_xcom_pull(self):
        """
        Test xcom_pull, using different filtering methods.
        """
        dag = models.DAG(
            dag_id='test_xcom', schedule_interval='@monthly',
            start_date=timezone.datetime(2016, 6, 1, 0, 0, 0))

        exec_date = timezone.utcnow()

        # Push a value
        task1 = DummyOperator(task_id='test_xcom_1', dag=dag, owner='airflow')
        ti1 = TI(task=task1, execution_date=exec_date)
        ti1.xcom_push(key='foo', value='bar')

        # Push another value with the same key (but by a different task)
        task2 = DummyOperator(task_id='test_xcom_2', dag=dag, owner='airflow')
        ti2 = TI(task=task2, execution_date=exec_date)
        ti2.xcom_push(key='foo', value='baz')

        # Pull with no arguments
        result = ti1.xcom_pull()
        self.assertEqual(result, None)
        # Pull the value pushed most recently by any task.
        result = ti1.xcom_pull(key='foo')
        self.assertIn(result, 'baz')
        # Pull the value pushed by the first task
        result = ti1.xcom_pull(task_ids='test_xcom_1', key='foo')
        self.assertEqual(result, 'bar')
        # Pull the value pushed by the second task
        result = ti1.xcom_pull(task_ids='test_xcom_2', key='foo')
        self.assertEqual(result, 'baz')
        # Pull the values pushed by both tasks
        result = ti1.xcom_pull(
            task_ids=['test_xcom_1', 'test_xcom_2'], key='foo')
        self.assertEqual(result, ('bar', 'baz'))

    def test_xcom_pull_after_success(self):
        """
        tests xcom set/clear relative to a task in a 'success' rerun scenario
        """
        key = 'xcom_key'
        value = 'xcom_value'

        dag = models.DAG(dag_id='test_xcom', schedule_interval='@monthly')
        task = DummyOperator(
            task_id='test_xcom',
            dag=dag,
            pool='test_xcom',
            owner='airflow',
            start_date=timezone.datetime(2016, 6, 2, 0, 0, 0))
        exec_date = timezone.utcnow()
        ti = TI(
            task=task, execution_date=exec_date)
        ti.run(mark_success=True)
        ti.xcom_push(key=key, value=value)
        self.assertEqual(ti.xcom_pull(task_ids='test_xcom', key=key), value)
        ti.run()
        # The second run and assert is to handle AIRFLOW-131 (don't clear on
        # prior success)
        self.assertEqual(ti.xcom_pull(task_ids='test_xcom', key=key), value)

        # Test AIRFLOW-703: Xcom shouldn't be cleared if the task doesn't
        # execute, even if dependencies are ignored
        ti.run(ignore_all_deps=True, mark_success=True)
        self.assertEqual(ti.xcom_pull(task_ids='test_xcom', key=key), value)
        # Xcom IS finally cleared once task has executed
        ti.run(ignore_all_deps=True)
        self.assertEqual(ti.xcom_pull(task_ids='test_xcom', key=key), None)

    def test_xcom_pull_different_execution_date(self):
        """
        tests xcom fetch behavior with different execution dates, using
        both xcom_pull with "include_prior_dates" and without
        """
        key = 'xcom_key'
        value = 'xcom_value'

        dag = models.DAG(dag_id='test_xcom', schedule_interval='@monthly')
        task = DummyOperator(
            task_id='test_xcom',
            dag=dag,
            pool='test_xcom',
            owner='airflow',
            start_date=timezone.datetime(2016, 6, 2, 0, 0, 0))
        exec_date = timezone.utcnow()
        ti = TI(
            task=task, execution_date=exec_date)
        ti.run(mark_success=True)
        ti.xcom_push(key=key, value=value)
        self.assertEqual(ti.xcom_pull(task_ids='test_xcom', key=key), value)
        ti.run()
        exec_date += datetime.timedelta(days=1)
        ti = TI(
            task=task, execution_date=exec_date)
        ti.run()
        # We have set a new execution date (and did not pass in
        # 'include_prior_dates'which means this task should now have a cleared
        # xcom value
        self.assertEqual(ti.xcom_pull(task_ids='test_xcom', key=key), None)
        # We *should* get a value using 'include_prior_dates'
        self.assertEqual(ti.xcom_pull(task_ids='test_xcom',
                                      key=key,
                                      include_prior_dates=True),
                         value)

    def test_post_execute_hook(self):
        """
        Test that post_execute hook is called with the Operator's result.
        The result ('error') will cause an error to be raised and trapped.
        """

        class TestError(Exception):
            pass

        class TestOperator(PythonOperator):
            def post_execute(self, context, result):
                if result == 'error':
                    raise TestError('expected error.')

        dag = models.DAG(dag_id='test_post_execute_dag')
        task = TestOperator(
            task_id='test_operator',
            dag=dag,
            python_callable=lambda: 'error',
            owner='airflow',
            start_date=timezone.datetime(2017, 2, 1))
        ti = TI(task=task, execution_date=timezone.utcnow())

        with self.assertRaises(TestError):
            ti.run()

    def test_check_and_change_state_before_execution(self):
        dag = models.DAG(dag_id='test_check_and_change_state_before_execution')
        task = DummyOperator(task_id='task', dag=dag, start_date=DEFAULT_DATE)
        ti = TI(
            task=task, execution_date=timezone.utcnow())
        self.assertEqual(ti._try_number, 0)
        self.assertTrue(ti._check_and_change_state_before_execution())
        # State should be running, and try_number column should be incremented
        self.assertEqual(ti.state, State.RUNNING)
        self.assertEqual(ti._try_number, 1)

    def test_check_and_change_state_before_execution_dep_not_met(self):
        dag = models.DAG(dag_id='test_check_and_change_state_before_execution')
        task = DummyOperator(task_id='task', dag=dag, start_date=DEFAULT_DATE)
        task2= DummyOperator(task_id='task2', dag=dag, start_date=DEFAULT_DATE)
        task >> task2
        ti = TI(
            task=task2, execution_date=timezone.utcnow())
        self.assertFalse(ti._check_and_change_state_before_execution())

    def test_try_number(self):
        """
        Test the try_number accessor behaves in various running states
        """
        dag = models.DAG(dag_id='test_check_and_change_state_before_execution')
        task = DummyOperator(task_id='task', dag=dag, start_date=DEFAULT_DATE)
        ti = TI(task=task, execution_date=timezone.utcnow())
        self.assertEqual(1, ti.try_number)
        ti.try_number = 2
        ti.state = State.RUNNING
        self.assertEqual(2, ti.try_number)
        ti.state = State.SUCCESS
        self.assertEqual(3, ti.try_number)

    def test_get_num_running_task_instances(self):
        session = settings.Session()

        dag = models.DAG(dag_id='test_get_num_running_task_instances')
        dag2 = models.DAG(dag_id='test_get_num_running_task_instances_dummy')
        task = DummyOperator(task_id='task', dag=dag, start_date=DEFAULT_DATE)
        task2 = DummyOperator(task_id='task', dag=dag2, start_date=DEFAULT_DATE)

        ti1 = TI(task=task, execution_date=DEFAULT_DATE)
        ti2 = TI(task=task, execution_date=DEFAULT_DATE + datetime.timedelta(days=1))
        ti3 = TI(task=task2, execution_date=DEFAULT_DATE)
        ti1.state = State.RUNNING
        ti2.state = State.QUEUED
        ti3.state = State.RUNNING
        session.add(ti1)
        session.add(ti2)
        session.add(ti3)
        session.commit()

        self.assertEquals(1, ti1.get_num_running_task_instances(session=session))
        self.assertEquals(1, ti2.get_num_running_task_instances(session=session))
        self.assertEquals(1, ti3.get_num_running_task_instances(session=session))

    def test_log_url(self):
        now = pendulum.now('Europe/Brussels')
        dag = DAG('dag', start_date=DEFAULT_DATE)
        task = DummyOperator(task_id='op', dag=dag)
        ti = TI(task=task, execution_date=now)
        d = urllib.parse.parse_qs(
            urllib.parse.urlparse(ti.log_url).query,
            keep_blank_values=True, strict_parsing=True)
        self.assertEqual(d['dag_id'][0], 'dag')
        self.assertEqual(d['task_id'][0], 'op')
        self.assertEqual(pendulum.parse(d['execution_date'][0]), now)

    def test_mark_success_url(self):
        now = pendulum.now('Europe/Brussels')
        dag = DAG('dag', start_date=DEFAULT_DATE)
        task = DummyOperator(task_id='op', dag=dag)
        ti = TI(task=task, execution_date=now)
        d = urllib.parse.parse_qs(
            urllib.parse.urlparse(ti.mark_success_url).query,
            keep_blank_values=True, strict_parsing=True)
        self.assertEqual(d['dag_id'][0], 'dag')
        self.assertEqual(d['task_id'][0], 'op')
        self.assertEqual(pendulum.parse(d['execution_date'][0]), now)

    def test_overwrite_params_with_dag_run_conf(self):
        task = DummyOperator(task_id='op')
        ti = TI(task=task, execution_date=datetime.datetime.now())
        dag_run = DagRun()
        dag_run.conf = {"override": True}
        params = {"override": False}

        ti.overwrite_params_with_dag_run_conf(params, dag_run)

        self.assertEqual(True, params["override"])

    def test_overwrite_params_with_dag_run_none(self):
        task = DummyOperator(task_id='op')
        ti = TI(task=task, execution_date=datetime.datetime.now())
        params = {"override": False}

        ti.overwrite_params_with_dag_run_conf(params, None)

        self.assertEqual(False, params["override"])

    def test_overwrite_params_with_dag_run_conf_none(self):
        task = DummyOperator(task_id='op')
        ti = TI(task=task, execution_date=datetime.datetime.now())
        params = {"override": False}
        dag_run = DagRun()

        ti.overwrite_params_with_dag_run_conf(params, dag_run)

        self.assertEqual(False, params["override"])


class ClearTasksTest(unittest.TestCase):

    def test_clear_task_instances(self):
        dag = DAG('test_clear_task_instances', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        task0 = DummyOperator(task_id='0', owner='test', dag=dag)
        task1 = DummyOperator(task_id='1', owner='test', dag=dag, retries=2)
        ti0 = TI(task=task0, execution_date=DEFAULT_DATE)
        ti1 = TI(task=task1, execution_date=DEFAULT_DATE)

        ti0.run()
        ti1.run()
        session = settings.Session()
        qry = session.query(TI).filter(
            TI.dag_id == dag.dag_id).all()
        clear_task_instances(qry, session, dag=dag)
        session.commit()
        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # Next try to run will be try 2
        self.assertEqual(ti0.try_number, 2)
        self.assertEqual(ti0.max_tries, 1)
        self.assertEqual(ti1.try_number, 2)
        self.assertEqual(ti1.max_tries, 3)

    def test_clear_task_instances_without_task(self):
        dag = DAG('test_clear_task_instances_without_task', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        task0 = DummyOperator(task_id='task0', owner='test', dag=dag)
        task1 = DummyOperator(task_id='task1', owner='test', dag=dag, retries=2)
        ti0 = TI(task=task0, execution_date=DEFAULT_DATE)
        ti1 = TI(task=task1, execution_date=DEFAULT_DATE)
        ti0.run()
        ti1.run()

        # Remove the task from dag.
        dag.task_dict = {}
        self.assertFalse(dag.has_task(task0.task_id))
        self.assertFalse(dag.has_task(task1.task_id))

        session = settings.Session()
        qry = session.query(TI).filter(
            TI.dag_id == dag.dag_id).all()
        clear_task_instances(qry, session)
        session.commit()
        # When dag is None, max_tries will be maximum of original max_tries or try_number.
        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # Next try to run will be try 2
        self.assertEqual(ti0.try_number, 2)
        self.assertEqual(ti0.max_tries, 1)
        self.assertEqual(ti1.try_number, 2)
        self.assertEqual(ti1.max_tries, 2)

    def test_clear_task_instances_without_dag(self):
        dag = DAG('test_clear_task_instances_without_dag', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        task0 = DummyOperator(task_id='task_0', owner='test', dag=dag)
        task1 = DummyOperator(task_id='task_1', owner='test', dag=dag, retries=2)
        ti0 = TI(task=task0, execution_date=DEFAULT_DATE)
        ti1 = TI(task=task1, execution_date=DEFAULT_DATE)
        ti0.run()
        ti1.run()

        session = settings.Session()
        qry = session.query(TI).filter(
            TI.dag_id == dag.dag_id).all()
        clear_task_instances(qry, session)
        session.commit()
        # When dag is None, max_tries will be maximum of original max_tries or try_number.
        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # Next try to run will be try 2
        self.assertEqual(ti0.try_number, 2)
        self.assertEqual(ti0.max_tries, 1)
        self.assertEqual(ti1.try_number, 2)
        self.assertEqual(ti1.max_tries, 2)

    def test_dag_clear(self):
        dag = DAG('test_dag_clear', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        task0 = DummyOperator(task_id='test_dag_clear_task_0', owner='test', dag=dag)
        ti0 = TI(task=task0, execution_date=DEFAULT_DATE)
        # Next try to run will be try 1
        self.assertEqual(ti0.try_number, 1)
        ti0.run()
        self.assertEqual(ti0.try_number, 2)
        dag.clear()
        ti0.refresh_from_db()
        self.assertEqual(ti0.try_number, 2)
        self.assertEqual(ti0.state, State.NONE)
        self.assertEqual(ti0.max_tries, 1)

        task1 = DummyOperator(task_id='test_dag_clear_task_1', owner='test',
                              dag=dag, retries=2)
        ti1 = TI(task=task1, execution_date=DEFAULT_DATE)
        self.assertEqual(ti1.max_tries, 2)
        ti1.try_number = 1
        # Next try will be 2
        ti1.run()
        self.assertEqual(ti1.try_number, 3)
        self.assertEqual(ti1.max_tries, 2)

        dag.clear()
        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # after clear dag, ti2 should show attempt 3 of 5
        self.assertEqual(ti1.max_tries, 4)
        self.assertEqual(ti1.try_number, 3)
        # after clear dag, ti1 should show attempt 2 of 2
        self.assertEqual(ti0.try_number, 2)
        self.assertEqual(ti0.max_tries, 1)

    def test_dags_clear(self):
        # setup
        session = settings.Session()
        dags, tis = [], []
        num_of_dags = 5
        for i in range(num_of_dags):
            dag = DAG('test_dag_clear_' + str(i), start_date=DEFAULT_DATE,
                      end_date=DEFAULT_DATE + datetime.timedelta(days=10))
            ti = TI(task=DummyOperator(task_id='test_task_clear_' + str(i), owner='test', dag=dag),
                    execution_date=DEFAULT_DATE)
            dags.append(dag)
            tis.append(ti)

        # test clear all dags
        for i in range(num_of_dags):
            tis[i].run()
            self.assertEqual(tis[i].state, State.SUCCESS)
            self.assertEqual(tis[i].try_number, 2)
            self.assertEqual(tis[i].max_tries, 0)

        DAG.clear_dags(dags)

        for i in range(num_of_dags):
            tis[i].refresh_from_db()
            self.assertEqual(tis[i].state, State.NONE)
            self.assertEqual(tis[i].try_number, 2)
            self.assertEqual(tis[i].max_tries, 1)

        # test dry_run
        for i in range(num_of_dags):
            tis[i].run()
            self.assertEqual(tis[i].state, State.SUCCESS)
            self.assertEqual(tis[i].try_number, 3)
            self.assertEqual(tis[i].max_tries, 1)

        DAG.clear_dags(dags, dry_run=True)

        for i in range(num_of_dags):
            tis[i].refresh_from_db()
            self.assertEqual(tis[i].state, State.SUCCESS)
            self.assertEqual(tis[i].try_number, 3)
            self.assertEqual(tis[i].max_tries, 1)

        # test only_failed
        from random import randint
        failed_dag_idx = randint(0, len(tis) - 1)
        tis[failed_dag_idx].state = State.FAILED
        session.merge(tis[failed_dag_idx])
        session.commit()

        DAG.clear_dags(dags, only_failed=True)

        for i in range(num_of_dags):
            tis[i].refresh_from_db()
            if i != failed_dag_idx:
                self.assertEqual(tis[i].state, State.SUCCESS)
                self.assertEqual(tis[i].try_number, 3)
                self.assertEqual(tis[i].max_tries, 1)
            else:
                self.assertEqual(tis[i].state, State.NONE)
                self.assertEqual(tis[i].try_number, 3)
                self.assertEqual(tis[i].max_tries, 2)

    def test_operator_clear(self):
        dag = DAG('test_operator_clear', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        t1 = DummyOperator(task_id='bash_op', owner='test', dag=dag)
        t2 = DummyOperator(task_id='dummy_op', owner='test', dag=dag, retries=1)

        t2.set_upstream(t1)

        ti1 = TI(task=t1, execution_date=DEFAULT_DATE)
        ti2 = TI(task=t2, execution_date=DEFAULT_DATE)
        ti2.run()
        # Dependency not met
        self.assertEqual(ti2.try_number, 1)
        self.assertEqual(ti2.max_tries, 1)

        t2.clear(upstream=True)
        ti1.run()
        ti2.run()
        self.assertEqual(ti1.try_number, 2)
        # max_tries is 0 because there is no task instance in db for ti1
        # so clear won't change the max_tries.
        self.assertEqual(ti1.max_tries, 0)
        self.assertEqual(ti2.try_number, 2)
        # try_number (0) + retries(1)
        self.assertEqual(ti2.max_tries, 1)

    def test_xcom_disable_pickle_type(self):
        configuration.load_test_config()

        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = "xcom_test1"
        dag_id = "test_dag1"
        task_id = "test_task1"

        configuration.set("core", "enable_xcom_pickling", "False")

        XCom.set(key=key,
                 value=json_obj,
                 dag_id=dag_id,
                 task_id=task_id,
                 execution_date=execution_date)

        ret_value = XCom.get_one(key=key,
                                 dag_id=dag_id,
                                 task_id=task_id,
                                 execution_date=execution_date)

        self.assertEqual(ret_value, json_obj)

        session = settings.Session()
        ret_value = session.query(XCom).filter(XCom.key == key, XCom.dag_id == dag_id,
                                               XCom.task_id == task_id,
                                               XCom.execution_date == execution_date
                                               ).first().value

        self.assertEqual(ret_value, json_obj)

    def test_xcom_enable_pickle_type(self):
        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = "xcom_test2"
        dag_id = "test_dag2"
        task_id = "test_task2"

        configuration.set("core", "enable_xcom_pickling", "True")

        XCom.set(key=key,
                 value=json_obj,
                 dag_id=dag_id,
                 task_id=task_id,
                 execution_date=execution_date)

        ret_value = XCom.get_one(key=key,
                                 dag_id=dag_id,
                                 task_id=task_id,
                                 execution_date=execution_date)

        self.assertEqual(ret_value, json_obj)

        session = settings.Session()
        ret_value = session.query(XCom).filter(XCom.key == key, XCom.dag_id == dag_id,
                                               XCom.task_id == task_id,
                                               XCom.execution_date == execution_date
                                               ).first().value

        self.assertEqual(ret_value, json_obj)

    def test_xcom_disable_pickle_type_fail_on_non_json(self):
        class PickleRce(object):
            def __reduce__(self):
                return os.system, ("ls -alt",)

        configuration.set("core", "xcom_enable_pickling", "False")

        self.assertRaises(TypeError, XCom.set,
                          key="xcom_test3",
                          value=PickleRce(),
                          dag_id="test_dag3",
                          task_id="test_task3",
                          execution_date=timezone.utcnow())

    def test_xcom_get_many(self):
        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = "xcom_test4"
        dag_id1 = "test_dag4"
        task_id1 = "test_task4"
        dag_id2 = "test_dag5"
        task_id2 = "test_task5"

        configuration.set("core", "xcom_enable_pickling", "True")

        XCom.set(key=key,
                 value=json_obj,
                 dag_id=dag_id1,
                 task_id=task_id1,
                 execution_date=execution_date)

        XCom.set(key=key,
                 value=json_obj,
                 dag_id=dag_id2,
                 task_id=task_id2,
                 execution_date=execution_date)

        results = XCom.get_many(key=key,
                                execution_date=execution_date)

        for result in results:
            self.assertEqual(result.value, json_obj)


class ConnectionTest(unittest.TestCase):
    @patch.object(configuration, 'get')
    def test_connection_extra_no_encryption(self, mock_get):
        """
        Tests extras on a new connection without encryption. The fernet key
        is set to a non-base64-encoded string and the extra is stored without
        encryption.
        """
        mock_get.return_value = 'cryptography_not_found_storing_passwords_in_plain_text'
        test_connection = Connection(extra='testextra')
        self.assertEqual(test_connection.extra, 'testextra')

    @patch.object(configuration, 'get')
    def test_connection_extra_with_encryption(self, mock_get):
        """
        Tests extras on a new connection with encryption. The fernet key
        is set to a base64 encoded string and the extra is encrypted.
        """
        # 'dGVzdA==' is base64 encoded 'test'
        mock_get.return_value = 'dGVzdA=='
        test_connection = Connection(extra='testextra')
        self.assertEqual(test_connection.extra, 'testextra')

    def test_connection_from_uri_without_extras(self):
        uri = 'scheme://user:password@host%2flocation:1234/schema'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host/location')
        self.assertEqual(connection.schema, 'schema')
        self.assertEqual(connection.login, 'user')
        self.assertEqual(connection.password, 'password')
        self.assertEqual(connection.port, 1234)
        self.assertIsNone(connection.extra)

    def test_connection_from_uri_with_extras(self):
        uri = 'scheme://user:password@host%2flocation:1234/schema?'\
            'extra1=a%20value&extra2=%2fpath%2f'
        connection = Connection(uri=uri)
        self.assertEqual(connection.conn_type, 'scheme')
        self.assertEqual(connection.host, 'host/location')
        self.assertEqual(connection.schema, 'schema')
        self.assertEqual(connection.login, 'user')
        self.assertEqual(connection.password, 'password')
        self.assertEqual(connection.port, 1234)
        self.assertDictEqual(connection.extra_dejson, {'extra1': 'a value',
                                                       'extra2': '/path/'})
