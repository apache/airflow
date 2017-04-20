# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import logging
import os
import unittest
import time

from airflow import models, settings, AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.models import DAG, TaskInstance as TI
from airflow.models import State as ST
from airflow.models import DagModel, DagStat
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils.state import State
from mock import patch
from nose_parameterized import parameterized


DEFAULT_DATE = datetime.datetime(2016, 1, 1)
TEST_DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'dags')


class DagTest(unittest.TestCase):

    def test_parms_not_passed_is_empty_dict(self):
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
        the DAG (unless they specifiy a different DAG)
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

        now = datetime.datetime.now()
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

    def create_dag_run(self, dag, state=State.RUNNING, task_states=None, execution_date=None):
        now = datetime.datetime.now()
        if execution_date is None:
            execution_date = now
        dag_run = dag.create_dagrun(
            run_id='manual__' + now.isoformat(),
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

    def test_id_for_date(self):
        run_id = models.DagRun.id_for_date(
            datetime.datetime(2015, 1, 2, 3, 4, 5, 6, None))
        self.assertEqual(
            'scheduled__2015-01-02T03:04:05', run_id,
            'Generated run_id did not match expectations: {0}'.format(run_id))

    def test_dagrun_find(self):
        session = settings.Session()
        now = datetime.datetime.now()

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
            start_date=datetime.datetime(2017, 1, 1)
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

        now = datetime.datetime.now()
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

        # upstream dependency failed, root has not run
        ti_op1.set_state(State.NONE, session)
        state = dr.update_state()
        self.assertEqual(State.FAILED, state)

    def test_get_task_instance_on_empty_dagrun(self):
        """
        Make sure that a proper value is returned when a dagrun has no task instances
        """
        dag = DAG(
            dag_id='test_get_task_instance_on_empty_dagrun',
            start_date=datetime.datetime(2017, 1, 1)
        )
        dag_task1 = ShortCircuitOperator(
            task_id='test_short_circuit_false',
            dag=dag,
            python_callable=lambda: False)

        session = settings.Session()

        now = datetime.datetime.now()

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
                execution_date=datetime.datetime(2015, 1, 1))
        dag_1_run_2 = self.create_dag_run(dag,
                execution_date=datetime.datetime(2015, 1, 2))
        dagruns = models.DagRun.get_latest_runs(session)
        session.close()
        for dagrun in dagruns:
            if dagrun.dag_id == 'test_latest_runs_1':
                self.assertEqual(dagrun.execution_date, datetime.datetime(2015, 1, 2))


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
        from tempfile import NamedTemporaryFile
        f = NamedTemporaryFile()
        f.write('\u3042'.encode('utf8'))  # write multi-byte char (hiragana)
        f.flush()

        dagbag = models.DagBag(include_examples=True)
        self.assertEqual([], dagbag.process_file(f.name))

    def test_zip(self):
        """
        test the loading of a DAG within a zip file that includes dependencies
        """
        dagbag = models.DagBag()
        dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, "test_zip.zip"))
        self.assertTrue(dagbag.get_dag("test_zip_dag"))

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


class TaskInstanceTest(unittest.TestCase):

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

        ti = TI(task=task, execution_date=datetime.datetime.now())
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
                             start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=datetime.datetime.now())
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
            start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=datetime.datetime.now())
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
            start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=datetime.datetime.now())
        ti.run()
        self.assertTrue(ti.state == models.State.SKIPPED)

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
            start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))

        def run_with_error(ti):
            try:
                ti.run()
            except AirflowException:
                pass

        ti = TI(
            task=task, execution_date=datetime.datetime.now())

        # first run -- up for retry
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti.try_number, 1)

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
            start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))

        def run_with_error(ti):
            try:
                ti.run()
            except AirflowException:
                pass

        ti = TI(
            task=task, execution_date=datetime.datetime.now())

        # first run -- up for retry
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti.try_number, 1)

        # second run -- fail
        run_with_error(ti)
        self.assertEqual(ti.state, State.FAILED)
        self.assertEqual(ti.try_number, 2)

        # Clear the TI state since you can't run a task with a FAILED state without
        # clearing it first
        ti.set_state(None, settings.Session())

        # third run -- up for retry
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti.try_number, 3)

        # fourth run -- fail
        run_with_error(ti)
        self.assertEqual(ti.state, State.FAILED)
        self.assertEqual(ti.try_number, 4)

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
            start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=DEFAULT_DATE)
        ti.end_date = datetime.datetime.now()

        ti.try_number = 1
        dt = ti.next_retry_datetime()
        # between 30 * 2^0.5 and 30 * 2^1 (15 and 30)
        self.assertEqual(dt, ti.end_date + datetime.timedelta(seconds=20.0))

        ti.try_number = 4
        dt = ti.next_retry_datetime()
        # between 30 * 2^2 and 30 * 2^3 (120 and 240)
        self.assertEqual(dt, ti.end_date + datetime.timedelta(seconds=181.0))

        ti.try_number = 6
        dt = ti.next_retry_datetime()
        # between 30 * 2^4 and 30 * 2^5 (480 and 960)
        self.assertEqual(dt, ti.end_date + datetime.timedelta(seconds=825.0))

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
        start_date = datetime.datetime(2016, 2, 1, 0, 0, 0)
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
            start_date=datetime.datetime(2016, 6, 2, 0, 0, 0))
        exec_date = datetime.datetime.now()
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
            start_date=datetime.datetime(2016, 6, 2, 0, 0, 0))
        exec_date = datetime.datetime.now()
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
            start_date=datetime.datetime(2017, 2, 1))
        ti = TI(task=task, execution_date=datetime.datetime.now())

        with self.assertRaises(TestError):
            ti.run()
