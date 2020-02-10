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

from airflow import models, settings
from airflow.jobs import BackfillJob
from airflow.models import DAG, DagRun, TaskInstance as TI, clear_task_instances
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule
from tests.models import DEFAULT_DATE


class TestDagRun(unittest.TestCase):

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
            for task_id, task_state in task_states.items():
                ti = dag_run.get_task_instance(task_id)
                ti.set_state(task_state, session)
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
        self.assertEqual(dr0.state, State.RUNNING)

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

        self.assertEqual(1,
                         len(models.DagRun.find(dag_id=dag_id1, external_trigger=True)))
        self.assertEqual(0,
                         len(models.DagRun.find(dag_id=dag_id1, external_trigger=False)))
        self.assertEqual(0,
                         len(models.DagRun.find(dag_id=dag_id2, external_trigger=True)))
        self.assertEqual(1,
                         len(models.DagRun.find(dag_id=dag_id2, external_trigger=False)))

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
        dag_run.update_state()
        self.assertEqual(State.SUCCESS, dag_run.state)

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
        dr.update_state()
        self.assertEqual(State.RUNNING, dr.state)

        # one has failed, but root is successful
        ti_op2.set_state(state=State.FAILED, session=session)
        ti_op3.set_state(state=State.SUCCESS, session=session)
        ti_op4.set_state(state=State.SUCCESS, session=session)
        dr.update_state()
        self.assertEqual(State.SUCCESS, dr.state)

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
            DummyOperator(task_id='dop', depends_on_past=True)
            DummyOperator(task_id='tc', task_concurrency=1)

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
        dr2.get_task_instance(task_id='dop')
        ti2_op1 = dr.get_task_instance(task_id='tc')
        dr.get_task_instance(task_id='tc')
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
        dag_run.update_state()
        self.assertEqual(State.SUCCESS, dag_run.state)

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
        dag_run.update_state()
        self.assertEqual(State.FAILED, dag_run.state)

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
        ShortCircuitOperator(
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
        self.create_dag_run(dag, execution_date=timezone.datetime(2015, 1, 1))
        self.create_dag_run(dag, execution_date=timezone.datetime(2015, 1, 2))
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
        self.assertEqual('flaky_task', flaky_ti.task_id)
        self.assertEqual(State.NONE, flaky_ti.state)

        dagrun.dag = with_all_tasks_removed(dag)

        dagrun.verify_integrity()
        flaky_ti.refresh_from_db()
        self.assertEqual(State.NONE, flaky_ti.state)

        dagrun.dag.add_task(DummyOperator(task_id='flaky_task', owner='test'))

        dagrun.verify_integrity()
        flaky_ti.refresh_from_db()
        self.assertEqual(State.NONE, flaky_ti.state)
