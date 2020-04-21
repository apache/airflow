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

import multiprocessing
import os
import pickle  # type: ignore
import signal
import unittest
from datetime import timedelta
from time import sleep
from unittest import mock

import sqlalchemy
from dateutil.relativedelta import relativedelta
from numpy.testing import assert_array_almost_equal
from pendulum import utcnow

from airflow import DAG, configuration, exceptions, jobs, settings, utils
from airflow.bin import cli
from airflow.configuration import AirflowConfigException, conf, run_command
from airflow.exceptions import AirflowException
from airflow.executors import SequentialExecutor
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.models import BaseOperator, Connection, DagBag, DagRun, Pool, TaskFail, TaskInstance, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.check_operator import CheckOperator, ValueCheckOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.settings import Session
from airflow.utils import timezone
from airflow.utils.dates import days_ago, infer_time_unit, round_time, scale_time_units
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from tests.test_utils.config import conf_vars

DEV_NULL = '/dev/null'
TEST_DAG_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'dags')
DEFAULT_DATE = datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = 'unit_tests'
EXAMPLE_DAG_DEFAULT_DATE = days_ago(2)


class OperatorSubclass(BaseOperator):
    """
    An operator to test template substitution
    """
    template_fields = ['some_templated_field']

    def __init__(self, some_templated_field, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.some_templated_field = some_templated_field

    def execute(self, context):
        pass


class TestCore(unittest.TestCase):
    TEST_SCHEDULE_WITH_NO_PREVIOUS_RUNS_DAG_ID = TEST_DAG_ID + 'test_schedule_dag_no_previous_runs'
    TEST_SCHEDULE_DAG_FAKE_SCHEDULED_PREVIOUS_DAG_ID = \
        TEST_DAG_ID + 'test_schedule_dag_fake_scheduled_previous'
    TEST_SCHEDULE_DAG_NO_END_DATE_UP_TO_TODAY_ONLY_DAG_ID = \
        TEST_DAG_ID + 'test_schedule_dag_no_end_date_up_to_today_only'
    TEST_SCHEDULE_ONCE_DAG_ID = TEST_DAG_ID + 'test_schedule_dag_once'
    TEST_SCHEDULE_RELATIVEDELTA_DAG_ID = TEST_DAG_ID + 'test_schedule_dag_relativedelta'
    TEST_SCHEDULE_START_END_DATES_DAG_ID = TEST_DAG_ID + 'test_schedule_dag_start_end_dates'

    default_scheduler_args = {"num_runs": 1}

    def setUp(self):
        self.dagbag = DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, default_args=self.args)
        self.dag_bash = self.dagbag.dags['example_bash_operator']
        self.runme_0 = self.dag_bash.get_task('runme_0')
        self.run_after_loop = self.dag_bash.get_task('run_after_loop')
        self.run_this_last = self.dag_bash.get_task('run_this_last')

    def tearDown(self):
        if os.environ.get('KUBERNETES_VERSION') is not None:
            return

        dag_ids_to_clean = [
            TEST_DAG_ID,
            self.TEST_SCHEDULE_WITH_NO_PREVIOUS_RUNS_DAG_ID,
            self.TEST_SCHEDULE_DAG_FAKE_SCHEDULED_PREVIOUS_DAG_ID,
            self.TEST_SCHEDULE_DAG_NO_END_DATE_UP_TO_TODAY_ONLY_DAG_ID,
            self.TEST_SCHEDULE_ONCE_DAG_ID,
            self.TEST_SCHEDULE_RELATIVEDELTA_DAG_ID,
            self.TEST_SCHEDULE_START_END_DATES_DAG_ID,
        ]
        session = Session()
        session.query(DagRun).filter(
            DagRun.dag_id.in_(dag_ids_to_clean)).delete(
            synchronize_session=False)
        session.query(TaskInstance).filter(
            TaskInstance.dag_id.in_(dag_ids_to_clean)).delete(
            synchronize_session=False)
        session.query(TaskFail).filter(
            TaskFail.dag_id.in_(dag_ids_to_clean)).delete(
            synchronize_session=False)
        session.commit()
        session.close()

    def test_schedule_dag_no_previous_runs(self):
        """
        Tests scheduling a dag with no previous runs
        """
        dag = DAG(self.TEST_SCHEDULE_WITH_NO_PREVIOUS_RUNS_DAG_ID)
        dag.add_task(BaseOperator(
            task_id="faketastic",
            owner='Also fake',
            start_date=datetime(2015, 1, 2, 0, 0)))

        dag_run = jobs.SchedulerJob(**self.default_scheduler_args).create_dag_run(dag)
        self.assertIsNotNone(dag_run)
        self.assertEqual(dag.dag_id, dag_run.dag_id)
        self.assertIsNotNone(dag_run.run_id)
        self.assertNotEqual('', dag_run.run_id)
        self.assertEqual(
            datetime(2015, 1, 2, 0, 0),
            dag_run.execution_date,
            msg='dag_run.execution_date did not match expectation: {0}'
            .format(dag_run.execution_date)
        )
        self.assertEqual(State.RUNNING, dag_run.state)
        self.assertFalse(dag_run.external_trigger)
        dag.clear()

    def test_schedule_dag_relativedelta(self):
        """
        Tests scheduling a dag with a relativedelta schedule_interval
        """
        delta = relativedelta(hours=+1)
        dag = DAG(self.TEST_SCHEDULE_RELATIVEDELTA_DAG_ID,
                  schedule_interval=delta)
        dag.add_task(BaseOperator(
            task_id="faketastic",
            owner='Also fake',
            start_date=datetime(2015, 1, 2, 0, 0)))

        dag_run = jobs.SchedulerJob(**self.default_scheduler_args).create_dag_run(dag)
        self.assertIsNotNone(dag_run)
        self.assertEqual(dag.dag_id, dag_run.dag_id)
        self.assertIsNotNone(dag_run.run_id)
        self.assertNotEqual('', dag_run.run_id)
        self.assertEqual(
            datetime(2015, 1, 2, 0, 0),
            dag_run.execution_date,
            msg='dag_run.execution_date did not match expectation: {0}'
            .format(dag_run.execution_date)
        )
        self.assertEqual(State.RUNNING, dag_run.state)
        self.assertFalse(dag_run.external_trigger)
        dag_run2 = jobs.SchedulerJob(**self.default_scheduler_args).create_dag_run(dag)
        self.assertIsNotNone(dag_run2)
        self.assertEqual(dag.dag_id, dag_run2.dag_id)
        self.assertIsNotNone(dag_run2.run_id)
        self.assertNotEqual('', dag_run2.run_id)
        self.assertEqual(
            datetime(2015, 1, 2, 0, 0) + delta,
            dag_run2.execution_date,
            msg='dag_run2.execution_date did not match expectation: {0}'
            .format(dag_run2.execution_date)
        )
        self.assertEqual(State.RUNNING, dag_run2.state)
        self.assertFalse(dag_run2.external_trigger)
        dag.clear()

    def test_schedule_dag_fake_scheduled_previous(self):
        """
        Test scheduling a dag where there is a prior DagRun
        which has the same run_id as the next run should have
        """
        delta = timedelta(hours=1)

        dag = DAG(self.TEST_SCHEDULE_DAG_FAKE_SCHEDULED_PREVIOUS_DAG_ID,
                  schedule_interval=delta,
                  start_date=DEFAULT_DATE)
        dag.add_task(BaseOperator(
            task_id="faketastic",
            owner='Also fake',
            start_date=DEFAULT_DATE))

        scheduler = jobs.SchedulerJob(**self.default_scheduler_args)
        dag.create_dagrun(run_id=DagRun.id_for_date(DEFAULT_DATE),
                          execution_date=DEFAULT_DATE,
                          state=State.SUCCESS,
                          external_trigger=True)
        dag_run = scheduler.create_dag_run(dag)
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

    def test_schedule_dag_once(self):
        """
        Tests scheduling a dag scheduled for @once - should be scheduled the first time
        it is called, and not scheduled the second.
        """
        dag = DAG(self.TEST_SCHEDULE_ONCE_DAG_ID)
        dag.schedule_interval = '@once'
        dag.add_task(BaseOperator(
            task_id="faketastic",
            owner='Also fake',
            start_date=datetime(2015, 1, 2, 0, 0)))
        dag_run = jobs.SchedulerJob(**self.default_scheduler_args).create_dag_run(dag)
        dag_run2 = jobs.SchedulerJob(**self.default_scheduler_args).create_dag_run(dag)

        self.assertIsNotNone(dag_run)
        self.assertIsNone(dag_run2)
        dag.clear()

    def test_fractional_seconds(self):
        """
        Tests if fractional seconds are stored in the database
        """
        dag = DAG(TEST_DAG_ID + 'test_fractional_seconds')
        dag.schedule_interval = '@once'
        dag.add_task(BaseOperator(
            task_id="faketastic",
            owner='Also fake',
            start_date=datetime(2015, 1, 2, 0, 0)))

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

    def test_schedule_dag_start_end_dates(self):
        """
        Tests that an attempt to schedule a task after the Dag's end_date
        does not succeed.
        """
        delta = timedelta(hours=1)
        runs = 3
        start_date = DEFAULT_DATE
        end_date = start_date + (runs - 1) * delta

        dag = DAG(self.TEST_SCHEDULE_START_END_DATES_DAG_ID,
                  start_date=start_date,
                  end_date=end_date,
                  schedule_interval=delta)
        dag.add_task(BaseOperator(task_id='faketastic', owner='Also fake'))

        # Create and schedule the dag runs
        dag_runs = []
        scheduler = jobs.SchedulerJob(**self.default_scheduler_args)
        for _ in range(runs):
            dag_runs.append(scheduler.create_dag_run(dag))

        additional_dag_run = scheduler.create_dag_run(dag)

        for dag_run in dag_runs:
            self.assertIsNotNone(dag_run)

        self.assertIsNone(additional_dag_run)

    def test_schedule_dag_no_end_date_up_to_today_only(self):
        """
        Tests that a Dag created without an end_date can only be scheduled up
        to and including the current datetime.

        For example, if today is 2016-01-01 and we are scheduling from a
        start_date of 2015-01-01, only jobs up to, but not including
        2016-01-01 should be scheduled.
        """
        session = settings.Session()
        delta = timedelta(days=1)
        now = utcnow()
        start_date = now.subtract(weeks=1)

        runs = (now - start_date).days

        dag = DAG(self.TEST_SCHEDULE_DAG_NO_END_DATE_UP_TO_TODAY_ONLY_DAG_ID,
                  start_date=start_date,
                  schedule_interval=delta)
        dag.add_task(BaseOperator(task_id='faketastic', owner='Also fake'))

        dag_runs = []
        scheduler = jobs.SchedulerJob(**self.default_scheduler_args)
        for _ in range(runs):
            dag_run = scheduler.create_dag_run(dag)
            dag_runs.append(dag_run)

            # Mark the DagRun as complete
            dag_run.state = State.SUCCESS
            session.merge(dag_run)
            session.commit()

        # Attempt to schedule an additional dag run (for 2016-01-01)
        additional_dag_run = scheduler.create_dag_run(dag)

        for dag_run in dag_runs:
            self.assertIsNotNone(dag_run)

        self.assertIsNone(additional_dag_run)

    def test_confirm_unittest_mod(self):
        self.assertTrue(conf.get('core', 'unit_test_mode'))

    def test_pickling(self):
        dp = self.dag.pickle()
        self.assertEqual(dp.pickle.dag_id, self.dag.dag_id)

    def test_rich_comparison_ops(self):

        class DAGsubclass(DAG):
            pass

        dag_eq = DAG(TEST_DAG_ID, default_args=self.args)

        dag_diff_load_time = DAG(TEST_DAG_ID, default_args=self.args)
        dag_diff_name = DAG(TEST_DAG_ID + '_neq', default_args=self.args)

        dag_subclass = DAGsubclass(TEST_DAG_ID, default_args=self.args)
        dag_subclass_diff_name = DAGsubclass(
            TEST_DAG_ID + '2', default_args=self.args)

        for d in [dag_eq, dag_diff_name, dag_subclass, dag_subclass_diff_name]:
            d.last_loaded = self.dag.last_loaded

        # test identity equality
        self.assertEqual(self.dag, self.dag)

        # test dag (in)equality based on _comps
        self.assertEqual(dag_eq, self.dag)
        self.assertNotEqual(dag_diff_name, self.dag)
        self.assertNotEqual(dag_diff_load_time, self.dag)

        # test dag inequality based on type even if _comps happen to match
        self.assertNotEqual(dag_subclass, self.dag)

        # a dag should equal an unpickled version of itself
        d = pickle.dumps(self.dag)
        self.assertEqual(pickle.loads(d), self.dag)

        # dags are ordered based on dag_id no matter what the type is
        self.assertLess(self.dag, dag_diff_name)
        self.assertGreater(self.dag, dag_diff_load_time)
        self.assertLess(self.dag, dag_subclass_diff_name)

        # greater than should have been created automatically by functools
        self.assertGreater(dag_diff_name, self.dag)

        # hashes are non-random and match equality
        self.assertEqual(hash(self.dag), hash(self.dag))
        self.assertEqual(hash(dag_eq), hash(self.dag))
        self.assertNotEqual(hash(dag_diff_name), hash(self.dag))
        self.assertNotEqual(hash(dag_subclass), hash(self.dag))

    def test_check_operators(self):

        conn_id = "sqlite_default"

        captainHook = BaseHook.get_hook(conn_id=conn_id)
        captainHook.run("CREATE TABLE operator_test_table (a, b)")
        captainHook.run("insert into operator_test_table values (1,2)")

        t = CheckOperator(
            task_id='check',
            sql="select count(*) from operator_test_table",
            conn_id=conn_id,
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        t = ValueCheckOperator(
            task_id='value_check',
            pass_value=95,
            tolerance=0.1,
            conn_id=conn_id,
            sql="SELECT 100",
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        captainHook.run("drop table operator_test_table")

    def test_clear_api(self):
        task = self.dag_bash.tasks[0]
        task.clear(
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
            upstream=True, downstream=True)
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.are_dependents_done()

    def test_illegal_args(self):
        """
        Tests that Operators reject illegal arguments
        """
        msg = 'Invalid arguments were passed to BashOperator '
        '(task_id: test_illegal_args).'
        with conf_vars({('operators', 'allow_illegal_arguments'): 'True'}):
            with self.assertWarns(PendingDeprecationWarning) as warning:
                BashOperator(
                    task_id='test_illegal_args',
                    bash_command='echo success',
                    dag=self.dag,
                    illegal_argument_1234='hello?')
                assert any(msg in str(w) for w in warning.warnings)

    def test_illegal_args_forbidden(self):
        """
        Tests that operators raise exceptions on illegal arguments when
        illegal arguments are not allowed.
        """
        with self.assertRaises(AirflowException) as ctx:
            BashOperator(
                task_id='test_illegal_args',
                bash_command='echo success',
                dag=self.dag,
                illegal_argument_1234='hello?')
        self.assertIn(
            ('Invalid arguments were passed to BashOperator '
             '(task_id: test_illegal_args).'),
            str(ctx.exception))

    def test_bash_operator(self):
        t = BashOperator(
            task_id='test_bash_operator',
            bash_command="echo success",
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_bash_operator_multi_byte_output(self):
        t = BashOperator(
            task_id='test_multi_byte_bash_operator',
            bash_command="echo \u2600",
            dag=self.dag,
            output_encoding='utf-8')
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_bash_operator_kill(self):
        import psutil
        sleep_time = "100%d" % os.getpid()
        t = BashOperator(
            task_id='test_bash_operator_kill',
            execution_timeout=timedelta(seconds=1),
            bash_command="/bin/bash -c 'sleep %s'" % sleep_time,
            dag=self.dag)
        self.assertRaises(
            exceptions.AirflowTaskTimeout,
            t.run,
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        sleep(2)
        pid = -1
        for proc in psutil.process_iter():
            if proc.cmdline() == ['sleep', sleep_time]:
                pid = proc.pid
        if pid != -1:
            os.kill(pid, signal.SIGTERM)
            self.fail("BashOperator's subprocess still running after stopping on timeout!")

    def test_on_failure_callback(self):
        # Annoying workaround for nonlocal not existing in python 2
        data = {'called': False}

        def check_failure(context, test_case=self):
            data['called'] = True
            error = context.get('exception')
            test_case.assertIsInstance(error, AirflowException)

        t = BashOperator(
            task_id='check_on_failure_callback',
            bash_command="exit 1",
            dag=self.dag,
            on_failure_callback=check_failure)
        self.assertRaises(
            exceptions.AirflowException,
            t.run,
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        self.assertTrue(data['called'])

    def test_dryrun(self):
        t = BashOperator(
            task_id='test_dryrun',
            bash_command="echo success",
            dag=self.dag)
        t.dry_run()

    def test_sqlite(self):
        import airflow.operators.sqlite_operator
        t = airflow.operators.sqlite_operator.SqliteOperator(
            task_id='time_sqlite',
            sql="CREATE TABLE IF NOT EXISTS unitest (dummy VARCHAR(20))",
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_timeout(self):
        t = PythonOperator(
            task_id='test_timeout',
            execution_timeout=timedelta(seconds=1),
            python_callable=lambda: sleep(5),
            dag=self.dag)
        self.assertRaises(
            exceptions.AirflowTaskTimeout,
            t.run,
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_python_op(self):
        def test_py_op(templates_dict, ds, **kwargs):
            if not templates_dict['ds'] == ds:
                raise Exception("failure")

        t = PythonOperator(
            task_id='test_py_op',
            python_callable=test_py_op,
            templates_dict={'ds': "{{ ds }}"},
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_complex_template(self):
        def verify_templated_field(context):
            self.assertEqual(context['ti'].task.some_templated_field['bar'][1],
                             context['ds'])

        t = OperatorSubclass(
            task_id='test_complex_template',
            some_templated_field={
                'foo': '123',
                'bar': ['baz', '{{ ds }}']
            },
            dag=self.dag)
        t.execute = verify_templated_field
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_template_with_variable(self):
        """
        Test the availability of variables in templates
        """
        val = {
            'test_value': 'a test value'
        }
        Variable.set("a_variable", val['test_value'])

        def verify_templated_field(context):
            self.assertEqual(context['ti'].task.some_templated_field,
                             val['test_value'])

        t = OperatorSubclass(
            task_id='test_complex_template',
            some_templated_field='{{ var.value.a_variable }}',
            dag=self.dag)
        t.execute = verify_templated_field
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_template_with_json_variable(self):
        """
        Test the availability of variables (serialized as JSON) in templates
        """
        val = {
            'test_value': {'foo': 'bar', 'obj': {'v1': 'yes', 'v2': 'no'}}
        }
        Variable.set("a_variable", val['test_value'], serialize_json=True)

        def verify_templated_field(context):
            self.assertEqual(context['ti'].task.some_templated_field,
                             val['test_value']['obj']['v2'])

        t = OperatorSubclass(
            task_id='test_complex_template',
            some_templated_field='{{ var.json.a_variable.obj.v2 }}',
            dag=self.dag)
        t.execute = verify_templated_field
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_template_with_json_variable_as_value(self):
        """
        Test the availability of variables (serialized as JSON) in templates, but
        accessed as a value
        """
        val = {
            'test_value': {'foo': 'bar'}
        }
        Variable.set("a_variable", val['test_value'], serialize_json=True)

        def verify_templated_field(context):
            self.assertEqual(context['ti'].task.some_templated_field,
                             '{\n  "foo": "bar"\n}')

        t = OperatorSubclass(
            task_id='test_complex_template',
            some_templated_field='{{ var.value.a_variable }}',
            dag=self.dag)
        t.execute = verify_templated_field
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_template_non_bool(self):
        """
        Test templates can handle objects with no sense of truthiness
        """

        class NonBoolObject:
            def __len__(self):
                return NotImplemented

            def __bool__(self):
                return NotImplemented

        t = OperatorSubclass(
            task_id='test_bad_template_obj',
            some_templated_field=NonBoolObject(),
            dag=self.dag)
        t.resolve_template_files()

    def test_task_get_template(self):
        TI = TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        ti.dag = self.dag_bash
        ti.run(ignore_ti_state=True)
        context = ti.get_template_context()

        # DEFAULT DATE is 2015-01-01
        self.assertEqual(context['ds'], '2015-01-01')
        self.assertEqual(context['ds_nodash'], '20150101')

        # next_ds is 2015-01-02 as the dag interval is daily
        self.assertEqual(context['next_ds'], '2015-01-02')
        self.assertEqual(context['next_ds_nodash'], '20150102')

        # prev_ds is 2014-12-31 as the dag interval is daily
        self.assertEqual(context['prev_ds'], '2014-12-31')
        self.assertEqual(context['prev_ds_nodash'], '20141231')

        self.assertEqual(context['ts'], '2015-01-01T00:00:00+00:00')
        self.assertEqual(context['ts_nodash'], '20150101T000000')
        self.assertEqual(context['ts_nodash_with_tz'], '20150101T000000+0000')

        self.assertEqual(context['yesterday_ds'], '2014-12-31')
        self.assertEqual(context['yesterday_ds_nodash'], '20141231')

        self.assertEqual(context['tomorrow_ds'], '2015-01-02')
        self.assertEqual(context['tomorrow_ds_nodash'], '20150102')

    def test_local_task_job(self):
        TI = TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        job = jobs.LocalTaskJob(task_instance=ti, ignore_ti_state=True)
        job.run()

    def test_raw_job(self):
        TI = TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        ti.dag = self.dag_bash
        ti.run(ignore_ti_state=True)

    def test_variable_set_get_round_trip(self):
        Variable.set("tested_var_set_id", "Monday morning breakfast")
        self.assertEqual("Monday morning breakfast", Variable.get("tested_var_set_id"))

    def test_variable_set_get_round_trip_json(self):
        value = {"a": 17, "b": 47}
        Variable.set("tested_var_set_id", value, serialize_json=True)
        self.assertEqual(value, Variable.get("tested_var_set_id", deserialize_json=True))

    def test_get_non_existing_var_should_return_default(self):
        default_value = "some default val"
        self.assertEqual(default_value, Variable.get("thisIdDoesNotExist",
                                                     default_var=default_value))

    def test_get_non_existing_var_should_raise_key_error(self):
        with self.assertRaises(KeyError):
            Variable.get("thisIdDoesNotExist")

    def test_get_non_existing_var_with_none_default_should_return_none(self):
        self.assertIsNone(Variable.get("thisIdDoesNotExist", default_var=None))

    def test_get_non_existing_var_should_not_deserialize_json_default(self):
        default_value = "}{ this is a non JSON default }{"
        self.assertEqual(default_value, Variable.get("thisIdDoesNotExist",
                                                     default_var=default_value,
                                                     deserialize_json=True))

    def test_variable_setdefault_round_trip(self):
        key = "tested_var_setdefault_1_id"
        value = "Monday morning breakfast in Paris"
        Variable.setdefault(key, value)
        self.assertEqual(value, Variable.get(key))

    def test_variable_setdefault_round_trip_json(self):
        key = "tested_var_setdefault_2_id"
        value = {"city": 'Paris', "Happiness": True}
        Variable.setdefault(key, value, deserialize_json=True)
        self.assertEqual(value, Variable.get(key, deserialize_json=True))

    def test_variable_setdefault_existing_json(self):
        key = "tested_var_setdefault_2_id"
        value = {"city": 'Paris', "Happiness": True}
        Variable.set(key, value, serialize_json=True)
        val = Variable.setdefault(key, value, deserialize_json=True)
        # Check the returned value, and the stored value are handled correctly.
        self.assertEqual(value, val)
        self.assertEqual(value, Variable.get(key, deserialize_json=True))

    def test_variable_delete(self):
        key = "tested_var_delete"
        value = "to be deleted"

        # No-op if the variable doesn't exist
        Variable.delete(key)
        with self.assertRaises(KeyError):
            Variable.get(key)

        # Set the variable
        Variable.set(key, value)
        self.assertEqual(value, Variable.get(key))

        # Delete the variable
        Variable.delete(key)
        with self.assertRaises(KeyError):
            Variable.get(key)

    def test_parameterized_config_gen(self):

        cfg = configuration.parameterized_config(configuration.DEFAULT_CONFIG)

        # making sure some basic building blocks are present:
        self.assertIn("[core]", cfg)
        self.assertIn("dags_folder", cfg)
        self.assertIn("sql_alchemy_conn", cfg)
        self.assertIn("fernet_key", cfg)

        # making sure replacement actually happened
        self.assertNotIn("{AIRFLOW_HOME}", cfg)
        self.assertNotIn("{FERNET_KEY}", cfg)

    def test_config_use_original_when_original_and_fallback_are_present(self):
        self.assertTrue(conf.has_option("core", "FERNET_KEY"))
        self.assertFalse(conf.has_option("core", "FERNET_KEY_CMD"))

        FERNET_KEY = conf.get('core', 'FERNET_KEY')

        with conf_vars({('core', 'FERNET_KEY_CMD'): 'printf HELLO'}):
            FALLBACK_FERNET_KEY = conf.get(
                "core",
                "FERNET_KEY"
            )

        self.assertEqual(FERNET_KEY, FALLBACK_FERNET_KEY)

    def test_config_throw_error_when_original_and_fallback_is_absent(self):
        self.assertTrue(conf.has_option("core", "FERNET_KEY"))
        self.assertFalse(conf.has_option("core", "FERNET_KEY_CMD"))

        with conf_vars({('core', 'fernet_key'): None}):
            with self.assertRaises(AirflowConfigException) as cm:
                conf.get("core", "FERNET_KEY")

        exception = str(cm.exception)
        message = "section/key [core/fernet_key] not found in config"
        self.assertEqual(message, exception)

    def test_config_override_original_when_non_empty_envvar_is_provided(self):
        key = "AIRFLOW__CORE__FERNET_KEY"
        value = "some value"

        with mock.patch.dict('os.environ', {key: value}):
            FERNET_KEY = conf.get('core', 'FERNET_KEY')

        self.assertEqual(value, FERNET_KEY)

    def test_config_override_original_when_empty_envvar_is_provided(self):
        key = "AIRFLOW__CORE__FERNET_KEY"
        value = ""

        with mock.patch.dict('os.environ', {key: value}):
            FERNET_KEY = conf.get('core', 'FERNET_KEY')

        self.assertEqual(value, FERNET_KEY)

    def test_round_time(self):

        rt1 = round_time(datetime(2015, 1, 1, 6), timedelta(days=1))
        self.assertEqual(datetime(2015, 1, 1, 0, 0), rt1)

        rt2 = round_time(datetime(2015, 1, 2), relativedelta(months=1))
        self.assertEqual(datetime(2015, 1, 1, 0, 0), rt2)

        rt3 = round_time(datetime(2015, 9, 16, 0, 0), timedelta(1), datetime(
            2015, 9, 14, 0, 0))
        self.assertEqual(datetime(2015, 9, 16, 0, 0), rt3)

        rt4 = round_time(datetime(2015, 9, 15, 0, 0), timedelta(1), datetime(
            2015, 9, 14, 0, 0))
        self.assertEqual(datetime(2015, 9, 15, 0, 0), rt4)

        rt5 = round_time(datetime(2015, 9, 14, 0, 0), timedelta(1), datetime(
            2015, 9, 14, 0, 0))
        self.assertEqual(datetime(2015, 9, 14, 0, 0), rt5)

        rt6 = round_time(datetime(2015, 9, 13, 0, 0), timedelta(1), datetime(
            2015, 9, 14, 0, 0))
        self.assertEqual(datetime(2015, 9, 14, 0, 0), rt6)

    def test_infer_time_unit(self):

        self.assertEqual('minutes', infer_time_unit([130, 5400, 10]))

        self.assertEqual('seconds', infer_time_unit([110, 50, 10, 100]))

        self.assertEqual('hours', infer_time_unit([100000, 50000, 10000, 20000]))

        self.assertEqual('days', infer_time_unit([200000, 100000]))

    def test_scale_time_units(self):

        # use assert_almost_equal from numpy.testing since we are comparing
        # floating point arrays
        arr1 = scale_time_units([130, 5400, 10], 'minutes')
        assert_array_almost_equal(arr1, [2.167, 90.0, 0.167], decimal=3)

        arr2 = scale_time_units([110, 50, 10, 100], 'seconds')
        assert_array_almost_equal(arr2, [110.0, 50.0, 10.0, 100.0], decimal=3)

        arr3 = scale_time_units([100000, 50000, 10000, 20000], 'hours')
        assert_array_almost_equal(arr3, [27.778, 13.889, 2.778, 5.556],
                                  decimal=3)

        arr4 = scale_time_units([200000, 100000], 'days')
        assert_array_almost_equal(arr4, [2.315, 1.157], decimal=3)

    def test_bad_trigger_rule(self):
        with self.assertRaises(AirflowException):
            DummyOperator(
                task_id='test_bad_trigger',
                trigger_rule="non_existent",
                dag=self.dag)

    def test_terminate_task(self):
        """If a task instance's db state get deleted, it should fail"""
        TI = TaskInstance
        dag = self.dagbag.dags.get('test_utils')
        task = dag.task_dict.get('sleeps_forever')

        ti = TI(task=task, execution_date=DEFAULT_DATE)
        job = jobs.LocalTaskJob(
            task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())

        # Running task instance asynchronously
        p = multiprocessing.Process(target=job.run)
        p.start()
        sleep(5)
        settings.engine.dispose()
        session = settings.Session()
        ti.refresh_from_db(session=session)
        # making sure it's actually running
        self.assertEqual(State.RUNNING, ti.state)
        ti = session.query(TI).filter_by(
            dag_id=task.dag_id,
            task_id=task.task_id,
            execution_date=DEFAULT_DATE
        ).one()

        # deleting the instance should result in a failure
        session.delete(ti)
        session.commit()
        # waiting for the async task to finish
        p.join()

        # making sure that the task ended up as failed
        ti.refresh_from_db(session=session)
        self.assertEqual(State.FAILED, ti.state)
        session.close()

    def test_task_fail_duration(self):
        """If a task fails, the duration should be recorded in TaskFail"""

        p = BashOperator(
            task_id='pass_sleepy',
            bash_command='sleep 3',
            dag=self.dag)
        f = BashOperator(
            task_id='fail_sleepy',
            bash_command='sleep 5',
            execution_timeout=timedelta(seconds=3),
            retry_delay=timedelta(seconds=0),
            dag=self.dag)
        session = settings.Session()
        try:
            p.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        except Exception:
            pass
        try:
            f.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        except Exception:
            pass
        p_fails = session.query(TaskFail).filter_by(
            task_id='pass_sleepy',
            dag_id=self.dag.dag_id,
            execution_date=DEFAULT_DATE).all()
        f_fails = session.query(TaskFail).filter_by(
            task_id='fail_sleepy',
            dag_id=self.dag.dag_id,
            execution_date=DEFAULT_DATE).all()

        self.assertEqual(0, len(p_fails))
        self.assertEqual(1, len(f_fails))
        self.assertGreaterEqual(sum([f.duration for f in f_fails]), 3)

    def test_run_command(self):
        write = r'sys.stdout.buffer.write("\u1000foo".encode("utf8"))'

        cmd = 'import sys; {0}; sys.stdout.flush()'.format(write)

        self.assertEqual(run_command("python -c '{0}'".format(cmd)), '\u1000foo')

        self.assertEqual(run_command('echo "foo bar"'), 'foo bar\n')
        self.assertRaises(AirflowConfigException, run_command, 'bash -c "exit 1"')

    def test_externally_triggered_dagrun(self):
        TI = TaskInstance

        # Create the dagrun between two "scheduled" execution dates of the DAG
        EXECUTION_DATE = DEFAULT_DATE + timedelta(days=2)
        EXECUTION_DS = EXECUTION_DATE.strftime('%Y-%m-%d')
        EXECUTION_DS_NODASH = EXECUTION_DS.replace('-', '')

        dag = DAG(
            TEST_DAG_ID,
            default_args=self.args,
            schedule_interval=timedelta(weeks=1),
            start_date=DEFAULT_DATE)
        task = DummyOperator(task_id='test_externally_triggered_dag_context',
                             dag=dag)
        dag.create_dagrun(run_id=DagRun.id_for_date(EXECUTION_DATE),
                          execution_date=EXECUTION_DATE,
                          state=State.RUNNING,
                          external_trigger=True)
        task.run(
            start_date=EXECUTION_DATE, end_date=EXECUTION_DATE)

        ti = TI(task=task, execution_date=EXECUTION_DATE)
        context = ti.get_template_context()

        # next_ds/prev_ds should be the execution date for manually triggered runs
        self.assertEqual(context['next_ds'], EXECUTION_DS)
        self.assertEqual(context['next_ds_nodash'], EXECUTION_DS_NODASH)

        self.assertEqual(context['prev_ds'], EXECUTION_DS)
        self.assertEqual(context['prev_ds_nodash'], EXECUTION_DS_NODASH)


class TestCli(unittest.TestCase):

    TEST_USER1_EMAIL = 'test-user1@example.com'
    TEST_USER2_EMAIL = 'test-user2@example.com'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._cleanup()

    def setUp(self):
        super().setUp()
        from airflow.www import app as application
        self.app, self.appbuilder = application.create_app(session=Session, testing=True)
        self.app.config['TESTING'] = True

        self.parser = cli.CLIFactory.get_parser()
        self.dagbag = DagBag(dag_folder=DEV_NULL, include_examples=True)
        settings.configure_orm()
        self.session = Session

    def tearDown(self):
        self._cleanup(session=self.session)
        for email in [self.TEST_USER1_EMAIL, self.TEST_USER2_EMAIL]:
            test_user = self.appbuilder.sm.find_user(email=email)
            if test_user:
                self.appbuilder.sm.del_register_user(test_user)
        for role_name in ['FakeTeamA', 'FakeTeamB']:
            if self.appbuilder.sm.find_role(role_name):
                self.appbuilder.sm.delete_role(role_name)

        super().tearDown()

    @staticmethod
    def _cleanup(session=None):
        if session is None:
            session = Session()

        session.query(Pool).delete()
        session.query(Variable).delete()
        session.commit()
        session.close()


class TestConnection(unittest.TestCase):
    def setUp(self):
        utils.db.initdb()

    @mock.patch.dict('os.environ', {
        'AIRFLOW_CONN_TEST_URI': 'postgres://username:password@ec2.compute.com:5432/the_database',
    })
    def test_using_env_var(self):
        c = SqliteHook.get_connection(conn_id='test_uri')
        self.assertEqual('ec2.compute.com', c.host)
        self.assertEqual('the_database', c.schema)
        self.assertEqual('username', c.login)
        self.assertEqual('password', c.password)
        self.assertEqual(5432, c.port)

    @mock.patch.dict('os.environ', {
        'AIRFLOW_CONN_TEST_URI_NO_CREDS': 'postgres://ec2.compute.com/the_database',
    })
    def test_using_unix_socket_env_var(self):
        c = SqliteHook.get_connection(conn_id='test_uri_no_creds')
        self.assertEqual('ec2.compute.com', c.host)
        self.assertEqual('the_database', c.schema)
        self.assertIsNone(c.login)
        self.assertIsNone(c.password)
        self.assertIsNone(c.port)

    def test_param_setup(self):
        c = Connection(conn_id='local_mysql', conn_type='mysql',
                       host='localhost', login='airflow',
                       password='airflow', schema='airflow')
        self.assertEqual('localhost', c.host)
        self.assertEqual('airflow', c.schema)
        self.assertEqual('airflow', c.login)
        self.assertEqual('airflow', c.password)
        self.assertIsNone(c.port)

    def test_env_var_priority(self):
        c = SqliteHook.get_connection(conn_id='airflow_db')
        self.assertNotEqual('ec2.compute.com', c.host)

        with mock.patch.dict('os.environ', {
            'AIRFLOW_CONN_AIRFLOW_DB': 'postgres://username:password@ec2.compute.com:5432/the_database',
        }):
            c = SqliteHook.get_connection(conn_id='airflow_db')
            self.assertEqual('ec2.compute.com', c.host)
            self.assertEqual('the_database', c.schema)
            self.assertEqual('username', c.login)
            self.assertEqual('password', c.password)
            self.assertEqual(5432, c.port)

    @mock.patch.dict('os.environ', {
        'AIRFLOW_CONN_TEST_URI': 'postgres://username:password@ec2.compute.com:5432/the_database',
        'AIRFLOW_CONN_TEST_URI_NO_CREDS': 'postgres://ec2.compute.com/the_database',
    })
    def test_dbapi_get_uri(self):
        conn = BaseHook.get_connection(conn_id='test_uri')
        hook = conn.get_hook()
        self.assertEqual('postgres://username:password@ec2.compute.com:5432/the_database', hook.get_uri())
        conn2 = BaseHook.get_connection(conn_id='test_uri_no_creds')
        hook2 = conn2.get_hook()
        self.assertEqual('postgres://ec2.compute.com/the_database', hook2.get_uri())

    @mock.patch.dict('os.environ', {
        'AIRFLOW_CONN_TEST_URI': 'postgres://username:password@ec2.compute.com:5432/the_database',
        'AIRFLOW_CONN_TEST_URI_NO_CREDS': 'postgres://ec2.compute.com/the_database',
    })
    def test_dbapi_get_sqlalchemy_engine(self):
        conn = BaseHook.get_connection(conn_id='test_uri')
        hook = conn.get_hook()
        engine = hook.get_sqlalchemy_engine()
        self.assertIsInstance(engine, sqlalchemy.engine.Engine)
        self.assertEqual('postgres://username:password@ec2.compute.com:5432/the_database', str(engine.url))

    @mock.patch.dict('os.environ', {
        'AIRFLOW_CONN_TEST_URI': 'postgres://username:password@ec2.compute.com:5432/the_database',
        'AIRFLOW_CONN_TEST_URI_NO_CREDS': 'postgres://ec2.compute.com/the_database',
    })
    def test_get_connections_env_var(self):
        conns = SqliteHook.get_connections(conn_id='test_uri')
        assert len(conns) == 1
        assert conns[0].host == 'ec2.compute.com'
        assert conns[0].schema == 'the_database'
        assert conns[0].login == 'username'
        assert conns[0].password == 'password'
        assert conns[0].port == 5432


if __name__ == '__main__':
    unittest.main()
