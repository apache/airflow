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

from typing import Optional
import io
import json
import multiprocessing
import os
import pickle  # type: ignore
import re
import signal
import subprocess
import tempfile
import unittest
import warnings
from datetime import timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from tempfile import NamedTemporaryFile
from time import sleep
from unittest import mock

import sqlalchemy
from dateutil.relativedelta import relativedelta
from numpy.testing import assert_array_almost_equal
from pendulum import utcnow

from airflow import configuration, models
from airflow import jobs, DAG, utils, settings, exceptions
from airflow.bin import cli
from airflow.configuration import AirflowConfigException, run_command, conf
from airflow.exceptions import AirflowException
from airflow.executors import SequentialExecutor
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.models import (
    BaseOperator,
    Connection,
    TaskFail,
    DagBag,
    DagRun,
    Pool,
    DagModel,
    TaskInstance,
    Variable,
)
from airflow.operators.bash_operator import BashOperator
from airflow.operators.check_operator import CheckOperator, ValueCheckOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.settings import Session
from airflow.utils import timezone
from airflow.utils.dates import (
    days_ago, infer_time_unit, round_time,
    scale_time_units
)
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.hooks import hdfs_hook
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
        with warnings.catch_warnings(record=True) as w:
            BashOperator(
                task_id='test_illegal_args',
                bash_command='echo success',
                dag=self.dag,
                illegal_argument_1234='hello?')
            self.assertTrue(
                issubclass(w[0].category, PendingDeprecationWarning))
            self.assertIn(
                ('Invalid arguments were passed to BashOperator '
                 '(task_id: test_illegal_args).'),
                w[0].message.args[0])

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

    def test_trigger_dagrun(self):
        def trigga(_, obj):
            if True:
                return obj

        t = TriggerDagRunOperator(
            task_id='test_trigger_dagrun',
            trigger_dag_id='example_bash_operator',
            python_callable=trigga,
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

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
        self.assertNotIn(key, os.environ)

        os.environ[key] = value
        FERNET_KEY = conf.get('core', 'FERNET_KEY')
        self.assertEqual(value, FERNET_KEY)

        # restore the envvar back to the original state
        del os.environ[key]

    def test_config_override_original_when_empty_envvar_is_provided(self):
        key = "AIRFLOW__CORE__FERNET_KEY"
        value = ""
        self.assertNotIn(key, os.environ)

        os.environ[key] = value
        FERNET_KEY = conf.get('core', 'FERNET_KEY')
        self.assertEqual(value, FERNET_KEY)

        # restore the envvar back to the original state
        del os.environ[key]

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

    def test_trigger_dagrun_with_execution_date(self):
        utc_now = timezone.utcnow()
        run_id = 'trig__' + utc_now.isoformat()

        def payload_generator(context, object):  # pylint: disable=unused-argument
            object.run_id = run_id
            return object

        task = TriggerDagRunOperator(task_id='test_trigger_dagrun_with_execution_date',
                                     trigger_dag_id='example_bash_operator',
                                     python_callable=payload_generator,
                                     execution_date=utc_now,
                                     dag=self.dag)
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        dag_runs = DagRun.find(dag_id='example_bash_operator', run_id=run_id)
        self.assertEqual(len(dag_runs), 1)
        dag_run = dag_runs[0]
        self.assertEqual(dag_run.execution_date, utc_now)

    def test_trigger_dagrun_with_str_execution_date(self):
        utc_now_str = timezone.utcnow().isoformat()
        self.assertIsInstance(utc_now_str, (str,))
        run_id = 'trig__' + utc_now_str

        def payload_generator(context, object):  # pylint: disable=unused-argument
            object.run_id = run_id
            return object

        task = TriggerDagRunOperator(
            task_id='test_trigger_dagrun_with_str_execution_date',
            trigger_dag_id='example_bash_operator',
            python_callable=payload_generator,
            execution_date=utc_now_str,
            dag=self.dag)
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        dag_runs = DagRun.find(dag_id='example_bash_operator', run_id=run_id)
        self.assertEqual(len(dag_runs), 1)
        dag_run = dag_runs[0]
        self.assertEqual(dag_run.execution_date.isoformat(), utc_now_str)

    def test_trigger_dagrun_with_templated_execution_date(self):
        task = TriggerDagRunOperator(
            task_id='test_trigger_dagrun_with_str_execution_date',
            trigger_dag_id='example_bash_operator',
            execution_date='{{ execution_date }}',
            dag=self.dag)

        self.assertTrue(isinstance(task.execution_date, str))
        self.assertEqual(task.execution_date, '{{ execution_date }}')

        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.render_templates()
        self.assertEqual(timezone.parse(task.execution_date), DEFAULT_DATE)

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

    def test_cli_list_dags(self):
        args = self.parser.parse_args(['dags', 'list', '--report'])
        cli.list_dags(args)

    def test_cli_list_dag_runs(self):
        cli.trigger_dag(self.parser.parse_args([
            'dags', 'trigger', 'example_bash_operator', ]))
        args = self.parser.parse_args(['dags', 'list_runs',
                                       'example_bash_operator',
                                       '--no_backfill'])
        cli.list_dag_runs(args)

    def test_cli_create_user_random_password(self):
        args = self.parser.parse_args([
            'users', 'create', '--username', 'test1', '--lastname', 'doe',
            '--firstname', 'jon',
            '--email', 'jdoe@foo.com', '--role', 'Viewer', '--use_random_password'
        ])
        cli.users_create(args)

    def test_cli_create_user_supplied_password(self):
        args = self.parser.parse_args([
            'users', 'create', '--username', 'test2', '--lastname', 'doe',
            '--firstname', 'jon',
            '--email', 'jdoe@apache.org', '--role', 'Viewer', '--password', 'test'
        ])
        cli.users_create(args)

    def test_cli_delete_user(self):
        args = self.parser.parse_args([
            'users', 'create', '--username', 'test3', '--lastname', 'doe',
            '--firstname', 'jon',
            '--email', 'jdoe@example.com', '--role', 'Viewer', '--use_random_password'
        ])
        cli.users_create(args)
        args = self.parser.parse_args([
            'users', 'delete', '--username', 'test3',
        ])
        cli.users_delete(args)

    def test_cli_list_users(self):
        for i in range(0, 3):
            args = self.parser.parse_args([
                'users', 'create', '--username', 'user{}'.format(i), '--lastname',
                'doe', '--firstname', 'jon',
                '--email', 'jdoe+{}@gmail.com'.format(i), '--role', 'Viewer',
                '--use_random_password'
            ])
            cli.users_create(args)
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            cli.users_list(self.parser.parse_args(['users', 'list']))
            stdout = mock_stdout.getvalue()
        for i in range(0, 3):
            self.assertIn('user{}'.format(i), stdout)

    def test_cli_import_users(self):
        def assertUserInRoles(email, roles):
            for role in roles:
                self.assertTrue(self._does_user_belong_to_role(email, role))

        def assertUserNotInRoles(email, roles):
            for role in roles:
                self.assertFalse(self._does_user_belong_to_role(email, role))

        assertUserNotInRoles(self.TEST_USER1_EMAIL, ['Admin', 'Op'])
        assertUserNotInRoles(self.TEST_USER2_EMAIL, ['Public'])
        users = [
            {
                "username": "imported_user1", "lastname": "doe1",
                "firstname": "jon", "email": self.TEST_USER1_EMAIL,
                "roles": ["Admin", "Op"]
            },
            {
                "username": "imported_user2", "lastname": "doe2",
                "firstname": "jon", "email": self.TEST_USER2_EMAIL,
                "roles": ["Public"]
            }
        ]
        self._import_users_from_file(users)

        assertUserInRoles(self.TEST_USER1_EMAIL, ['Admin', 'Op'])
        assertUserInRoles(self.TEST_USER2_EMAIL, ['Public'])

        users = [
            {
                "username": "imported_user1", "lastname": "doe1",
                "firstname": "jon", "email": self.TEST_USER1_EMAIL,
                "roles": ["Public"]
            },
            {
                "username": "imported_user2", "lastname": "doe2",
                "firstname": "jon", "email": self.TEST_USER2_EMAIL,
                "roles": ["Admin"]
            }
        ]
        self._import_users_from_file(users)

        assertUserNotInRoles(self.TEST_USER1_EMAIL, ['Admin', 'Op'])
        assertUserInRoles(self.TEST_USER1_EMAIL, ['Public'])
        assertUserNotInRoles(self.TEST_USER2_EMAIL, ['Public'])
        assertUserInRoles(self.TEST_USER2_EMAIL, ['Admin'])

    def test_cli_export_users(self):
        user1 = {"username": "imported_user1", "lastname": "doe1",
                 "firstname": "jon", "email": self.TEST_USER1_EMAIL,
                 "roles": ["Public"]}
        user2 = {"username": "imported_user2", "lastname": "doe2",
                 "firstname": "jon", "email": self.TEST_USER2_EMAIL,
                 "roles": ["Admin"]}
        self._import_users_from_file([user1, user2])

        users_filename = self._export_users_to_file()
        with open(users_filename, mode='r') as file:
            retrieved_users = json.loads(file.read())
        os.remove(users_filename)

        # ensure that an export can be imported
        self._import_users_from_file(retrieved_users)

        def find_by_username(username):
            matches = [u for u in retrieved_users
                       if u['username'] == username]
            if not matches:
                self.fail("Couldn't find user with username {}".format(username))
            else:
                matches[0].pop('id')  # this key not required for import
                return matches[0]

        self.assertEqual(find_by_username('imported_user1'), user1)
        self.assertEqual(find_by_username('imported_user2'), user2)

    def _import_users_from_file(self, user_list):
        json_file_content = json.dumps(user_list)
        f = NamedTemporaryFile(delete=False)
        try:
            f.write(json_file_content.encode())
            f.flush()

            args = self.parser.parse_args([
                'users', 'import', f.name
            ])
            cli.users_import(args)
        finally:
            os.remove(f.name)

    def _export_users_to_file(self):
        f = NamedTemporaryFile(delete=False)
        args = self.parser.parse_args([
            'users', 'export', f.name
        ])
        cli.users_export(args)
        return f.name

    def _does_user_belong_to_role(self, email, rolename):
        user = self.appbuilder.sm.find_user(email=email)
        role = self.appbuilder.sm.find_role(rolename)
        if user and role:
            return role in user.roles

        return False

    def test_cli_add_user_role(self):
        args = self.parser.parse_args([
            'users', 'create', '--username', 'test4', '--lastname', 'doe',
            '--firstname', 'jon',
            '--email', self.TEST_USER1_EMAIL, '--role', 'Viewer', '--use_random_password'
        ])
        cli.users_create(args)

        self.assertFalse(
            self._does_user_belong_to_role(email=self.TEST_USER1_EMAIL,
                                           rolename='Op'),
            "User should not yet be a member of role 'Op'"
        )

        args = self.parser.parse_args([
            'users', 'add_role', '--username', 'test4', '--role', 'Op'
        ])
        cli.users_manage_role(args, remove=False)

        self.assertTrue(
            self._does_user_belong_to_role(email=self.TEST_USER1_EMAIL,
                                           rolename='Op'),
            "User should have been added to role 'Op'"
        )

    def test_cli_remove_user_role(self):
        args = self.parser.parse_args([
            'users', 'create', '--username', 'test4', '--lastname', 'doe',
            '--firstname', 'jon',
            '--email', self.TEST_USER1_EMAIL, '--role', 'Viewer', '--use_random_password'
        ])
        cli.users_create(args)

        self.assertTrue(
            self._does_user_belong_to_role(email=self.TEST_USER1_EMAIL,
                                           rolename='Viewer'),
            "User should have been created with role 'Viewer'"
        )

        args = self.parser.parse_args([
            'users', 'remove_role', '--username', 'test4', '--role', 'Viewer'
        ])
        cli.users_manage_role(args, remove=True)

        self.assertFalse(
            self._does_user_belong_to_role(email=self.TEST_USER1_EMAIL,
                                           rolename='Viewer'),
            "User should have been removed from role 'Viewer'"
        )

    @mock.patch("airflow.bin.cli.DagBag")
    def test_cli_sync_perm(self, dagbag_mock):
        self.expect_dagbag_contains([
            DAG('has_access_control',
                access_control={
                    'Public': {'can_dag_read'}
                }),
            DAG('no_access_control')
        ], dagbag_mock)
        self.appbuilder.sm = mock.Mock()

        args = self.parser.parse_args([
            'sync_perm'
        ])
        cli.sync_perm(args)

        assert self.appbuilder.sm.sync_roles.call_count == 1

        self.assertEqual(2,
                         len(self.appbuilder.sm.sync_perm_for_dag.mock_calls))
        self.appbuilder.sm.sync_perm_for_dag.assert_any_call(
            'has_access_control',
            {'Public': {'can_dag_read'}}
        )
        self.appbuilder.sm.sync_perm_for_dag.assert_any_call(
            'no_access_control',
            None,
        )

    def expect_dagbag_contains(self, dags, dagbag_mock):
        dagbag = mock.Mock()
        dagbag.dags = {dag.dag_id: dag for dag in dags}
        dagbag_mock.return_value = dagbag

    def test_cli_create_roles(self):
        self.assertIsNone(self.appbuilder.sm.find_role('FakeTeamA'))
        self.assertIsNone(self.appbuilder.sm.find_role('FakeTeamB'))

        args = self.parser.parse_args([
            'roles', 'create', 'FakeTeamA', 'FakeTeamB'
        ])
        cli.roles_create(args)

        self.assertIsNotNone(self.appbuilder.sm.find_role('FakeTeamA'))
        self.assertIsNotNone(self.appbuilder.sm.find_role('FakeTeamB'))

    def test_cli_create_roles_is_reentrant(self):
        self.assertIsNone(self.appbuilder.sm.find_role('FakeTeamA'))
        self.assertIsNone(self.appbuilder.sm.find_role('FakeTeamB'))

        args = self.parser.parse_args([
            'roles', 'create', 'FakeTeamA', 'FakeTeamB'
        ])

        cli.roles_create(args)

        self.assertIsNotNone(self.appbuilder.sm.find_role('FakeTeamA'))
        self.assertIsNotNone(self.appbuilder.sm.find_role('FakeTeamB'))

    def test_cli_list_roles(self):
        self.appbuilder.sm.add_role('FakeTeamA')
        self.appbuilder.sm.add_role('FakeTeamB')

        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            cli.roles_list(self.parser.parse_args(['roles', 'list']))
            stdout = mock_stdout.getvalue()

        self.assertIn('FakeTeamA', stdout)
        self.assertIn('FakeTeamB', stdout)

    def test_cli_list_tasks(self):
        for dag_id in self.dagbag.dags.keys():
            args = self.parser.parse_args(['tasks', 'list', dag_id])
            cli.list_tasks(args)

        args = self.parser.parse_args([
            'tasks', 'list', 'example_bash_operator', '--tree'])
        cli.list_tasks(args)

    def test_cli_list_jobs(self):
        args = self.parser.parse_args(['dags', 'list_jobs'])
        cli.list_jobs(args)

    def test_cli_list_jobs_with_args(self):
        args = self.parser.parse_args(['dags', 'list_jobs', '--dag_id',
                                       'example_bash_operator',
                                       '--state', 'success',
                                       '--limit', '100'])
        cli.list_jobs(args)

    @mock.patch("airflow.bin.cli.db.initdb")
    def test_cli_initdb(self, initdb_mock):
        cli.initdb(self.parser.parse_args(['db', 'init']))

        initdb_mock.assert_called_once_with()

    @mock.patch("airflow.bin.cli.db.resetdb")
    def test_cli_resetdb(self, resetdb_mock):
        cli.resetdb(self.parser.parse_args(['db', 'reset', '--yes']))

        resetdb_mock.assert_called_once_with()

    def test_cli_connections_list(self):
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            cli.connections_list(self.parser.parse_args(['connections', 'list']))
            stdout = mock_stdout.getvalue()
        conns = [[x.strip("'") for x in re.findall(r"'\w+'", line)[:2]]
                 for ii, line in enumerate(stdout.split('\n'))
                 if ii % 2 == 1]
        conns = [conn for conn in conns if len(conn) > 0]

        # Assert that some of the connections are present in the output as
        # expected:
        self.assertIn(['aws_default', 'aws'], conns)
        self.assertIn(['hive_cli_default', 'hive_cli'], conns)
        self.assertIn(['emr_default', 'emr'], conns)
        self.assertIn(['mssql_default', 'mssql'], conns)
        self.assertIn(['mysql_default', 'mysql'], conns)
        self.assertIn(['postgres_default', 'postgres'], conns)
        self.assertIn(['wasb_default', 'wasb'], conns)
        self.assertIn(['segment_default', 'segment'], conns)

    def test_cli_connections_list_redirect(self):
        cmd = ['airflow', 'connections', 'list']
        with tempfile.TemporaryFile() as fp:
            p = subprocess.Popen(cmd, stdout=fp)
            p.wait()
            self.assertEqual(0, p.returncode)

    def test_cli_connections_add_delete(self):
        # Add connections:
        uri = 'postgresql://airflow:airflow@host:5432/airflow'
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new1',
                 '--conn_uri=%s' % uri]))
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new2',
                 '--conn_uri=%s' % uri]))
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new3',
                 '--conn_uri=%s' % uri, '--conn_extra', "{'extra': 'yes'}"]))
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new4',
                 '--conn_uri=%s' % uri, '--conn_extra', "{'extra': 'yes'}"]))
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new5',
                 '--conn_type=hive_metastore', '--conn_login=airflow',
                 '--conn_password=airflow', '--conn_host=host',
                 '--conn_port=9083', '--conn_schema=airflow']))
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new6',
                 '--conn_uri', "", '--conn_type=google_cloud_platform', '--conn_extra', "{'extra': 'yes'}"]))
            stdout = mock_stdout.getvalue()

        # Check addition stdout
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            ("\tSuccessfully added `conn_id`=new1 : " +
             "postgresql://airflow:airflow@host:5432/airflow"),
            ("\tSuccessfully added `conn_id`=new2 : " +
             "postgresql://airflow:airflow@host:5432/airflow"),
            ("\tSuccessfully added `conn_id`=new3 : " +
             "postgresql://airflow:airflow@host:5432/airflow"),
            ("\tSuccessfully added `conn_id`=new4 : " +
             "postgresql://airflow:airflow@host:5432/airflow"),
            ("\tSuccessfully added `conn_id`=new5 : " +
             "hive_metastore://airflow:airflow@host:9083/airflow"),
            ("\tSuccessfully added `conn_id`=new6 : " +
             "google_cloud_platform://:@:")
        ])

        # Attempt to add duplicate
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new1',
                 '--conn_uri=%s' % uri]))
            stdout = mock_stdout.getvalue()

        # Check stdout for addition attempt
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            "\tA connection with `conn_id`=new1 already exists",
        ])

        # Attempt to add without providing conn_uri
        with self.assertRaises(SystemExit) as exc:
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new']))

        self.assertEqual(
            exc.exception.code,
            "The following args are required to add a connection: ['conn_uri or conn_type']"
        )

        # Prepare to add connections
        session = settings.Session()
        extra = {'new1': None,
                 'new2': None,
                 'new3': "{'extra': 'yes'}",
                 'new4': "{'extra': 'yes'}"}

        # Add connections
        for index in range(1, 6):
            conn_id = 'new%s' % index
            result = (session
                      .query(Connection)
                      .filter(Connection.conn_id == conn_id)
                      .first())
            result = (result.conn_id, result.conn_type, result.host,
                      result.port, result.get_extra())
            if conn_id in ['new1', 'new2', 'new3', 'new4']:
                self.assertEqual(result, (conn_id, 'postgres', 'host', 5432,
                                          extra[conn_id]))
            elif conn_id == 'new5':
                self.assertEqual(result, (conn_id, 'hive_metastore', 'host',
                                          9083, None))
            elif conn_id == 'new6':
                self.assertEqual(result, (conn_id, 'google_cloud_platform',
                                          None, None, "{'extra': 'yes'}"))

        # Delete connections
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            cli.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new1']))
            cli.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new2']))
            cli.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new3']))
            cli.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new4']))
            cli.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new5']))
            cli.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new6']))
            stdout = mock_stdout.getvalue()

        # Check deletion stdout
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            "\tSuccessfully deleted `conn_id`=new1",
            "\tSuccessfully deleted `conn_id`=new2",
            "\tSuccessfully deleted `conn_id`=new3",
            "\tSuccessfully deleted `conn_id`=new4",
            "\tSuccessfully deleted `conn_id`=new5",
            "\tSuccessfully deleted `conn_id`=new6"
        ])

        # Check deletions
        for index in range(1, 7):
            conn_id = 'new%s' % index
            result = (session.query(Connection)
                      .filter(Connection.conn_id == conn_id)
                      .first())

            self.assertTrue(result is None)

        # Attempt to delete a non-existing connection
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            cli.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'fake']))
            stdout = mock_stdout.getvalue()

        # Check deletion attempt stdout
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            "\tDid not find a connection with `conn_id`=fake",
        ])

        session.close()

    def test_cli_test(self):
        cli.test(self.parser.parse_args([
            'tasks', 'test', 'example_bash_operator', 'runme_0',
            DEFAULT_DATE.isoformat()]))
        cli.test(self.parser.parse_args([
            'tasks', 'test', 'example_bash_operator', 'runme_0', '--dry_run',
            DEFAULT_DATE.isoformat()]))

    def test_cli_test_with_params(self):
        cli.test(self.parser.parse_args([
            'tasks', 'test', 'example_passing_params_via_test_command', 'run_this',
            '-tp', '{"foo":"bar"}', DEFAULT_DATE.isoformat()]))
        cli.test(self.parser.parse_args([
            'tasks', 'test', 'example_passing_params_via_test_command', 'also_run_this',
            '-tp', '{"foo":"bar"}', DEFAULT_DATE.isoformat()]))

    def test_cli_run(self):
        cli.run(self.parser.parse_args([
            'tasks', 'run', 'example_bash_operator', 'runme_0', '-l',
            DEFAULT_DATE.isoformat()]))

    def test_task_state(self):
        cli.task_state(self.parser.parse_args([
            'tasks', 'state', 'example_bash_operator', 'runme_0',
            DEFAULT_DATE.isoformat()]))

    def test_dag_state(self):
        self.assertEqual(None, cli.dag_state(self.parser.parse_args([
            'dags', 'state', 'example_bash_operator', DEFAULT_DATE.isoformat()])))

    def test_pause(self):
        args = self.parser.parse_args([
            'dags', 'pause', 'example_bash_operator'])
        cli.pause(args)
        self.assertIn(self.dagbag.dags['example_bash_operator'].is_paused, [True, 1])

        args = self.parser.parse_args([
            'dags', 'unpause', 'example_bash_operator'])
        cli.unpause(args)
        self.assertIn(self.dagbag.dags['example_bash_operator'].is_paused, [False, 0])

    def test_subdag_clear(self):
        args = self.parser.parse_args([
            'tasks', 'clear', 'example_subdag_operator', '--no_confirm'])
        cli.clear(args)
        args = self.parser.parse_args([
            'tasks', 'clear', 'example_subdag_operator', '--no_confirm', '--exclude_subdags'])
        cli.clear(args)

    def test_parentdag_downstream_clear(self):
        args = self.parser.parse_args([
            'tasks', 'clear', 'example_subdag_operator.section-1', '--no_confirm'])
        cli.clear(args)
        args = self.parser.parse_args([
            'tasks', 'clear', 'example_subdag_operator.section-1', '--no_confirm',
            '--exclude_parentdag'])
        cli.clear(args)

    def test_get_dags(self):
        dags = cli.get_dags(self.parser.parse_args(['tasks', 'clear', 'example_subdag_operator',
                                                    '-c']))
        self.assertEqual(len(dags), 1)

        dags = cli.get_dags(self.parser.parse_args(['tasks', 'clear', 'subdag', '-dx', '-c']))
        self.assertGreater(len(dags), 1)

        with self.assertRaises(AirflowException):
            cli.get_dags(self.parser.parse_args(['tasks', 'clear', 'foobar', '-dx', '-c']))

    def test_process_subdir_path_with_placeholder(self):
        self.assertEqual(os.path.join(settings.DAGS_FOLDER, 'abc'), cli.process_subdir('DAGS_FOLDER/abc'))

    def test_trigger_dag(self):
        cli.trigger_dag(self.parser.parse_args([
            'dags', 'trigger', 'example_bash_operator',
            '-c', '{"foo": "bar"}']))
        self.assertRaises(
            ValueError,
            cli.trigger_dag,
            self.parser.parse_args([
                'dags', 'trigger', 'example_bash_operator',
                '--run_id', 'trigger_dag_xxx',
                '-c', 'NOT JSON'])
        )

    def test_delete_dag(self):
        DM = DagModel
        key = "my_dag_id"
        session = settings.Session()
        session.add(DM(dag_id=key))
        session.commit()
        cli.delete_dag(self.parser.parse_args([
            'dags', 'delete', key, '--yes']))
        self.assertEqual(session.query(DM).filter_by(dag_id=key).count(), 0)
        self.assertRaises(
            AirflowException,
            cli.delete_dag,
            self.parser.parse_args([
                'dags', 'delete',
                'does_not_exist_dag',
                '--yes'])
        )

    def test_delete_dag_existing_file(self):
        # Test to check that the DAG should be deleted even if
        # the file containing it is not deleted
        DM = DagModel
        key = "my_dag_id"
        session = settings.Session()
        with tempfile.NamedTemporaryFile() as f:
            session.add(DM(dag_id=key, fileloc=f.name))
            session.commit()
            cli.delete_dag(self.parser.parse_args([
                'dags', 'delete', key, '--yes']))
            self.assertEqual(session.query(DM).filter_by(dag_id=key).count(), 0)

    def test_pool_create(self):
        cli.pool_set(self.parser.parse_args(['pools', 'set', 'foo', '1', 'test']))
        self.assertEqual(self.session.query(Pool).count(), 1)

    def test_pool_get(self):
        cli.pool_set(self.parser.parse_args(['pools', 'set', 'foo', '1', 'test']))
        try:
            cli.pool_get(self.parser.parse_args(['pools', 'get', 'foo']))
        except Exception as e:
            self.fail("The 'pool -g foo' command raised unexpectedly: %s" % e)

    def test_pool_delete(self):
        cli.pool_set(self.parser.parse_args(['pools', 'set', 'foo', '1', 'test']))
        cli.pool_delete(self.parser.parse_args(['pools', 'delete', 'foo']))
        self.assertEqual(self.session.query(Pool).count(), 0)

    def test_pool_import_export(self):
        # Create two pools first
        pool_config_input = {
            "foo": {
                "description": "foo_test",
                "slots": 1
            },
            "baz": {
                "description": "baz_test",
                "slots": 2
            }
        }
        with open('pools_import.json', mode='w') as file:
            json.dump(pool_config_input, file)

        # Import json
        try:
            cli.pool_import(self.parser.parse_args(['pools', 'import', 'pools_import.json']))
        except Exception as e:
            self.fail("The 'pool import pools_import.json' failed: %s" % e)

        # Export json
        try:
            cli.pool_export(self.parser.parse_args(['pools', 'export', 'pools_export.json']))
        except Exception as e:
            self.fail("The 'pool export pools_export.json' failed: %s" % e)

        with open('pools_export.json', mode='r') as file:
            pool_config_output = json.load(file)
            self.assertEqual(
                pool_config_input,
                pool_config_output,
                "Input and output pool files are not same")
        os.remove('pools_import.json')
        os.remove('pools_export.json')

    def test_variables(self):
        # Checks if all subcommands are properly received
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'foo', '{"foo":"bar"}']))
        cli.variables_get(self.parser.parse_args([
            'variables', 'get', 'foo']))
        cli.variables_get(self.parser.parse_args([
            'variables', 'get', 'baz', '-d', 'bar']))
        cli.variables_list(self.parser.parse_args([
            'variables', 'list']))
        cli.variables_delete(self.parser.parse_args([
            'variables', 'delete', 'bar']))
        cli.variables_import(self.parser.parse_args([
            'variables', 'import', DEV_NULL]))
        cli.variables_export(self.parser.parse_args([
            'variables', 'export', DEV_NULL]))

        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'bar', 'original']))
        # First export
        cli.variables_export(self.parser.parse_args([
            'variables', 'export', 'variables1.json']))

        first_exp = open('variables1.json', 'r')

        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'bar', 'updated']))
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'foo', '{"foo":"oops"}']))
        cli.variables_delete(self.parser.parse_args([
            'variables', 'delete', 'foo']))
        # First import
        cli.variables_import(self.parser.parse_args([
            'variables', 'import', 'variables1.json']))

        self.assertEqual('original', Variable.get('bar'))
        self.assertEqual('{\n  "foo": "bar"\n}', Variable.get('foo'))
        # Second export
        cli.variables_export(self.parser.parse_args([
            'variables', 'export', 'variables2.json']))

        second_exp = open('variables2.json', 'r')
        self.assertEqual(first_exp.read(), second_exp.read())
        second_exp.close()
        first_exp.close()
        # Second import
        cli.variables_import(self.parser.parse_args([
            'variables', 'import', 'variables2.json']))

        self.assertEqual('original', Variable.get('bar'))
        self.assertEqual('{\n  "foo": "bar"\n}', Variable.get('foo'))

        # Set a dict
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'dict', '{"foo": "oops"}']))
        # Set a list
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'list', '["oops"]']))
        # Set str
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'str', 'hello string']))
        # Set int
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'int', '42']))
        # Set float
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'float', '42.0']))
        # Set true
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'true', 'true']))
        # Set false
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'false', 'false']))
        # Set none
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'null', 'null']))

        # Export and then import
        cli.variables_export(self.parser.parse_args([
            'variables', 'export', 'variables3.json']))
        cli.variables_import(self.parser.parse_args([
            'variables', 'import', 'variables3.json']))

        # Assert value
        self.assertEqual({'foo': 'oops'}, models.Variable.get('dict', deserialize_json=True))
        self.assertEqual(['oops'], models.Variable.get('list', deserialize_json=True))
        self.assertEqual('hello string', models.Variable.get('str'))  # cannot json.loads(str)
        self.assertEqual(42, models.Variable.get('int', deserialize_json=True))
        self.assertEqual(42.0, models.Variable.get('float', deserialize_json=True))
        self.assertEqual(True, models.Variable.get('true', deserialize_json=True))
        self.assertEqual(False, models.Variable.get('false', deserialize_json=True))
        self.assertEqual(None, models.Variable.get('null', deserialize_json=True))

        os.remove('variables1.json')
        os.remove('variables2.json')
        os.remove('variables3.json')

    def _wait_pidfile(self, pidfile):
        while True:
            try:
                with open(pidfile) as file:
                    return int(file.read())
            except Exception:
                sleep(1)

    def test_cli_webserver_foreground(self):
        # Confirm that webserver hasn't been launched.
        # pgrep returns exit status 1 if no process matched.
        self.assertEqual(1, subprocess.Popen(["pgrep", "-c", "airflow"]).wait())
        self.assertEqual(1, subprocess.Popen(["pgrep", "-c", "gunicorn"]).wait())

        # Run webserver in foreground and terminate it.
        p = subprocess.Popen(["airflow", "webserver"])
        p.terminate()
        p.wait()

        # Assert that no process remains.
        self.assertEqual(1, subprocess.Popen(["pgrep", "-c", "airflow"]).wait())
        self.assertEqual(1, subprocess.Popen(["pgrep", "-c", "gunicorn"]).wait())

    @unittest.skipIf("TRAVIS" in os.environ and bool(os.environ["TRAVIS"]),
                     "Skipping test due to lack of required file permission")
    def test_cli_webserver_foreground_with_pid(self):
        # Run webserver in foreground with --pid option
        pidfile = tempfile.mkstemp()[1]
        p = subprocess.Popen(["airflow", "webserver", "--pid", pidfile])

        # Check the file specified by --pid option exists
        self._wait_pidfile(pidfile)

        # Terminate webserver
        p.terminate()
        p.wait()

    @unittest.skipIf("TRAVIS" in os.environ and bool(os.environ["TRAVIS"]),
                     "Skipping test due to lack of required file permission")
    def test_cli_webserver_background(self):
        import psutil

        # Confirm that webserver hasn't been launched.
        self.assertEqual(1, subprocess.Popen(["pgrep", "-c", "airflow"]).wait())
        self.assertEqual(1, subprocess.Popen(["pgrep", "-c", "gunicorn"]).wait())

        # Run webserver in background.
        subprocess.Popen(["airflow", "webserver", "-D"])
        pidfile = cli.setup_locations("webserver")[0]
        self._wait_pidfile(pidfile)

        # Assert that gunicorn and its monitor are launched.
        self.assertEqual(0, subprocess.Popen(["pgrep", "-c", "airflow"]).wait())
        self.assertEqual(0, subprocess.Popen(["pgrep", "-c", "gunicorn"]).wait())

        # Terminate monitor process.
        pidfile = cli.setup_locations("webserver-monitor")[0]
        pid = self._wait_pidfile(pidfile)
        p = psutil.Process(pid)
        p.terminate()
        p.wait()

        # Assert that no process remains.
        self.assertEqual(1, subprocess.Popen(["pgrep", "-c", "airflow"]).wait())
        self.assertEqual(1, subprocess.Popen(["pgrep", "-c", "gunicorn"]).wait())

    # Patch for causing webserver timeout
    @mock.patch("airflow.bin.cli.get_num_workers_running", return_value=0)
    def test_cli_webserver_shutdown_when_gunicorn_master_is_killed(self, _):
        # Shorten timeout so that this test doesn't take too long time
        args = self.parser.parse_args(['webserver'])
        with conf_vars({('webserver', 'web_server_master_timeout'): '10'}):
            with self.assertRaises(SystemExit) as e:
                cli.webserver(args)
        self.assertEqual(e.exception.code, 1)


class FakeWebHDFSHook:
    def __init__(self, conn_id):
        self.conn_id = conn_id

    def get_conn(self):
        return self.conn_id

    def check_for_path(self, hdfs_path):
        return hdfs_path


class FakeSnakeBiteClientException(Exception):
    pass


class FakeSnakeBiteClient:

    def __init__(self):
        self.started = True

    def ls(self, path, include_toplevel=False):
        """
        the fake snakebite client
        :param path: the array of path to test
        :param include_toplevel: to return the toplevel directory info
        :return: a list for path for the matching queries
        """
        if path[0] == '/datadirectory/empty_directory' and not include_toplevel:
            return []
        elif path[0] == '/datadirectory/datafile':
            return [{
                'group': 'supergroup',
                'permission': 420,
                'file_type': 'f',
                'access_time': 1481122343796,
                'block_replication': 3,
                'modification_time': 1481122343862,
                'length': 0,
                'blocksize': 134217728,
                'owner': 'hdfs',
                'path': '/datadirectory/datafile'
            }]
        elif path[0] == '/datadirectory/empty_directory' and include_toplevel:
            return [{
                'group': 'supergroup',
                'permission': 493,
                'file_type': 'd',
                'access_time': 0,
                'block_replication': 0,
                'modification_time': 1481132141540,
                'length': 0,
                'blocksize': 0,
                'owner': 'hdfs',
                'path': '/datadirectory/empty_directory'
            }]
        elif path[0] == '/datadirectory/not_empty_directory' and include_toplevel:
            return [{
                'group': 'supergroup',
                'permission': 493,
                'file_type': 'd',
                'access_time': 0,
                'block_replication': 0,
                'modification_time': 1481132141540,
                'length': 0,
                'blocksize': 0,
                'owner': 'hdfs',
                'path': '/datadirectory/empty_directory'
            }, {
                'group': 'supergroup',
                'permission': 420,
                'file_type': 'f',
                'access_time': 1481122343796,
                'block_replication': 3,
                'modification_time': 1481122343862,
                'length': 0,
                'blocksize': 134217728,
                'owner': 'hdfs',
                'path': '/datadirectory/not_empty_directory/test_file'
            }]
        elif path[0] == '/datadirectory/not_empty_directory':
            return [{
                'group': 'supergroup',
                'permission': 420,
                'file_type': 'f',
                'access_time': 1481122343796,
                'block_replication': 3,
                'modification_time': 1481122343862,
                'length': 0,
                'blocksize': 134217728,
                'owner': 'hdfs',
                'path': '/datadirectory/not_empty_directory/test_file'
            }]
        elif path[0] == '/datadirectory/not_existing_file_or_directory':
            raise FakeSnakeBiteClientException
        elif path[0] == '/datadirectory/regex_dir':
            return [{
                'group': 'supergroup',
                'permission': 420,
                'file_type': 'f',
                'access_time': 1481122343796,
                'block_replication': 3,
                'modification_time': 1481122343862, 'length': 12582912,
                'blocksize': 134217728,
                'owner': 'hdfs',
                'path': '/datadirectory/regex_dir/test1file'
            }, {
                'group': 'supergroup',
                'permission': 420,
                'file_type': 'f',
                'access_time': 1481122343796,
                'block_replication': 3,
                'modification_time': 1481122343862,
                'length': 12582912,
                'blocksize': 134217728,
                'owner': 'hdfs',
                'path': '/datadirectory/regex_dir/test2file'
            }, {
                'group': 'supergroup',
                'permission': 420,
                'file_type': 'f',
                'access_time': 1481122343796,
                'block_replication': 3,
                'modification_time': 1481122343862,
                'length': 12582912,
                'blocksize': 134217728,
                'owner': 'hdfs',
                'path': '/datadirectory/regex_dir/test3file'
            }, {
                'group': 'supergroup',
                'permission': 420,
                'file_type': 'f',
                'access_time': 1481122343796,
                'block_replication': 3,
                'modification_time': 1481122343862,
                'length': 12582912,
                'blocksize': 134217728,
                'owner': 'hdfs',
                'path': '/datadirectory/regex_dir/copying_file_1.txt._COPYING_'
            }, {
                'group': 'supergroup',
                'permission': 420,
                'file_type': 'f',
                'access_time': 1481122343796,
                'block_replication': 3,
                'modification_time': 1481122343862,
                'length': 12582912,
                'blocksize': 134217728,
                'owner': 'hdfs',
                'path': '/datadirectory/regex_dir/copying_file_3.txt.sftp'
            }]
        else:
            raise FakeSnakeBiteClientException


class FakeHDFSHook:
    def __init__(self, conn_id=None):
        self.conn_id = conn_id

    def get_conn(self):
        client = FakeSnakeBiteClient()
        return client


class TestConnection(unittest.TestCase):
    def setUp(self):
        utils.db.initdb()
        os.environ['AIRFLOW_CONN_TEST_URI'] = (
            'postgres://username:password@ec2.compute.com:5432/the_database')
        os.environ['AIRFLOW_CONN_TEST_URI_NO_CREDS'] = (
            'postgres://ec2.compute.com/the_database')

    def tearDown(self):
        env_vars = ['AIRFLOW_CONN_TEST_URI', 'AIRFLOW_CONN_AIRFLOW_DB']
        for ev in env_vars:
            if ev in os.environ:
                del os.environ[ev]

    def test_using_env_var(self):
        c = SqliteHook.get_connection(conn_id='test_uri')
        self.assertEqual('ec2.compute.com', c.host)
        self.assertEqual('the_database', c.schema)
        self.assertEqual('username', c.login)
        self.assertEqual('password', c.password)
        self.assertEqual(5432, c.port)

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

        os.environ['AIRFLOW_CONN_AIRFLOW_DB'] = \
            'postgres://username:password@ec2.compute.com:5432/the_database'
        c = SqliteHook.get_connection(conn_id='airflow_db')
        self.assertEqual('ec2.compute.com', c.host)
        self.assertEqual('the_database', c.schema)
        self.assertEqual('username', c.login)
        self.assertEqual('password', c.password)
        self.assertEqual(5432, c.port)
        del os.environ['AIRFLOW_CONN_AIRFLOW_DB']

    def test_dbapi_get_uri(self):
        conn = BaseHook.get_connection(conn_id='test_uri')
        hook = conn.get_hook()
        self.assertEqual('postgres://username:password@ec2.compute.com:5432/the_database', hook.get_uri())
        conn2 = BaseHook.get_connection(conn_id='test_uri_no_creds')
        hook2 = conn2.get_hook()
        self.assertEqual('postgres://ec2.compute.com/the_database', hook2.get_uri())

    def test_dbapi_get_sqlalchemy_engine(self):
        conn = BaseHook.get_connection(conn_id='test_uri')
        hook = conn.get_hook()
        engine = hook.get_sqlalchemy_engine()
        self.assertIsInstance(engine, sqlalchemy.engine.Engine)
        self.assertEqual('postgres://username:password@ec2.compute.com:5432/the_database', str(engine.url))

    def test_get_connections_env_var(self):
        conns = SqliteHook.get_connections(conn_id='test_uri')
        assert len(conns) == 1
        assert conns[0].host == 'ec2.compute.com'
        assert conns[0].schema == 'the_database'
        assert conns[0].login == 'username'
        assert conns[0].password == 'password'
        assert conns[0].port == 5432


class TestWebHDFSHook(unittest.TestCase):
    def test_simple_init(self):
        from airflow.hooks.webhdfs_hook import WebHDFSHook
        c = WebHDFSHook()
        self.assertIsNone(c.proxy_user)

    def test_init_proxy_user(self):
        from airflow.hooks.webhdfs_hook import WebHDFSHook
        c = WebHDFSHook(proxy_user='someone')
        self.assertEqual('someone', c.proxy_user)


HDFSHook = None  # type: Optional[hdfs_hook.HDFSHook]
snakebite = None  # type: None


@unittest.skipIf(HDFSHook is None,
                 "Skipping test because HDFSHook is not installed")
class TestHDFSHook(unittest.TestCase):
    def setUp(self):
        os.environ['AIRFLOW_CONN_HDFS_DEFAULT'] = 'hdfs://localhost:8020'

    def test_get_client(self):
        client = HDFSHook(proxy_user='foo').get_conn()
        self.assertIsInstance(client, snakebite.client.Client)
        self.assertEqual('localhost', client.host)
        self.assertEqual(8020, client.port)
        self.assertEqual('foo', client.service.channel.effective_user)

    @mock.patch('airflow.hooks.hdfs_hook.AutoConfigClient')
    @mock.patch('airflow.hooks.hdfs_hook.HDFSHook.get_connections')
    def test_get_autoconfig_client(self, mock_get_connections,
                                   MockAutoConfigClient):
        c = Connection(conn_id='hdfs', conn_type='hdfs',
                       host='localhost', port=8020, login='foo',
                       extra=json.dumps({'autoconfig': True}))
        mock_get_connections.return_value = [c]
        HDFSHook(hdfs_conn_id='hdfs').get_conn()
        MockAutoConfigClient.assert_called_once_with(effective_user='foo',
                                                     use_sasl=False)

    @mock.patch('airflow.hooks.hdfs_hook.AutoConfigClient')
    def test_get_autoconfig_client_no_conn(self, MockAutoConfigClient):
        HDFSHook(hdfs_conn_id='hdfs_missing', autoconfig=True).get_conn()
        MockAutoConfigClient.assert_called_once_with(effective_user=None,
                                                     use_sasl=False)

    @mock.patch('airflow.hooks.hdfs_hook.HDFSHook.get_connections')
    def test_get_ha_client(self, mock_get_connections):
        c1 = Connection(conn_id='hdfs_default', conn_type='hdfs',
                        host='localhost', port=8020)
        c2 = Connection(conn_id='hdfs_default', conn_type='hdfs',
                        host='localhost2', port=8020)
        mock_get_connections.return_value = [c1, c2]
        client = HDFSHook().get_conn()
        self.assertIsInstance(client, snakebite.client.HAClient)


send_email_test = mock.Mock()


class TestEmail(unittest.TestCase):
    def setUp(self):
        conf.remove_option('email', 'EMAIL_BACKEND')

    @mock.patch('airflow.utils.email.send_email')
    def test_default_backend(self, mock_send_email):
        res = utils.email.send_email('to', 'subject', 'content')
        mock_send_email.assert_called_once_with('to', 'subject', 'content')
        self.assertEqual(mock_send_email.return_value, res)

    @mock.patch('airflow.utils.email.send_email_smtp')
    def test_custom_backend(self, mock_send_email):
        with conf_vars({('email', 'email_backend'): 'tests.core.send_email_test'}):
            utils.email.send_email('to', 'subject', 'content')
        send_email_test.assert_called_once_with(
            'to', 'subject', 'content', files=None, dryrun=False,
            cc=None, bcc=None, mime_charset='utf-8', mime_subtype='mixed')
        self.assertFalse(mock_send_email.called)


class TestEmailSmtp(unittest.TestCase):
    def setUp(self):
        conf.set('smtp', 'SMTP_SSL', 'False')

    @mock.patch('airflow.utils.email.send_MIME_email')
    def test_send_smtp(self, mock_send_mime):
        attachment = tempfile.NamedTemporaryFile()
        attachment.write(b'attachment')
        attachment.seek(0)
        utils.email.send_email_smtp('to', 'subject', 'content', files=[attachment.name])
        self.assertTrue(mock_send_mime.called)
        call_args = mock_send_mime.call_args[0]
        self.assertEqual(conf.get('smtp', 'SMTP_MAIL_FROM'), call_args[0])
        self.assertEqual(['to'], call_args[1])
        msg = call_args[2]
        self.assertEqual('subject', msg['Subject'])
        self.assertEqual(conf.get('smtp', 'SMTP_MAIL_FROM'), msg['From'])
        self.assertEqual(2, len(msg.get_payload()))
        filename = 'attachment; filename="' + os.path.basename(attachment.name) + '"'
        self.assertEqual(filename, msg.get_payload()[-1].get('Content-Disposition'))
        mimeapp = MIMEApplication('attachment')
        self.assertEqual(mimeapp.get_payload(), msg.get_payload()[-1].get_payload())

    @mock.patch('airflow.utils.email.send_MIME_email')
    def test_send_smtp_with_multibyte_content(self, mock_send_mime):
        utils.email.send_email_smtp('to', 'subject', '🔥', mime_charset='utf-8')
        self.assertTrue(mock_send_mime.called)
        call_args = mock_send_mime.call_args[0]
        msg = call_args[2]
        mimetext = MIMEText('🔥', 'mixed', 'utf-8')
        self.assertEqual(mimetext.get_payload(), msg.get_payload()[0].get_payload())

    @mock.patch('airflow.utils.email.send_MIME_email')
    def test_send_bcc_smtp(self, mock_send_mime):
        attachment = tempfile.NamedTemporaryFile()
        attachment.write(b'attachment')
        attachment.seek(0)
        utils.email.send_email_smtp('to', 'subject', 'content', files=[attachment.name], cc='cc', bcc='bcc')
        self.assertTrue(mock_send_mime.called)
        call_args = mock_send_mime.call_args[0]
        self.assertEqual(conf.get('smtp', 'SMTP_MAIL_FROM'), call_args[0])
        self.assertEqual(['to', 'cc', 'bcc'], call_args[1])
        msg = call_args[2]
        self.assertEqual('subject', msg['Subject'])
        self.assertEqual(conf.get('smtp', 'SMTP_MAIL_FROM'), msg['From'])
        self.assertEqual(2, len(msg.get_payload()))
        self.assertEqual('attachment; filename="' + os.path.basename(attachment.name) + '"',
                         msg.get_payload()[-1].get('Content-Disposition'))
        mimeapp = MIMEApplication('attachment')
        self.assertEqual(mimeapp.get_payload(), msg.get_payload()[-1].get_payload())

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime(self, mock_smtp, mock_smtp_ssl):
        mock_smtp.return_value = mock.Mock()
        mock_smtp_ssl.return_value = mock.Mock()
        msg = MIMEMultipart()
        utils.email.send_MIME_email('from', 'to', msg, dryrun=False)
        mock_smtp.assert_called_once_with(
            conf.get('smtp', 'SMTP_HOST'),
            conf.getint('smtp', 'SMTP_PORT'),
        )
        self.assertTrue(mock_smtp.return_value.starttls.called)
        mock_smtp.return_value.login.assert_called_once_with(
            conf.get('smtp', 'SMTP_USER'),
            conf.get('smtp', 'SMTP_PASSWORD'),
        )
        mock_smtp.return_value.sendmail.assert_called_once_with('from', 'to', msg.as_string())
        self.assertTrue(mock_smtp.return_value.quit.called)

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_ssl(self, mock_smtp, mock_smtp_ssl):
        mock_smtp.return_value = mock.Mock()
        mock_smtp_ssl.return_value = mock.Mock()
        with conf_vars({('smtp', 'smtp_ssl'): 'True'}):
            utils.email.send_MIME_email('from', 'to', MIMEMultipart(), dryrun=False)
        self.assertFalse(mock_smtp.called)
        mock_smtp_ssl.assert_called_once_with(
            conf.get('smtp', 'SMTP_HOST'),
            conf.getint('smtp', 'SMTP_PORT'),
        )

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_noauth(self, mock_smtp, mock_smtp_ssl):
        mock_smtp.return_value = mock.Mock()
        mock_smtp_ssl.return_value = mock.Mock()
        with conf_vars({
                ('smtp', 'smtp_user'): None,
                ('smtp', 'smtp_password'): None,
        }):
            utils.email.send_MIME_email('from', 'to', MIMEMultipart(), dryrun=False)
        self.assertFalse(mock_smtp_ssl.called)
        mock_smtp.assert_called_once_with(
            conf.get('smtp', 'SMTP_HOST'),
            conf.getint('smtp', 'SMTP_PORT'),
        )
        self.assertFalse(mock_smtp.login.called)

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_dryrun(self, mock_smtp, mock_smtp_ssl):
        utils.email.send_MIME_email('from', 'to', MIMEMultipart(), dryrun=True)
        self.assertFalse(mock_smtp.called)
        self.assertFalse(mock_smtp_ssl.called)


if __name__ == '__main__':
    unittest.main()
