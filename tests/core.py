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

from __future__ import print_function

import doctest
import os
import re
import unittest
import multiprocessing
import mock
from numpy.testing import assert_array_almost_equal
import tempfile
from datetime import datetime, time, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import signal
from time import sleep
import warnings

from dateutil.relativedelta import relativedelta
import sqlalchemy

from airflow import configuration
from airflow.executors import SequentialExecutor, LocalExecutor
from airflow.models import Variable
from tests.test_utils.fake_datetime import FakeDatetime

configuration.load_test_config()
from airflow import jobs, models, DAG, utils, macros, settings, exceptions
from airflow.models import BaseOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.check_operator import CheckOperator, ValueCheckOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators import sensors
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.bin import cli
from airflow.www import app as application
from airflow.settings import Session
from airflow.utils.state import State
from airflow.utils.dates import infer_time_unit, round_time, scale_time_units
from airflow.utils.logging import LoggingMixin
from lxml import html
from airflow.exceptions import AirflowException
from airflow.configuration import AirflowConfigException

import six

NUM_EXAMPLE_DAGS = 18
DEV_NULL = '/dev/null'
TEST_DAG_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'dags')
DEFAULT_DATE = datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = 'unit_tests'


try:
    import cPickle as pickle
except ImportError:
    # Python 3
    import pickle


def reset(dag_id=TEST_DAG_ID):
    session = Session()
    tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
    tis.delete()
    session.commit()
    session.close()


reset()


class OperatorSubclass(BaseOperator):
    """
    An operator to test template substitution
    """
    template_fields = ['some_templated_field']

    def __init__(self, some_templated_field, *args, **kwargs):
        super(OperatorSubclass, self).__init__(*args, **kwargs)
        self.some_templated_field = some_templated_field

    def execute(*args, **kwargs):
        pass


class CoreTest(unittest.TestCase):

    # These defaults make the test faster to run
    default_scheduler_args = {"file_process_interval": 0,
                              "processor_poll_interval": 0.5,
                              "num_runs": 1}

    def setUp(self):
        configuration.load_test_config()
        self.dagbag = models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=self.args)
        self.dag = dag
        self.dag_bash = self.dagbag.dags['example_bash_operator']
        self.runme_0 = self.dag_bash.get_task('runme_0')
        self.run_after_loop = self.dag_bash.get_task('run_after_loop')
        self.run_this_last = self.dag_bash.get_task('run_this_last')

    def test_schedule_dag_no_previous_runs(self):
        """
        Tests scheduling a dag with no previous runs
        """
        dag = DAG(TEST_DAG_ID + 'test_schedule_dag_no_previous_runs')
        dag.add_task(models.BaseOperator(
            task_id="faketastic",
            owner='Also fake',
            start_date=datetime(2015, 1, 2, 0, 0)))

        dag_run = jobs.SchedulerJob(**self.default_scheduler_args).create_dag_run(dag)
        self.assertIsNotNone(dag_run)
        self.assertEqual(dag.dag_id, dag_run.dag_id)
        self.assertIsNotNone(dag_run.run_id)
        self.assertNotEqual('', dag_run.run_id)
        self.assertEqual(datetime(2015, 1, 2, 0, 0), dag_run.execution_date, msg=
            'dag_run.execution_date did not match expectation: {0}'
                .format(dag_run.execution_date))
        self.assertEqual(State.RUNNING, dag_run.state)
        self.assertFalse(dag_run.external_trigger)
        dag.clear()

    def test_schedule_dag_fake_scheduled_previous(self):
        """
        Test scheduling a dag where there is a prior DagRun
        which has the same run_id as the next run should have
        """
        delta = timedelta(hours=1)
        dag = DAG(TEST_DAG_ID + 'test_schedule_dag_fake_scheduled_previous',
                  schedule_interval=delta,
                  start_date=DEFAULT_DATE)
        dag.add_task(models.BaseOperator(
            task_id="faketastic",
            owner='Also fake',
            start_date=DEFAULT_DATE))

        scheduler = jobs.SchedulerJob(**self.default_scheduler_args)
        dag.create_dagrun(run_id=models.DagRun.id_for_date(DEFAULT_DATE),
                          execution_date=DEFAULT_DATE,
                          state=State.SUCCESS,
                          external_trigger=True)
        dag_run = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dag_run)
        self.assertEqual(dag.dag_id, dag_run.dag_id)
        self.assertIsNotNone(dag_run.run_id)
        self.assertNotEqual('', dag_run.run_id)
        self.assertEqual(DEFAULT_DATE + delta, dag_run.execution_date, msg=
            'dag_run.execution_date did not match expectation: {0}'
                .format(dag_run.execution_date))
        self.assertEqual(State.RUNNING, dag_run.state)
        self.assertFalse(dag_run.external_trigger)

    def test_schedule_dag_once(self):
        """
        Tests scheduling a dag scheduled for @once - should be scheduled the first time
        it is called, and not scheduled the second.
        """
        dag = DAG(TEST_DAG_ID + 'test_schedule_dag_once')
        dag.schedule_interval = '@once'
        dag.add_task(models.BaseOperator(
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
        dag.add_task(models.BaseOperator(
            task_id="faketastic",
            owner='Also fake',
            start_date=datetime(2015, 1, 2, 0, 0)))

        start_date = datetime.now()

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
        dag = DAG(TEST_DAG_ID + 'test_schedule_dag_start_end_dates',
                  start_date=start_date,
                  end_date=end_date,
                  schedule_interval=delta)
        dag.add_task(models.BaseOperator(task_id='faketastic',
                                         owner='Also fake'))

        # Create and schedule the dag runs
        dag_runs = []
        scheduler = jobs.SchedulerJob(**self.default_scheduler_args)
        for i in range(runs):
            dag_runs.append(scheduler.create_dag_run(dag))

        additional_dag_run = scheduler.create_dag_run(dag)

        for dag_run in dag_runs:
            self.assertIsNotNone(dag_run)

        self.assertIsNone(additional_dag_run)

    @mock.patch('airflow.jobs.datetime', FakeDatetime)
    def test_schedule_dag_no_end_date_up_to_today_only(self):
        """
        Tests that a Dag created without an end_date can only be scheduled up
        to and including the current datetime.

        For example, if today is 2016-01-01 and we are scheduling from a
        start_date of 2015-01-01, only jobs up to, but not including
        2016-01-01 should be scheduled.
        """
        from datetime import datetime
        FakeDatetime.now = classmethod(lambda cls: datetime(2016, 1, 1))

        session = settings.Session()
        delta = timedelta(days=1)
        start_date = DEFAULT_DATE
        runs = 365
        dag = DAG(TEST_DAG_ID + 'test_schedule_dag_no_end_date_up_to_today_only',
                  start_date=start_date,
                  schedule_interval=delta)
        dag.add_task(models.BaseOperator(task_id='faketastic',
                                         owner='Also fake'))

        dag_runs = []
        scheduler = jobs.SchedulerJob(**self.default_scheduler_args)
        for i in range(runs):
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
        self.assertTrue(configuration.get('core', 'unit_test_mode'))

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
        self.assertEqual(pickle.loads(pickle.dumps(self.dag)), self.dag)

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

    def test_time_sensor(self):
        t = sensors.TimeSensor(
            task_id='time_sensor_check',
            target_time=time(0),
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

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
        ti = models.TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.are_dependents_done()

    def test_illegal_args(self):
        """
        Tests that Operators reject illegal arguments
        """
        with warnings.catch_warnings(record=True) as w:
            t = BashOperator(
                task_id='test_illegal_args',
                bash_command='echo success',
                dag=self.dag,
                illegal_argument_1234='hello?')
            self.assertTrue(
                issubclass(w[0].category, PendingDeprecationWarning))
            self.assertIn(
                'Invalid arguments were passed to BashOperator.',
                w[0].message.args[0])

    def test_bash_operator(self):
        t = BashOperator(
            task_id='time_sensor_check',
            bash_command="echo success",
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_bash_operator_multi_byte_output(self):
        t = BashOperator(
            task_id='test_multi_byte_bash_operator',
            bash_command=u"echo \u2600",
            dag=self.dag,
            output_encoding='utf-8')
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_bash_operator_kill(self):
        import subprocess
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

    def test_trigger_dagrun(self):
        def trigga(context, obj):
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
            task_id='time_sensor_check',
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

    def test_timedelta_sensor(self):
        t = sensors.TimeDeltaSensor(
            task_id='timedelta_sensor_check',
            delta=timedelta(seconds=2),
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_sensor(self):
        t = sensors.ExternalTaskSensor(
            task_id='test_external_task_sensor_check',
            external_dag_id=TEST_DAG_ID,
            external_task_id='time_sensor_check',
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_sensor_delta(self):
        t = sensors.ExternalTaskSensor(
            task_id='test_external_task_sensor_check_delta',
            external_dag_id=TEST_DAG_ID,
            external_task_id='time_sensor_check',
            execution_delta=timedelta(0),
            allowed_states=['success'],
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_sensor_fn(self):
        self.test_time_sensor()
        # check that the execution_fn works
        t = sensors.ExternalTaskSensor(
            task_id='test_external_task_sensor_check_delta',
            external_dag_id=TEST_DAG_ID,
            external_task_id='time_sensor_check',
            execution_date_fn=lambda dt: dt + timedelta(0),
            allowed_states=['success'],
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # double check that the execution is being called by failing the test
        t2 = sensors.ExternalTaskSensor(
            task_id='test_external_task_sensor_check_delta',
            external_dag_id=TEST_DAG_ID,
            external_task_id='time_sensor_check',
            execution_date_fn=lambda dt: dt + timedelta(days=1),
            allowed_states=['success'],
            timeout=1,
            poke_interval=1,
            dag=self.dag)
        with self.assertRaises(exceptions.AirflowSensorTimeout):
            t2.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_sensor_error_delta_and_fn(self):
        """
        Test that providing execution_delta and a function raises an error
        """
        with self.assertRaises(ValueError):
            t = sensors.ExternalTaskSensor(
                task_id='test_external_task_sensor_check_delta',
                external_dag_id=TEST_DAG_ID,
                external_task_id='time_sensor_check',
                execution_delta=timedelta(0),
                execution_date_fn=lambda dt: dt,
                allowed_states=['success'],
                dag=self.dag)

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
            provide_context=True,
            python_callable=test_py_op,
            templates_dict={'ds': "{{ ds }}"},
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_complex_template(self):
        def verify_templated_field(context):
            self.assertEqual(context['ti'].task.some_templated_field['bar'][1], context['ds'])

        t = OperatorSubclass(
            task_id='test_complex_template',
            some_templated_field={
                'foo': '123',
                'bar': ['baz', '{{ ds }}']
            },
            on_success_callback=verify_templated_field,
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_template_with_variable(self):
        """
        Test the availability of variables in templates
        """
        val = {
            'success':False,
            'test_value': 'a test value'
        }
        Variable.set("a_variable", val['test_value'])

        def verify_templated_field(context):
            self.assertEqual(context['ti'].task.some_templated_field,
                             val['test_value'])
            val['success'] = True

        t = OperatorSubclass(
            task_id='test_complex_template',
            some_templated_field='{{ var.value.a_variable }}',
            on_success_callback=verify_templated_field,
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        self.assertTrue(val['success'])

    def test_template_with_json_variable(self):
        """
        Test the availability of variables (serialized as JSON) in templates
        """
        val = {
            'success': False,
            'test_value': {'foo': 'bar', 'obj': {'v1': 'yes', 'v2': 'no'}}
        }
        Variable.set("a_variable", val['test_value'], serialize_json=True)

        def verify_templated_field(context):
            self.assertEqual(context['ti'].task.some_templated_field,
                             val['test_value']['obj']['v2'])
            val['success'] = True

        t = OperatorSubclass(
            task_id='test_complex_template',
            some_templated_field='{{ var.json.a_variable.obj.v2 }}',
            on_success_callback=verify_templated_field,
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        self.assertTrue(val['success'])

    def test_template_with_json_variable_as_value(self):
        """
        Test the availability of variables (serialized as JSON) in templates, but
        accessed as a value
        """
        val = {
            'success': False,
            'test_value': {'foo': 'bar'}
        }
        Variable.set("a_variable", val['test_value'], serialize_json=True)

        def verify_templated_field(context):
            self.assertEqual(context['ti'].task.some_templated_field,
                             u'{"foo": "bar"}')
            val['success'] = True

        t = OperatorSubclass(
            task_id='test_complex_template',
            some_templated_field='{{ var.value.a_variable }}',
            on_success_callback=verify_templated_field,
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        self.assertTrue(val['success'])

    def test_template_non_bool(self):
        """
        Test templates can handle objects with no sense of truthiness
        """
        class NonBoolObject(object):
            def __len__(self):
                return NotImplemented
            def __bool__(self):
                return NotImplemented

        t = OperatorSubclass(
            task_id='test_bad_template_obj',
            some_templated_field=NonBoolObject(),
            dag=self.dag)
        t.resolve_template_files()

    def test_import_examples(self):
        self.assertEqual(len(self.dagbag.dags), NUM_EXAMPLE_DAGS)

    def test_local_task_job(self):
        TI = models.TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        job = jobs.LocalTaskJob(task_instance=ti, ignore_ti_state=True)
        job.run()

    @mock.patch('airflow.utils.dag_processing.datetime', FakeDatetime)
    def test_scheduler_job(self):
        FakeDatetime.now = classmethod(lambda cls: datetime(2016, 1, 1))
        job = jobs.SchedulerJob(dag_id='example_bash_operator',
                                **self.default_scheduler_args)
        job.run()
        log_base_directory = configuration.conf.get("scheduler",
            "child_process_log_directory")
        latest_log_directory_path = os.path.join(log_base_directory, "latest")
        # verify that the symlink to the latest logs exists
        self.assertTrue(os.path.islink(latest_log_directory_path))

        # verify that the symlink points to the correct log directory
        log_directory = os.path.join(log_base_directory, "2016-01-01")
        self.assertEqual(os.readlink(latest_log_directory_path), log_directory)

    def test_raw_job(self):
        TI = models.TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        ti.dag = self.dag_bash
        ti.run(ignore_ti_state=True)

    def test_doctests(self):
        modules = [utils, macros]
        for mod in modules:
            failed, tests = doctest.testmod(mod)
            if failed:
                raise Exception("Failed a doctest")

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
        value = {"city": 'Paris', "Hapiness": True}
        Variable.setdefault(key, value, deserialize_json=True)
        self.assertEqual(value, Variable.get(key, deserialize_json=True))

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
        self.assertTrue(configuration.has_option("core", "FERNET_KEY"))
        self.assertFalse(configuration.has_option("core", "FERNET_KEY_CMD"))

        FERNET_KEY = configuration.get('core', 'FERNET_KEY')

        configuration.set("core", "FERNET_KEY_CMD", "printf HELLO")

        FALLBACK_FERNET_KEY = configuration.get(
            "core",
            "FERNET_KEY"
        )

        self.assertEqual(FERNET_KEY, FALLBACK_FERNET_KEY)

        # restore the conf back to the original state
        configuration.remove_option("core", "FERNET_KEY_CMD")

    def test_config_throw_error_when_original_and_fallback_is_absent(self):
        self.assertTrue(configuration.has_option("core", "FERNET_KEY"))
        self.assertFalse(configuration.has_option("core", "FERNET_KEY_CMD"))

        FERNET_KEY = configuration.get("core", "FERNET_KEY")
        configuration.remove_option("core", "FERNET_KEY")

        with self.assertRaises(AirflowConfigException) as cm:
            configuration.get("core", "FERNET_KEY")

        exception = str(cm.exception)
        message = "section/key [core/fernet_key] not found in config"
        self.assertEqual(message, exception)

        # restore the conf back to the original state
        configuration.set("core", "FERNET_KEY", FERNET_KEY)
        self.assertTrue(configuration.has_option("core", "FERNET_KEY"))

    def test_config_override_original_when_non_empty_envvar_is_provided(self):
        key = "AIRFLOW__CORE__FERNET_KEY"
        value = "some value"
        self.assertNotIn(key, os.environ)

        os.environ[key] = value
        FERNET_KEY = configuration.get('core', 'FERNET_KEY')
        self.assertEqual(value, FERNET_KEY)

        # restore the envvar back to the original state
        del os.environ[key]

    def test_config_override_original_when_empty_envvar_is_provided(self):
        key = "AIRFLOW__CORE__FERNET_KEY"
        value = ""
        self.assertNotIn(key, os.environ)

        os.environ[key] = value
        FERNET_KEY = configuration.get('core', 'FERNET_KEY')
        self.assertEqual(value, FERNET_KEY)

        # restore the envvar back to the original state
        del os.environ[key]

    def test_class_with_logger_should_have_logger_with_correct_name(self):

        # each class should automatically receive a logger with a correct name

        class Blah(LoggingMixin):
            pass

        self.assertEqual("tests.core.Blah", Blah().logger.name)
        self.assertEqual("airflow.executors.sequential_executor.SequentialExecutor", SequentialExecutor().logger.name)
        self.assertEqual("airflow.executors.local_executor.LocalExecutor", LocalExecutor().logger.name)

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

    def test_duplicate_dependencies(self):

        regexp = "Dependency (.*)runme_0(.*)run_after_loop(.*) " \
                 "already registered"

        with self.assertRaisesRegexp(AirflowException, regexp):
            self.runme_0.set_downstream(self.run_after_loop)

        with self.assertRaisesRegexp(AirflowException, regexp):
            self.run_after_loop.set_upstream(self.runme_0)

    def test_cyclic_dependencies_1(self):

        regexp = "Cycle detected in DAG. (.*)runme_0(.*)"
        with self.assertRaisesRegexp(AirflowException, regexp):
            self.runme_0.set_upstream(self.run_after_loop)

    def test_cyclic_dependencies_2(self):
        regexp = "Cycle detected in DAG. (.*)run_after_loop(.*)"
        with self.assertRaisesRegexp(AirflowException, regexp):
            self.run_after_loop.set_downstream(self.runme_0)

    def test_cyclic_dependencies_3(self):
        regexp = "Cycle detected in DAG. (.*)run_this_last(.*)"
        with self.assertRaisesRegexp(AirflowException, regexp):
            self.run_this_last.set_downstream(self.runme_0)

    def test_bad_trigger_rule(self):
        with self.assertRaises(AirflowException):
            DummyOperator(
                task_id='test_bad_trigger',
                trigger_rule="non_existant",
                dag=self.dag)

    def test_terminate_task(self):
        """If a task instance's db state get deleted, it should fail"""
        TI = models.TaskInstance
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
        ti = (
            session.query(TI)
            .filter_by(
                dag_id=task.dag_id,
                task_id=task.task_id,
                execution_date=DEFAULT_DATE)
            .one()
        )
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
        except:
            pass
        try:
            f.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        except:
            pass
        p_fails = session.query(models.TaskFail).filter_by(
            task_id='pass_sleepy',
            dag_id=self.dag.dag_id,
            execution_date=DEFAULT_DATE).all()
        f_fails = session.query(models.TaskFail).filter_by(
            task_id='fail_sleepy',
            dag_id=self.dag.dag_id,
            execution_date=DEFAULT_DATE).all()
        print(f_fails)
        self.assertEqual(0, len(p_fails))
        self.assertEqual(1, len(f_fails))
        # C
        self.assertGreaterEqual(sum([f.duration for f in f_fails]), 3)

    def test_dag_stats(self):
        """Correctly sets/dirties/cleans rows of DagStat table"""

        session = settings.Session()

        session.query(models.DagRun).delete()
        session.query(models.DagStat).delete()
        session.commit()

        run1 = self.dag_bash.create_dagrun(
            run_id="run1",
            execution_date=DEFAULT_DATE,
            state=State.RUNNING)

        models.DagStat.clean_dirty([self.dag_bash.dag_id], session=session)

        qry = session.query(models.DagStat).all()

        self.assertEqual(1, len(qry))
        self.assertEqual(self.dag_bash.dag_id, qry[0].dag_id)
        self.assertEqual(State.RUNNING, qry[0].state)
        self.assertEqual(1, qry[0].count)
        self.assertFalse(qry[0].dirty)

        run2 = self.dag_bash.create_dagrun(
            run_id="run2",
            execution_date=DEFAULT_DATE+timedelta(days=1),
            state=State.RUNNING)

        models.DagStat.clean_dirty([self.dag_bash.dag_id], session=session)

        qry = session.query(models.DagStat).all()

        self.assertEqual(1, len(qry))
        self.assertEqual(self.dag_bash.dag_id, qry[0].dag_id)
        self.assertEqual(State.RUNNING, qry[0].state)
        self.assertEqual(2, qry[0].count)
        self.assertFalse(qry[0].dirty)

        session.query(models.DagRun).first().state = State.SUCCESS
        session.commit()

        models.DagStat.clean_dirty([self.dag_bash.dag_id], session=session)

        qry = session.query(models.DagStat).filter(models.DagStat.state == State.SUCCESS).all()
        self.assertEqual(1, len(qry))
        self.assertEqual(self.dag_bash.dag_id, qry[0].dag_id)
        self.assertEqual(State.SUCCESS, qry[0].state)
        self.assertEqual(1, qry[0].count)
        self.assertFalse(qry[0].dirty)

        qry = session.query(models.DagStat).filter(models.DagStat.state == State.RUNNING).all()
        self.assertEqual(1, len(qry))
        self.assertEqual(self.dag_bash.dag_id, qry[0].dag_id)
        self.assertEqual(State.RUNNING, qry[0].state)
        self.assertEqual(1, qry[0].count)
        self.assertFalse(qry[0].dirty)

        session.query(models.DagRun).delete()
        session.query(models.DagStat).delete()
        session.commit()
        session.close()


class CliTests(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        app = application.create_app()
        app.config['TESTING'] = True
        self.parser = cli.CLIFactory.get_parser()
        self.dagbag = models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        # Persist DAGs

    def test_cli_list_dags(self):
        args = self.parser.parse_args(['list_dags', '--report'])
        cli.list_dags(args)

    def test_cli_list_tasks(self):
        for dag_id in self.dagbag.dags.keys():
            args = self.parser.parse_args(['list_tasks', dag_id])
            cli.list_tasks(args)

        args = self.parser.parse_args([
            'list_tasks', 'example_bash_operator', '--tree'])
        cli.list_tasks(args)

    def test_cli_initdb(self):
        cli.initdb(self.parser.parse_args(['initdb']))

    def test_cli_connections_list(self):
        with mock.patch('sys.stdout',
                        new_callable=six.StringIO) as mock_stdout:
            cli.connections(self.parser.parse_args(['connections', '--list']))
            stdout = mock_stdout.getvalue()
        conns = [[x.strip("'") for x in re.findall("'\w+'", line)[:2]]
                  for ii, line in enumerate(stdout.split('\n'))
                  if ii % 2 == 1]
        conns = [conn for conn in conns if len(conn) > 0]

        # Assert that some of the connections are present in the output as
        # expected:
        self.assertIn(['aws_default', 'aws'], conns)
        self.assertIn(['beeline_default', 'beeline'], conns)
        self.assertIn(['bigquery_default', 'bigquery'], conns)
        self.assertIn(['emr_default', 'emr'], conns)
        self.assertIn(['mssql_default', 'mssql'], conns)
        self.assertIn(['mysql_default', 'mysql'], conns)
        self.assertIn(['postgres_default', 'postgres'], conns)

        # Attempt to list connections with invalid cli args
        with mock.patch('sys.stdout',
                        new_callable=six.StringIO) as mock_stdout:
            cli.connections(self.parser.parse_args(
                ['connections', '--list', '--conn_id=fake',
                 '--conn_uri=fake-uri']))
            stdout = mock_stdout.getvalue()

        # Check list attempt stdout
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            ("\tThe following args are not compatible with the " +
             "--list flag: ['conn_id', 'conn_uri']"),
        ])

    def test_cli_connections_add_delete(self):
        # Add connections:
        uri = 'postgresql://airflow:airflow@host:5432/airflow'
        with mock.patch('sys.stdout',
                        new_callable=six.StringIO) as mock_stdout:
            cli.connections(self.parser.parse_args(
                ['connections', '--add', '--conn_id=new1',
                 '--conn_uri=%s' % uri]))
            cli.connections(self.parser.parse_args(
                ['connections', '-a', '--conn_id=new2',
                 '--conn_uri=%s' % uri]))
            cli.connections(self.parser.parse_args(
                ['connections', '--add', '--conn_id=new3',
                 '--conn_uri=%s' % uri, '--conn_extra', "{'extra': 'yes'}"]))
            cli.connections(self.parser.parse_args(
                ['connections', '-a', '--conn_id=new4',
                 '--conn_uri=%s' % uri, '--conn_extra', "{'extra': 'yes'}"]))
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
        ])

        # Attempt to add duplicate
        with mock.patch('sys.stdout',
                        new_callable=six.StringIO) as mock_stdout:
            cli.connections(self.parser.parse_args(
                ['connections', '--add', '--conn_id=new1',
                 '--conn_uri=%s' % uri]))
            stdout = mock_stdout.getvalue()

        # Check stdout for addition attempt
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            "\tA connection with `conn_id`=new1 already exists",
        ])

        # Attempt to add without providing conn_id
        with mock.patch('sys.stdout',
                        new_callable=six.StringIO) as mock_stdout:
            cli.connections(self.parser.parse_args(
                ['connections', '--add', '--conn_uri=%s' % uri]))
            stdout = mock_stdout.getvalue()

        # Check stdout for addition attempt
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            ("\tThe following args are required to add a connection:" +
             " ['conn_id']"),
        ])

        # Attempt to add without providing conn_uri
        with mock.patch('sys.stdout',
                        new_callable=six.StringIO) as mock_stdout:
            cli.connections(self.parser.parse_args(
                ['connections', '--add', '--conn_id=new']))
            stdout = mock_stdout.getvalue()

        # Check stdout for addition attempt
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            ("\tThe following args are required to add a connection:" +
             " ['conn_uri']"),
        ])

        # Prepare to add connections
        session = settings.Session()
        extra = {'new1': None,
                 'new2': None,
                 'new3': "{'extra': 'yes'}",
                 'new4': "{'extra': 'yes'}"}

        # Add connections
        for conn_id in ['new1', 'new2', 'new3', 'new4']:
            result = (session
                      .query(models.Connection)
                      .filter(models.Connection.conn_id == conn_id)
                      .first())
            result = (result.conn_id, result.conn_type, result.host,
                      result.port, result.get_extra())
            self.assertEqual(result, (conn_id, 'postgres', 'host', 5432,
                                      extra[conn_id]))

        # Delete connections
        with mock.patch('sys.stdout',
                        new_callable=six.StringIO) as mock_stdout:
            cli.connections(self.parser.parse_args(
                ['connections', '--delete', '--conn_id=new1']))
            cli.connections(self.parser.parse_args(
                ['connections', '--delete', '--conn_id=new2']))
            cli.connections(self.parser.parse_args(
                ['connections', '--delete', '--conn_id=new3']))
            cli.connections(self.parser.parse_args(
                ['connections', '--delete', '--conn_id=new4']))
            stdout = mock_stdout.getvalue()

        # Check deletion stdout
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            "\tSuccessfully deleted `conn_id`=new1",
            "\tSuccessfully deleted `conn_id`=new2",
            "\tSuccessfully deleted `conn_id`=new3",
            "\tSuccessfully deleted `conn_id`=new4"
        ])

        # Check deletions
        for conn_id in ['new1', 'new2', 'new3', 'new4']:
            result = (session
                      .query(models.Connection)
                      .filter(models.Connection.conn_id == conn_id)
                      .first())

            self.assertTrue(result is None)

        # Attempt to delete a non-existing connnection
        with mock.patch('sys.stdout',
                        new_callable=six.StringIO) as mock_stdout:
            cli.connections(self.parser.parse_args(
                ['connections', '--delete', '--conn_id=fake']))
            stdout = mock_stdout.getvalue()

        # Check deletion attempt stdout
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            "\tDid not find a connection with `conn_id`=fake",
        ])

        # Attempt to delete with invalid cli args
        with mock.patch('sys.stdout',
                        new_callable=six.StringIO) as mock_stdout:
            cli.connections(self.parser.parse_args(
                ['connections', '--delete', '--conn_id=fake',
                 '--conn_uri=%s' % uri]))
            stdout = mock_stdout.getvalue()

        # Check deletion attempt stdout
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            ("\tThe following args are not compatible with the " +
             "--delete flag: ['conn_uri']"),
        ])

        session.close()

    def test_cli_test(self):
        cli.test(self.parser.parse_args([
            'test', 'example_bash_operator', 'runme_0',
            DEFAULT_DATE.isoformat()]))
        cli.test(self.parser.parse_args([
            'test', 'example_bash_operator', 'runme_0', '--dry_run',
            DEFAULT_DATE.isoformat()]))

    def test_cli_test_with_params(self):
        cli.test(self.parser.parse_args([
            'test', 'example_passing_params_via_test_command', 'run_this',
            '-tp', '{"foo":"bar"}', DEFAULT_DATE.isoformat()]))
        cli.test(self.parser.parse_args([
            'test', 'example_passing_params_via_test_command', 'also_run_this',
            '-tp', '{"foo":"bar"}', DEFAULT_DATE.isoformat()]))

    def test_cli_run(self):
        cli.run(self.parser.parse_args([
            'run', 'example_bash_operator', 'runme_0', '-l',
            DEFAULT_DATE.isoformat()]))

    def test_task_state(self):
        cli.task_state(self.parser.parse_args([
            'task_state', 'example_bash_operator', 'runme_0',
            DEFAULT_DATE.isoformat()]))

    def test_dag_state(self):
        self.assertEqual(None, cli.dag_state(self.parser.parse_args([
            'dag_state', 'example_bash_operator', DEFAULT_DATE.isoformat()])))

    def test_pause(self):
        args = self.parser.parse_args([
            'pause', 'example_bash_operator'])
        cli.pause(args)
        self.assertIn(self.dagbag.dags['example_bash_operator'].is_paused, [True, 1])

        args = self.parser.parse_args([
            'unpause', 'example_bash_operator'])
        cli.unpause(args)
        self.assertIn(self.dagbag.dags['example_bash_operator'].is_paused, [False, 0])

    def test_subdag_clear(self):
        args = self.parser.parse_args([
            'clear', 'example_subdag_operator', '--no_confirm'])
        cli.clear(args)
        args = self.parser.parse_args([
            'clear', 'example_subdag_operator', '--no_confirm', '--exclude_subdags'])
        cli.clear(args)

    def test_backfill(self):
        cli.backfill(self.parser.parse_args([
            'backfill', 'example_bash_operator',
            '-s', DEFAULT_DATE.isoformat()]))

        cli.backfill(self.parser.parse_args([
            'backfill', 'example_bash_operator', '-t', 'runme_0', '--dry_run',
            '-s', DEFAULT_DATE.isoformat()]))

        cli.backfill(self.parser.parse_args([
            'backfill', 'example_bash_operator', '--dry_run',
            '-s', DEFAULT_DATE.isoformat()]))

        cli.backfill(self.parser.parse_args([
            'backfill', 'example_bash_operator', '-l',
            '-s', DEFAULT_DATE.isoformat()]))

    def test_process_subdir_path_with_placeholder(self):
        self.assertEqual(os.path.join(settings.DAGS_FOLDER, 'abc'), cli.process_subdir('DAGS_FOLDER/abc'))

    def test_trigger_dag(self):
        cli.trigger_dag(self.parser.parse_args([
            'trigger_dag', 'example_bash_operator',
            '-c', '{"foo": "bar"}']))
        self.assertRaises(
            ValueError,
            cli.trigger_dag,
            self.parser.parse_args([
                'trigger_dag', 'example_bash_operator',
                '--run_id', 'trigger_dag_xxx',
                '-c', 'NOT JSON'])
        )

    def test_pool(self):
        # Checks if all subcommands are properly received
        cli.pool(self.parser.parse_args([
            'pool', '-s', 'foo', '1', '"my foo pool"']))
        cli.pool(self.parser.parse_args([
            'pool', '-g', 'foo']))
        cli.pool(self.parser.parse_args([
            'pool', '-x', 'foo']))

    def test_variables(self):
        # Checks if all subcommands are properly received
        cli.variables(self.parser.parse_args([
            'variables', '-s', 'foo', '{"foo":"bar"}']))
        cli.variables(self.parser.parse_args([
            'variables', '-g', 'foo']))
        cli.variables(self.parser.parse_args([
            'variables', '-g', 'baz', '-d', 'bar']))
        cli.variables(self.parser.parse_args([
            'variables']))
        cli.variables(self.parser.parse_args([
            'variables', '-x', 'bar']))
        cli.variables(self.parser.parse_args([
            'variables', '-i', DEV_NULL]))
        cli.variables(self.parser.parse_args([
            'variables', '-e', DEV_NULL]))

        cli.variables(self.parser.parse_args([
            'variables', '-s', 'bar', 'original']))
        # First export
        cli.variables(self.parser.parse_args([
            'variables', '-e', 'variables1.json']))

        first_exp = open('variables1.json', 'r')

        cli.variables(self.parser.parse_args([
            'variables', '-s', 'bar', 'updated']))
        cli.variables(self.parser.parse_args([
            'variables', '-s', 'foo', '{"foo":"oops"}']))
        cli.variables(self.parser.parse_args([
            'variables', '-x', 'foo']))
        # First import
        cli.variables(self.parser.parse_args([
            'variables', '-i', 'variables1.json']))

        self.assertEqual('original', models.Variable.get('bar'))
        self.assertEqual('{"foo": "bar"}', models.Variable.get('foo'))
        # Second export
        cli.variables(self.parser.parse_args([
            'variables', '-e', 'variables2.json']))

        second_exp = open('variables2.json', 'r')
        self.assertEqual(first_exp.read(), second_exp.read())
        second_exp.close()
        first_exp.close()
        # Second import
        cli.variables(self.parser.parse_args([
            'variables', '-i', 'variables2.json']))

        self.assertEqual('original', models.Variable.get('bar'))
        self.assertEqual('{"foo": "bar"}', models.Variable.get('foo'))

        session = settings.Session()
        session.query(Variable).delete()
        session.commit()
        session.close()
        os.remove('variables1.json')
        os.remove('variables2.json')

class WebUiTests(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        configuration.conf.set("webserver", "authenticate", "False")
        configuration.conf.set("webserver", "expose_config", "True")
        app = application.create_app()
        app.config['TESTING'] = True
        self.app = app.test_client()

        self.dagbag = models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        self.dag_bash = self.dagbag.dags['example_bash_operator']
        self.runme_0 = self.dag_bash.get_task('runme_0')

    def test_index(self):
        response = self.app.get('/', follow_redirects=True)
        self.assertIn("DAGs", response.data.decode('utf-8'))
        self.assertIn("example_bash_operator", response.data.decode('utf-8'))

    def test_query(self):
        response = self.app.get('/admin/queryview/')
        self.assertIn("Ad Hoc Query", response.data.decode('utf-8'))
        response = self.app.get(
            "/admin/queryview/?"
            "conn_id=airflow_db&"
            "sql=SELECT+COUNT%281%29+as+TEST+FROM+task_instance")
        self.assertIn("TEST", response.data.decode('utf-8'))

    def test_health(self):
        response = self.app.get('/health')
        self.assertIn('The server is healthy!', response.data.decode('utf-8'))

    def test_headers(self):
        response = self.app.get('/admin/airflow/headers')
        self.assertIn('"headers":', response.data.decode('utf-8'))

    def test_noaccess(self):
        response = self.app.get('/admin/airflow/noaccess')
        self.assertIn("You don't seem to have access.", response.data.decode('utf-8'))

    def test_pickle_info(self):
        response = self.app.get('/admin/airflow/pickle_info')
        self.assertIn('{', response.data.decode('utf-8'))

    def test_dag_views(self):
        response = self.app.get(
            '/admin/airflow/graph?dag_id=example_bash_operator')
        self.assertIn("runme_0", response.data.decode('utf-8'))
        response = self.app.get(
            '/admin/airflow/tree?num_runs=25&dag_id=example_bash_operator')
        self.assertIn("runme_0", response.data.decode('utf-8'))
        response = self.app.get(
            '/admin/airflow/duration?days=30&dag_id=example_bash_operator')
        self.assertIn("example_bash_operator", response.data.decode('utf-8'))
        response = self.app.get(
            '/admin/airflow/tries?days=30&dag_id=example_bash_operator')
        self.assertIn("example_bash_operator", response.data.decode('utf-8'))
        response = self.app.get(
            '/admin/airflow/landing_times?'
            'days=30&dag_id=example_bash_operator')
        self.assertIn("example_bash_operator", response.data.decode('utf-8'))
        response = self.app.get(
            '/admin/airflow/gantt?dag_id=example_bash_operator')
        self.assertIn("example_bash_operator", response.data.decode('utf-8'))
        response = self.app.get(
            '/admin/airflow/code?dag_id=example_bash_operator')
        self.assertIn("example_bash_operator", response.data.decode('utf-8'))
        response = self.app.get(
            '/admin/airflow/blocked')
        response = self.app.get(
            '/admin/configurationview/')
        self.assertIn("Airflow Configuration", response.data.decode('utf-8'))
        self.assertIn("Running Configuration", response.data.decode('utf-8'))
        response = self.app.get(
            '/admin/airflow/rendered?'
            'task_id=runme_1&dag_id=example_bash_operator&'
            'execution_date={}'.format(DEFAULT_DATE_ISO))
        self.assertIn("example_bash_operator", response.data.decode('utf-8'))
        response = self.app.get(
            '/admin/airflow/log?task_id=run_this_last&'
            'dag_id=example_bash_operator&execution_date={}'
            ''.format(DEFAULT_DATE_ISO))
        self.assertIn("run_this_last", response.data.decode('utf-8'))
        response = self.app.get(
            '/admin/airflow/task?'
            'task_id=runme_0&dag_id=example_bash_operator&'
            'execution_date={}'.format(DEFAULT_DATE_DS))
        self.assertIn("Attributes", response.data.decode('utf-8'))
        response = self.app.get(
            '/admin/airflow/dag_stats')
        self.assertIn("example_bash_operator", response.data.decode('utf-8'))
        response = self.app.get(
            '/admin/airflow/task_stats')
        self.assertIn("example_bash_operator", response.data.decode('utf-8'))
        url = (
            "/admin/airflow/success?task_id=run_this_last&"
            "dag_id=example_bash_operator&upstream=false&downstream=false&"
            "future=false&past=false&execution_date={}&"
            "origin=/admin".format(DEFAULT_DATE_DS))
        response = self.app.get(url)
        self.assertIn("Wait a minute", response.data.decode('utf-8'))
        response = self.app.get(url + "&confirmed=true")
        response = self.app.get(
            '/admin/airflow/clear?task_id=run_this_last&'
            'dag_id=example_bash_operator&future=true&past=false&'
            'upstream=true&downstream=false&'
            'execution_date={}&'
            'origin=/admin'.format(DEFAULT_DATE_DS))
        self.assertIn("Wait a minute", response.data.decode('utf-8'))
        url = (
            "/admin/airflow/success?task_id=section-1&"
            "dag_id=example_subdag_operator&upstream=true&downstream=true&"
            "recursive=true&future=false&past=false&execution_date={}&"
            "origin=/admin".format(DEFAULT_DATE_DS))
        response = self.app.get(url)
        self.assertIn("Wait a minute", response.data.decode('utf-8'))
        self.assertIn("section-1-task-1", response.data.decode('utf-8'))
        self.assertIn("section-1-task-2", response.data.decode('utf-8'))
        self.assertIn("section-1-task-3", response.data.decode('utf-8'))
        self.assertIn("section-1-task-4", response.data.decode('utf-8'))
        self.assertIn("section-1-task-5", response.data.decode('utf-8'))
        response = self.app.get(url + "&confirmed=true")
        url = (
            "/admin/airflow/clear?task_id=runme_1&"
            "dag_id=example_bash_operator&future=false&past=false&"
            "upstream=false&downstream=true&"
            "execution_date={}&"
            "origin=/admin".format(DEFAULT_DATE_DS))
        response = self.app.get(url)
        self.assertIn("Wait a minute", response.data.decode('utf-8'))
        response = self.app.get(url + "&confirmed=true")
        url = (
            "/admin/airflow/run?task_id=runme_0&"
            "dag_id=example_bash_operator&ignore_all_deps=false&ignore_ti_state=true&"
            "ignore_task_deps=true&execution_date={}&"
            "origin=/admin".format(DEFAULT_DATE_DS))
        response = self.app.get(url)
        response = self.app.get(
            "/admin/airflow/refresh?dag_id=example_bash_operator")
        response = self.app.get("/admin/airflow/refresh_all")
        response = self.app.get(
            "/admin/airflow/paused?"
            "dag_id=example_python_operator&is_paused=false")
        response = self.app.get("/admin/xcom", follow_redirects=True)
        self.assertIn("Xcoms", response.data.decode('utf-8'))

    def test_charts(self):
        session = Session()
        chart_label = "Airflow task instance by type"
        chart = session.query(
            models.Chart).filter(models.Chart.label == chart_label).first()
        chart_id = chart.id
        session.close()
        response = self.app.get(
            '/admin/airflow/chart'
            '?chart_id={}&iteration_no=1'.format(chart_id))
        self.assertIn("Airflow task instance by type", response.data.decode('utf-8'))
        response = self.app.get(
            '/admin/airflow/chart_data'
            '?chart_id={}&iteration_no=1'.format(chart_id))
        self.assertIn("example", response.data.decode('utf-8'))
        response = self.app.get(
            '/admin/airflow/dag_details?dag_id=example_branch_operator')
        self.assertIn("run_this_first", response.data.decode('utf-8'))

    def test_fetch_task_instance(self):
        url = (
            "/admin/airflow/object/task_instances?"
            "dag_id=example_bash_operator&"
            "execution_date={}".format(DEFAULT_DATE_DS))
        response = self.app.get(url)
        self.assertIn("{}", response.data.decode('utf-8'))

        TI = models.TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        job = jobs.LocalTaskJob(task_instance=ti, ignore_ti_state=True)
        job.run()

        response = self.app.get(url)
        self.assertIn("runme_0", response.data.decode('utf-8'))

    def tearDown(self):
        configuration.conf.set("webserver", "expose_config", "False")
        self.dag_bash.clear(start_date=DEFAULT_DATE, end_date=datetime.now())


class WebPasswordAuthTest(unittest.TestCase):
    def setUp(self):
        configuration.conf.set("webserver", "authenticate", "True")
        configuration.conf.set("webserver", "auth_backend", "airflow.contrib.auth.backends.password_auth")

        app = application.create_app()
        app.config['TESTING'] = True
        self.app = app.test_client()
        from airflow.contrib.auth.backends.password_auth import PasswordUser

        session = Session()
        user = models.User()
        password_user = PasswordUser(user)
        password_user.username = 'airflow_passwordauth'
        password_user.password = 'password'
        print(password_user._password)
        session.add(password_user)
        session.commit()
        session.close()

    def get_csrf(self, response):
        tree = html.fromstring(response.data)
        form = tree.find('.//form')

        return form.find('.//input[@name="_csrf_token"]').value

    def login(self, username, password):
        response = self.app.get('/admin/airflow/login')
        csrf_token = self.get_csrf(response)

        return self.app.post('/admin/airflow/login', data=dict(
            username=username,
            password=password,
            csrf_token=csrf_token
        ), follow_redirects=True)

    def logout(self):
        return self.app.get('/admin/airflow/logout', follow_redirects=True)

    def test_login_logout_password_auth(self):
        self.assertTrue(configuration.getboolean('webserver', 'authenticate'))

        response = self.login('user1', 'whatever')
        self.assertIn('Incorrect login details', response.data.decode('utf-8'))

        response = self.login('airflow_passwordauth', 'wrongpassword')
        self.assertIn('Incorrect login details', response.data.decode('utf-8'))

        response = self.login('airflow_passwordauth', 'password')
        self.assertIn('Data Profiling', response.data.decode('utf-8'))

        response = self.logout()
        self.assertIn('form-signin', response.data.decode('utf-8'))

    def test_unauthorized_password_auth(self):
        response = self.app.get("/admin/airflow/landing_times")
        self.assertEqual(response.status_code, 302)

    def tearDown(self):
        configuration.load_test_config()
        session = Session()
        session.query(models.User).delete()
        session.commit()
        session.close()
        configuration.conf.set("webserver", "authenticate", "False")


class WebLdapAuthTest(unittest.TestCase):
    def setUp(self):
        configuration.conf.set("webserver", "authenticate", "True")
        configuration.conf.set("webserver", "auth_backend", "airflow.contrib.auth.backends.ldap_auth")
        try:
            configuration.conf.add_section("ldap")
        except:
            pass
        configuration.conf.set("ldap", "uri", "ldap://localhost:3890")
        configuration.conf.set("ldap", "user_filter", "objectClass=*")
        configuration.conf.set("ldap", "user_name_attr", "uid")
        configuration.conf.set("ldap", "bind_user", "cn=Manager,dc=example,dc=com")
        configuration.conf.set("ldap", "bind_password", "insecure")
        configuration.conf.set("ldap", "basedn", "dc=example,dc=com")
        configuration.conf.set("ldap", "cacert", "")

        app = application.create_app()
        app.config['TESTING'] = True
        self.app = app.test_client()

    def get_csrf(self, response):
        tree = html.fromstring(response.data)
        form = tree.find('.//form')

        return form.find('.//input[@name="_csrf_token"]').value

    def login(self, username, password):
        response = self.app.get('/admin/airflow/login')
        csrf_token = self.get_csrf(response)

        return self.app.post('/admin/airflow/login', data=dict(
            username=username,
            password=password,
            csrf_token=csrf_token
        ), follow_redirects=True)

    def logout(self):
        return self.app.get('/admin/airflow/logout', follow_redirects=True)

    def test_login_logout_ldap(self):
        self.assertTrue(configuration.getboolean('webserver', 'authenticate'))

        response = self.login('user1', 'userx')
        self.assertIn('Incorrect login details', response.data.decode('utf-8'))

        response = self.login('userz', 'user1')
        self.assertIn('Incorrect login details', response.data.decode('utf-8'))

        response = self.login('user1', 'user1')
        self.assertIn('Data Profiling', response.data.decode('utf-8'))

        response = self.logout()
        self.assertIn('form-signin', response.data.decode('utf-8'))

    def test_unauthorized(self):
        response = self.app.get("/admin/airflow/landing_times")
        self.assertEqual(response.status_code, 302)

    def test_no_filter(self):
        response = self.login('user1', 'user1')
        self.assertIn('Data Profiling', response.data.decode('utf-8'))
        self.assertIn('Connections', response.data.decode('utf-8'))

    def test_with_filters(self):
        configuration.conf.set('ldap', 'superuser_filter',
                               'description=superuser')
        configuration.conf.set('ldap', 'data_profiler_filter',
                               'description=dataprofiler')

        response = self.login('dataprofiler', 'dataprofiler')
        self.assertIn('Data Profiling', response.data.decode('utf-8'))

        response = self.login('superuser', 'superuser')
        self.assertIn('Connections', response.data.decode('utf-8'))

    def tearDown(self):
        configuration.load_test_config()
        session = Session()
        session.query(models.User).delete()
        session.commit()
        session.close()
        configuration.conf.set("webserver", "authenticate", "False")


class LdapGroupTest(unittest.TestCase):
    def setUp(self):
        configuration.conf.set("webserver", "authenticate", "True")
        configuration.conf.set("webserver", "auth_backend", "airflow.contrib.auth.backends.ldap_auth")
        try:
            configuration.conf.add_section("ldap")
        except:
            pass
        configuration.conf.set("ldap", "uri", "ldap://localhost:3890")
        configuration.conf.set("ldap", "user_filter", "objectClass=*")
        configuration.conf.set("ldap", "user_name_attr", "uid")
        configuration.conf.set("ldap", "bind_user", "cn=Manager,dc=example,dc=com")
        configuration.conf.set("ldap", "bind_password", "insecure")
        configuration.conf.set("ldap", "basedn", "dc=example,dc=com")
        configuration.conf.set("ldap", "cacert", "")

    def test_group_belonging(self):
        from airflow.contrib.auth.backends.ldap_auth import LdapUser
        users = {"user1": ["group1", "group3"],
                 "user2": ["group2"]
                 }
        for user in users:
            mu = models.User(username=user,
                             is_superuser=False)
            auth = LdapUser(mu)
            self.assertEqual(set(users[user]), set(auth.ldap_groups))

    def tearDown(self):
        configuration.load_test_config()
        configuration.conf.set("webserver", "authenticate", "False")


class FakeSession(object):
    def __init__(self):
        from requests import Response
        self.response = Response()
        self.response.status_code = 200
        self.response._content = 'airbnb/airflow'.encode('ascii', 'ignore')

    def send(self, request, **kwargs):
        return self.response

    def prepare_request(self, request):
        return self.response

class HttpOpSensorTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE_ISO}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    @mock.patch('requests.Session', FakeSession)
    def test_get(self):
        t = SimpleHttpOperator(
            task_id='get_op',
            method='GET',
            endpoint='/search',
            data={"client": "ubuntu", "q": "airflow"},
            headers={},
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @mock.patch('requests.Session', FakeSession)
    def test_get_response_check(self):
        t = SimpleHttpOperator(
            task_id='get_op',
            method='GET',
            endpoint='/search',
            data={"client": "ubuntu", "q": "airflow"},
            response_check=lambda response: ("airbnb/airflow" in response.text),
            headers={},
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @mock.patch('requests.Session', FakeSession)
    def test_sensor(self):
        sensor = sensors.HttpSensor(
            task_id='http_sensor_check',
            conn_id='http_default',
            endpoint='/search',
            params={"client": "ubuntu", "q": "airflow"},
            headers={},
            response_check=lambda response: ("airbnb/airflow" in response.text),
            poke_interval=5,
            timeout=15,
            dag=self.dag)
        sensor.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)


class FakeWebHDFSHook(object):
    def __init__(self, conn_id):
        self.conn_id = conn_id

    def get_conn(self):
        return self.conn_id

    def check_for_path(self, hdfs_path):
        return hdfs_path


class FakeSnakeBiteClientException(Exception):
    pass


class FakeSnakeBiteClient(object):

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
            return [{'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                     'block_replication': 3, 'modification_time': 1481122343862, 'length': 0, 'blocksize': 134217728,
                     'owner': u'hdfs', 'path': '/datadirectory/datafile'}]
        elif path[0] == '/datadirectory/empty_directory' and include_toplevel:
            return [
                {'group': u'supergroup', 'permission': 493, 'file_type': 'd', 'access_time': 0, 'block_replication': 0,
                 'modification_time': 1481132141540, 'length': 0, 'blocksize': 0, 'owner': u'hdfs',
                 'path': '/datadirectory/empty_directory'}]
        elif path[0] == '/datadirectory/not_empty_directory' and include_toplevel:
            return [
                {'group': u'supergroup', 'permission': 493, 'file_type': 'd', 'access_time': 0, 'block_replication': 0,
                 'modification_time': 1481132141540, 'length': 0, 'blocksize': 0, 'owner': u'hdfs',
                 'path': '/datadirectory/empty_directory'},
                {'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                 'block_replication': 3, 'modification_time': 1481122343862, 'length': 0, 'blocksize': 134217728,
                 'owner': u'hdfs', 'path': '/datadirectory/not_empty_directory/test_file'}]
        elif path[0] == '/datadirectory/not_empty_directory':
            return [{'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                     'block_replication': 3, 'modification_time': 1481122343862, 'length': 0, 'blocksize': 134217728,
                     'owner': u'hdfs', 'path': '/datadirectory/not_empty_directory/test_file'}]
        elif path[0] == '/datadirectory/not_existing_file_or_directory':
            raise FakeSnakeBiteClientException
        elif path[0] == '/datadirectory/regex_dir':
            return [{'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                     'block_replication': 3, 'modification_time': 1481122343862, 'length': 12582912, 'blocksize': 134217728,
                     'owner': u'hdfs', 'path': '/datadirectory/regex_dir/test1file'},
                    {'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                     'block_replication': 3, 'modification_time': 1481122343862, 'length': 12582912, 'blocksize': 134217728,
                     'owner': u'hdfs', 'path': '/datadirectory/regex_dir/test2file'},
                    {'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                     'block_replication': 3, 'modification_time': 1481122343862, 'length': 12582912, 'blocksize': 134217728,
                     'owner': u'hdfs', 'path': '/datadirectory/regex_dir/test3file'},
                    {'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                     'block_replication': 3, 'modification_time': 1481122343862, 'length': 12582912, 'blocksize': 134217728,
                     'owner': u'hdfs', 'path': '/datadirectory/regex_dir/copying_file_1.txt._COPYING_'},
                    {'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1481122343796,
                     'block_replication': 3, 'modification_time': 1481122343862, 'length': 12582912, 'blocksize': 134217728,
                     'owner': u'hdfs', 'path': '/datadirectory/regex_dir/copying_file_3.txt.sftp'}
                    ]
        else:
            raise FakeSnakeBiteClientException


class FakeHDFSHook(object):
    def __init__(self, conn_id=None):
        self.conn_id = conn_id

    def get_conn(self):
        client = FakeSnakeBiteClient()
        return client


class ConnectionTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
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
        c = models.Connection(conn_id='local_mysql', conn_type='mysql',
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


class WebHDFSHookTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

    def test_simple_init(self):
        from airflow.hooks.webhdfs_hook import WebHDFSHook
        c = WebHDFSHook()
        self.assertIsNone(c.proxy_user)

    def test_init_proxy_user(self):
        from airflow.hooks.webhdfs_hook import WebHDFSHook
        c = WebHDFSHook(proxy_user='someone')
        self.assertEqual('someone', c.proxy_user)


try:
    from airflow.hooks.S3_hook import S3Hook
except ImportError:
    S3Hook = None


@unittest.skipIf(S3Hook is None,
                 "Skipping test because S3Hook is not installed")
class S3HookTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        self.s3_test_url = "s3://test/this/is/not/a-real-key.txt"

    def test_parse_s3_url(self):
        parsed = S3Hook.parse_s3_url(self.s3_test_url)
        self.assertEqual(parsed,
                         ("test", "this/is/not/a-real-key.txt"),
                         "Incorrect parsing of the s3 url")


HELLO_SERVER_CMD = """
import socket, sys
listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(('localhost', 2134))
listener.listen(1)
sys.stdout.write('ready')
sys.stdout.flush()
conn = listener.accept()[0]
conn.sendall(b'hello')
"""


class SSHHookTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        from airflow.contrib.hooks.ssh_hook import SSHHook
        self.hook = SSHHook()
        self.hook.no_host_key_check = True

    def test_remote_cmd(self):
        output = self.hook.check_output(["echo", "-n", "airflow"])
        self.assertEqual(output, b"airflow")

    def test_tunnel(self):
        print("Setting up remote listener")
        import subprocess
        import socket

        self.handle = self.hook.Popen([
            "python", "-c", '"{0}"'.format(HELLO_SERVER_CMD)
        ], stdout=subprocess.PIPE)

        print("Setting up tunnel")
        with self.hook.tunnel(2135, 2134):
            print("Tunnel up")
            server_output = self.handle.stdout.read(5)
            self.assertEqual(server_output, b"ready")
            print("Connecting to server via tunnel")
            s = socket.socket()
            s.connect(("localhost", 2135))
            print("Receiving...", )
            response = s.recv(5)
            self.assertEqual(response, b"hello")
            print("Closing connection")
            s.close()
            print("Waiting for listener...")
            output, _ = self.handle.communicate()
            self.assertEqual(self.handle.returncode, 0)
            print("Closing tunnel")


send_email_test = mock.Mock()


class EmailTest(unittest.TestCase):
    def setUp(self):
        configuration.remove_option('email', 'EMAIL_BACKEND')

    @mock.patch('airflow.utils.email.send_email')
    def test_default_backend(self, mock_send_email):
        res = utils.email.send_email('to', 'subject', 'content')
        mock_send_email.assert_called_with('to', 'subject', 'content')
        self.assertEqual(mock_send_email.return_value, res)

    @mock.patch('airflow.utils.email.send_email_smtp')
    def test_custom_backend(self, mock_send_email):
        configuration.set('email', 'EMAIL_BACKEND', 'tests.core.send_email_test')
        utils.email.send_email('to', 'subject', 'content')
        send_email_test.assert_called_with('to', 'subject', 'content', files=None, dryrun=False, cc=None, bcc=None, mime_subtype='mixed')
        self.assertFalse(mock_send_email.called)


class EmailSmtpTest(unittest.TestCase):
    def setUp(self):
        configuration.set('smtp', 'SMTP_SSL', 'False')

    @mock.patch('airflow.utils.email.send_MIME_email')
    def test_send_smtp(self, mock_send_mime):
        attachment = tempfile.NamedTemporaryFile()
        attachment.write(b'attachment')
        attachment.seek(0)
        utils.email.send_email_smtp('to', 'subject', 'content', files=[attachment.name])
        self.assertTrue(mock_send_mime.called)
        call_args = mock_send_mime.call_args[0]
        self.assertEqual(configuration.get('smtp', 'SMTP_MAIL_FROM'), call_args[0])
        self.assertEqual(['to'], call_args[1])
        msg = call_args[2]
        self.assertEqual('subject', msg['Subject'])
        self.assertEqual(configuration.get('smtp', 'SMTP_MAIL_FROM'), msg['From'])
        self.assertEqual(2, len(msg.get_payload()))
        self.assertEqual(u'attachment; filename="' + os.path.basename(attachment.name) + '"',
            msg.get_payload()[-1].get(u'Content-Disposition'))
        mimeapp = MIMEApplication('attachment')
        self.assertEqual(mimeapp.get_payload(), msg.get_payload()[-1].get_payload())

    @mock.patch('airflow.utils.email.send_MIME_email')
    def test_send_bcc_smtp(self, mock_send_mime):
        attachment = tempfile.NamedTemporaryFile()
        attachment.write(b'attachment')
        attachment.seek(0)
        utils.email.send_email_smtp('to', 'subject', 'content', files=[attachment.name], cc='cc', bcc='bcc')
        self.assertTrue(mock_send_mime.called)
        call_args = mock_send_mime.call_args[0]
        self.assertEqual(configuration.get('smtp', 'SMTP_MAIL_FROM'), call_args[0])
        self.assertEqual(['to', 'cc', 'bcc'], call_args[1])
        msg = call_args[2]
        self.assertEqual('subject', msg['Subject'])
        self.assertEqual(configuration.get('smtp', 'SMTP_MAIL_FROM'), msg['From'])
        self.assertEqual(2, len(msg.get_payload()))
        self.assertEqual(u'attachment; filename="' + os.path.basename(attachment.name) + '"',
            msg.get_payload()[-1].get(u'Content-Disposition'))
        mimeapp = MIMEApplication('attachment')
        self.assertEqual(mimeapp.get_payload(), msg.get_payload()[-1].get_payload())


    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime(self, mock_smtp, mock_smtp_ssl):
        mock_smtp.return_value = mock.Mock()
        mock_smtp_ssl.return_value = mock.Mock()
        msg = MIMEMultipart()
        utils.email.send_MIME_email('from', 'to', msg, dryrun=False)
        mock_smtp.assert_called_with(
            configuration.get('smtp', 'SMTP_HOST'),
            configuration.getint('smtp', 'SMTP_PORT'),
        )
        self.assertTrue(mock_smtp.return_value.starttls.called)
        mock_smtp.return_value.login.assert_called_with(
            configuration.get('smtp', 'SMTP_USER'),
            configuration.get('smtp', 'SMTP_PASSWORD'),
        )
        mock_smtp.return_value.sendmail.assert_called_with('from', 'to', msg.as_string())
        self.assertTrue(mock_smtp.return_value.quit.called)

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_ssl(self, mock_smtp, mock_smtp_ssl):
        configuration.set('smtp', 'SMTP_SSL', 'True')
        mock_smtp.return_value = mock.Mock()
        mock_smtp_ssl.return_value = mock.Mock()
        utils.email.send_MIME_email('from', 'to', MIMEMultipart(), dryrun=False)
        self.assertFalse(mock_smtp.called)
        mock_smtp_ssl.assert_called_with(
            configuration.get('smtp', 'SMTP_HOST'),
            configuration.getint('smtp', 'SMTP_PORT'),
        )

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_noauth(self, mock_smtp, mock_smtp_ssl):
        configuration.conf.remove_option('smtp', 'SMTP_USER')
        configuration.conf.remove_option('smtp', 'SMTP_PASSWORD')
        mock_smtp.return_value = mock.Mock()
        mock_smtp_ssl.return_value = mock.Mock()
        utils.email.send_MIME_email('from', 'to', MIMEMultipart(), dryrun=False)
        self.assertFalse(mock_smtp_ssl.called)
        mock_smtp.assert_called_with(
            configuration.get('smtp', 'SMTP_HOST'),
            configuration.getint('smtp', 'SMTP_PORT'),
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
