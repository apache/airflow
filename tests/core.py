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
import json
import os
import re
import unittest
import multiprocessing
import mock
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
from airflow.utils.dates import round_time
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
        assert dag_run is not None
        assert dag_run.dag_id == dag.dag_id
        assert dag_run.run_id is not None
        assert dag_run.run_id != ''
        assert dag_run.execution_date == datetime(2015, 1, 2, 0, 0), (
            'dag_run.execution_date did not match expectation: {0}'
                .format(dag_run.execution_date))
        assert dag_run.state == State.RUNNING
        assert dag_run.external_trigger == False
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
        assert dag_run is not None
        assert dag_run.dag_id == dag.dag_id
        assert dag_run.run_id is not None
        assert dag_run.run_id != ''
        assert dag_run.execution_date == DEFAULT_DATE + delta, (
            'dag_run.execution_date did not match expectation: {0}'
                .format(dag_run.execution_date))
        assert dag_run.state == State.RUNNING
        assert dag_run.external_trigger == False

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

        assert dag_run is not None
        assert dag_run2 is None
        dag.clear()

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
            assert dag_run is not None

        assert additional_dag_run is None

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
            assert dag_run is not None

        assert additional_dag_run is None

    def test_confirm_unittest_mod(self):
        assert configuration.get('core', 'unit_test_mode')

    def test_pickling(self):
        dp = self.dag.pickle()
        assert self.dag.dag_id == dp.pickle.dag_id

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
        assert self.dag == self.dag

        # test dag (in)equality based on _comps
        assert self.dag == dag_eq
        assert self.dag != dag_diff_name
        assert self.dag != dag_diff_load_time

        # test dag inequality based on type even if _comps happen to match
        assert self.dag != dag_subclass

        # a dag should equal an unpickled version of itself
        assert self.dag == pickle.loads(pickle.dumps(self.dag))

        # dags are ordered based on dag_id no matter what the type is
        assert self.dag < dag_diff_name
        assert not self.dag < dag_diff_load_time
        assert self.dag < dag_subclass_diff_name

        # greater than should have been created automatically by functools
        assert dag_diff_name > self.dag

        # hashes are non-random and match equality
        assert hash(self.dag) == hash(self.dag)
        assert hash(self.dag) == hash(dag_eq)
        assert hash(self.dag) != hash(dag_diff_name)
        assert hash(self.dag) != hash(dag_subclass)

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
        assert val['success']

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
        assert val['success']

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
        assert val['success']

    def test_import_examples(self):
        self.assertEqual(len(self.dagbag.dags), NUM_EXAMPLE_DAGS)

    def test_local_task_job(self):
        TI = models.TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        job = jobs.LocalTaskJob(task_instance=ti, ignore_ti_state=True)
        job.run()

    def test_scheduler_job(self):
        job = jobs.SchedulerJob(dag_id='example_bash_operator',
                                **self.default_scheduler_args)
        job.run()

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
        assert "Monday morning breakfast" == Variable.get("tested_var_set_id")

    def test_variable_set_get_round_trip_json(self):
        value = {"a": 17, "b": 47}
        Variable.set("tested_var_set_id", value, serialize_json=True)
        assert value == Variable.get("tested_var_set_id", deserialize_json=True)

    def test_get_non_existing_var_should_return_default(self):
        default_value = "some default val"
        assert default_value == Variable.get("thisIdDoesNotExist",
                                             default_var=default_value)

    def test_get_non_existing_var_should_not_deserialize_json_default(self):
        default_value = "}{ this is a non JSON default }{"
        assert default_value == Variable.get("thisIdDoesNotExist",
                                             default_var=default_value,
                                             deserialize_json=True)

    def test_parameterized_config_gen(self):

        cfg = configuration.parameterized_config(configuration.DEFAULT_CONFIG)

        # making sure some basic building blocks are present:
        assert "[core]" in cfg
        assert "dags_folder" in cfg
        assert "sql_alchemy_conn" in cfg
        assert "fernet_key" in cfg

        # making sure replacement actually happened
        assert "{AIRFLOW_HOME}" not in cfg
        assert "{FERNET_KEY}" not in cfg

    def test_config_use_original_when_original_and_fallback_are_present(self):
        assert configuration.has_option("core", "FERNET_KEY")
        assert not configuration.has_option("core", "FERNET_KEY_CMD")

        FERNET_KEY = configuration.get('core', 'FERNET_KEY')

        configuration.set("core", "FERNET_KEY_CMD", "printf HELLO")

        FALLBACK_FERNET_KEY = configuration.get(
            "core",
            "FERNET_KEY"
        )

        assert FALLBACK_FERNET_KEY == FERNET_KEY

        # restore the conf back to the original state
        configuration.remove_option("core", "FERNET_KEY_CMD")

    def test_config_throw_error_when_original_and_fallback_is_absent(self):
        assert configuration.has_option("core", "FERNET_KEY")
        assert not configuration.has_option("core", "FERNET_KEY_CMD")

        FERNET_KEY = configuration.get("core", "FERNET_KEY")
        configuration.remove_option("core", "FERNET_KEY")

        with self.assertRaises(AirflowConfigException) as cm:
            configuration.get("core", "FERNET_KEY")

        exception = str(cm.exception)
        message = "section/key [core/fernet_key] not found in config"
        assert exception == message

        # restore the conf back to the original state
        configuration.set("core", "FERNET_KEY", FERNET_KEY)
        assert configuration.has_option("core", "FERNET_KEY")

    def test_class_with_logger_should_have_logger_with_correct_name(self):

        # each class should automatically receive a logger with a correct name

        class Blah(LoggingMixin):
            pass

        assert Blah().logger.name == "tests.core.Blah"
        assert SequentialExecutor().logger.name == "airflow.executors.sequential_executor.SequentialExecutor"
        assert LocalExecutor().logger.name == "airflow.executors.local_executor.LocalExecutor"

    def test_round_time(self):

        rt1 = round_time(datetime(2015, 1, 1, 6), timedelta(days=1))
        assert rt1 == datetime(2015, 1, 1, 0, 0)

        rt2 = round_time(datetime(2015, 1, 2), relativedelta(months=1))
        assert rt2 == datetime(2015, 1, 1, 0, 0)

        rt3 = round_time(datetime(2015, 9, 16, 0, 0), timedelta(1), datetime(
            2015, 9, 14, 0, 0))
        assert rt3 == datetime(2015, 9, 16, 0, 0)

        rt4 = round_time(datetime(2015, 9, 15, 0, 0), timedelta(1), datetime(
            2015, 9, 14, 0, 0))
        assert rt4 == datetime(2015, 9, 15, 0, 0)

        rt5 = round_time(datetime(2015, 9, 14, 0, 0), timedelta(1), datetime(
            2015, 9, 14, 0, 0))
        assert rt5 == datetime(2015, 9, 14, 0, 0)

        rt6 = round_time(datetime(2015, 9, 13, 0, 0), timedelta(1), datetime(
            2015, 9, 14, 0, 0))
        assert rt6 == datetime(2015, 9, 14, 0, 0)

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
        assert State.RUNNING == ti.state
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
        assert State.FAILED == ti.state
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
        assert len(p_fails) == 0
        assert len(f_fails) == 1
        # C
        assert sum([f.duration for f in f_fails]) >= 3

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

        assert len(qry) == 1
        assert qry[0].dag_id == self.dag_bash.dag_id and\
                qry[0].state == State.RUNNING and\
                qry[0].count == 1 and\
                qry[0].dirty == False

        run2 = self.dag_bash.create_dagrun(
            run_id="run2",
            execution_date=DEFAULT_DATE+timedelta(days=1),
            state=State.RUNNING)

        models.DagStat.clean_dirty([self.dag_bash.dag_id], session=session)

        qry = session.query(models.DagStat).all()

        assert len(qry) == 1
        assert qry[0].dag_id == self.dag_bash.dag_id and\
                qry[0].state == State.RUNNING and\
                qry[0].count == 2 and\
                qry[0].dirty == False

        session.query(models.DagRun).first().state = State.SUCCESS
        session.commit()

        models.DagStat.clean_dirty([self.dag_bash.dag_id], session=session)

        qry = session.query(models.DagStat).filter(models.DagStat.state == State.SUCCESS).all()
        assert len(qry) == 1
        assert qry[0].dag_id == self.dag_bash.dag_id and\
                qry[0].state == State.SUCCESS and\
                qry[0].count == 1 and\
                qry[0].dirty == False

        qry = session.query(models.DagStat).filter(models.DagStat.state == State.RUNNING).all()
        assert len(qry) == 1
        assert qry[0].dag_id == self.dag_bash.dag_id and\
                qry[0].state == State.RUNNING and\
                qry[0].count == 1 and\
                qry[0].dirty == False

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
        assert self.dagbag.dags['example_bash_operator'].is_paused in [True, 1]

        args = self.parser.parse_args([
            'unpause', 'example_bash_operator'])
        cli.unpause(args)
        assert self.dagbag.dags['example_bash_operator'].is_paused in [False, 0]

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
        assert cli.process_subdir('DAGS_FOLDER/abc') == os.path.join(configuration.get_dags_folder(), 'abc')

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

        assert models.Variable.get('bar') == 'original'
        assert models.Variable.get('foo') == '{"foo": "bar"}'
        # Second export
        cli.variables(self.parser.parse_args([
            'variables', '-e', 'variables2.json']))

        second_exp = open('variables2.json', 'r')
        assert second_exp.read() == first_exp.read()
        second_exp.close()
        first_exp.close()
        # Second import
        cli.variables(self.parser.parse_args([
            'variables', '-i', 'variables2.json']))

        assert models.Variable.get('bar') == 'original'
        assert models.Variable.get('foo') == '{"foo": "bar"}'

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
        app = application.create_app()
        app.config['TESTING'] = True
        self.app = app.test_client()

        self.dagbag = models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        self.dag_bash = self.dagbag.dags['example_bash_operator']
        self.runme_0 = self.dag_bash.get_task('runme_0')

    def test_index(self):
        response = self.app.get('/', follow_redirects=True)
        assert "DAGs" in response.data.decode('utf-8')
        assert "example_bash_operator" in response.data.decode('utf-8')

    def test_query(self):
        response = self.app.get('/admin/queryview/')
        assert "Ad Hoc Query" in response.data.decode('utf-8')
        response = self.app.get(
            "/admin/queryview/?"
            "conn_id=airflow_db&"
            "sql=SELECT+COUNT%281%29+as+TEST+FROM+task_instance")
        assert "TEST" in response.data.decode('utf-8')

    def test_health(self):
        response = self.app.get('/health')
        assert 'The server is healthy!' in response.data.decode('utf-8')

    def test_dag_views(self):
        response = self.app.get(
            '/admin/airflow/graph?dag_id=example_bash_operator')
        assert "runme_0" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/tree?num_runs=25&dag_id=example_bash_operator')
        assert "runme_0" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/duration?days=30&dag_id=example_bash_operator')
        assert "example_bash_operator" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/tries?days=30&dag_id=example_bash_operator')
        assert "example_bash_operator" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/landing_times?'
            'days=30&dag_id=example_bash_operator')
        assert "example_bash_operator" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/gantt?dag_id=example_bash_operator')
        assert "example_bash_operator" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/code?dag_id=example_bash_operator')
        assert "example_bash_operator" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/blocked')
        response = self.app.get(
            '/admin/configurationview/')
        assert "Airflow Configuration" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/rendered?'
            'task_id=runme_1&dag_id=example_bash_operator&'
            'execution_date={}'.format(DEFAULT_DATE_ISO))
        assert "example_bash_operator" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/log?task_id=run_this_last&'
            'dag_id=example_bash_operator&execution_date={}'
            ''.format(DEFAULT_DATE_ISO))
        assert "run_this_last" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/task?'
            'task_id=runme_0&dag_id=example_bash_operator&'
            'execution_date={}'.format(DEFAULT_DATE_DS))
        assert "Attributes" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/dag_stats')
        assert "example_bash_operator" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/task_stats')
        assert "example_bash_operator" in response.data.decode('utf-8')
        url = (
            "/admin/airflow/success?task_id=run_this_last&"
            "dag_id=example_bash_operator&upstream=false&downstream=false&"
            "future=false&past=false&execution_date={}&"
            "origin=/admin".format(DEFAULT_DATE_DS))
        response = self.app.get(url)
        assert "Wait a minute" in response.data.decode('utf-8')
        response = self.app.get(url + "&confirmed=true")
        response = self.app.get(
            '/admin/airflow/clear?task_id=run_this_last&'
            'dag_id=example_bash_operator&future=true&past=false&'
            'upstream=true&downstream=false&'
            'execution_date={}&'
            'origin=/admin'.format(DEFAULT_DATE_DS))
        assert "Wait a minute" in response.data.decode('utf-8')
        url = (
            "/admin/airflow/success?task_id=section-1&"
            "dag_id=example_subdag_operator&upstream=true&downstream=true&"
            "recursive=true&future=false&past=false&execution_date={}&"
            "origin=/admin".format(DEFAULT_DATE_DS))
        response = self.app.get(url)
        assert "Wait a minute" in response.data.decode('utf-8')
        assert "section-1-task-1" in response.data.decode('utf-8')
        assert "section-1-task-2" in response.data.decode('utf-8')
        assert "section-1-task-3" in response.data.decode('utf-8')
        assert "section-1-task-4" in response.data.decode('utf-8')
        assert "section-1-task-5" in response.data.decode('utf-8')
        response = self.app.get(url + "&confirmed=true")
        url = (
            "/admin/airflow/clear?task_id=runme_1&"
            "dag_id=example_bash_operator&future=false&past=false&"
            "upstream=false&downstream=true&"
            "execution_date={}&"
            "origin=/admin".format(DEFAULT_DATE_DS))
        response = self.app.get(url)
        assert "Wait a minute" in response.data.decode('utf-8')
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
        assert "Airflow task instance by type" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/chart_data'
            '?chart_id={}&iteration_no=1'.format(chart_id))
        assert "example" in response.data.decode('utf-8')
        response = self.app.get(
            '/admin/airflow/dag_details?dag_id=example_branch_operator')
        assert "run_this_first" in response.data.decode('utf-8')

    def test_fetch_task_instance(self):
        url = (
            "/admin/airflow/object/task_instances?"
            "dag_id=example_bash_operator&"
            "execution_date={}".format(DEFAULT_DATE_DS))
        response = self.app.get(url)
        assert "{}" in response.data.decode('utf-8')

        TI = models.TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        job = jobs.LocalTaskJob(task_instance=ti, ignore_ti_state=True)
        job.run()

        response = self.app.get(url)
        assert "runme_0" in response.data.decode('utf-8')

    def tearDown(self):
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
        assert configuration.getboolean('webserver', 'authenticate') is True

        response = self.login('user1', 'whatever')
        assert 'Incorrect login details' in response.data.decode('utf-8')

        response = self.login('airflow_passwordauth', 'wrongpassword')
        assert 'Incorrect login details' in response.data.decode('utf-8')

        response = self.login('airflow_passwordauth', 'password')
        assert 'Data Profiling' in response.data.decode('utf-8')

        response = self.logout()
        assert 'form-signin' in response.data.decode('utf-8')

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
        assert configuration.getboolean('webserver', 'authenticate') is True

        response = self.login('user1', 'userx')
        assert 'Incorrect login details' in response.data.decode('utf-8')

        response = self.login('userz', 'user1')
        assert 'Incorrect login details' in response.data.decode('utf-8')

        response = self.login('user1', 'user1')
        assert 'Data Profiling' in response.data.decode('utf-8')

        response = self.logout()
        assert 'form-signin' in response.data.decode('utf-8')

    def test_unauthorized(self):
        response = self.app.get("/admin/airflow/landing_times")
        self.assertEqual(response.status_code, 302)

    def test_no_filter(self):
        response = self.login('user1', 'user1')
        assert 'Data Profiling' in response.data.decode('utf-8')
        assert 'Connections' in response.data.decode('utf-8')

    def test_with_filters(self):
        configuration.conf.set('ldap', 'superuser_filter',
                               'description=superuser')
        configuration.conf.set('ldap', 'data_profiler_filter',
                               'description=dataprofiler')

        response = self.login('dataprofiler', 'dataprofiler')
        assert 'Data Profiling' in response.data.decode('utf-8')

        response = self.login('superuser', 'superuser')
        assert 'Connections' in response.data.decode('utf-8')

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
            assert set(auth.ldap_groups) == set(users[user])

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
        assert c.host == 'ec2.compute.com'
        assert c.schema == 'the_database'
        assert c.login == 'username'
        assert c.password == 'password'
        assert c.port == 5432

    def test_using_unix_socket_env_var(self):
        c = SqliteHook.get_connection(conn_id='test_uri_no_creds')
        assert c.host == 'ec2.compute.com'
        assert c.schema == 'the_database'
        assert c.login is None
        assert c.password is None
        assert c.port is None

    def test_param_setup(self):
        c = models.Connection(conn_id='local_mysql', conn_type='mysql',
                              host='localhost', login='airflow',
                              password='airflow', schema='airflow')
        assert c.host == 'localhost'
        assert c.schema == 'airflow'
        assert c.login == 'airflow'
        assert c.password == 'airflow'
        assert c.port is None

    def test_env_var_priority(self):
        c = SqliteHook.get_connection(conn_id='airflow_db')
        assert c.host != 'ec2.compute.com'

        os.environ['AIRFLOW_CONN_AIRFLOW_DB'] = \
            'postgres://username:password@ec2.compute.com:5432/the_database'
        c = SqliteHook.get_connection(conn_id='airflow_db')
        assert c.host == 'ec2.compute.com'
        assert c.schema == 'the_database'
        assert c.login == 'username'
        assert c.password == 'password'
        assert c.port == 5432
        del os.environ['AIRFLOW_CONN_AIRFLOW_DB']

    def test_dbapi_get_uri(self):
        conn = BaseHook.get_connection(conn_id='test_uri')
        hook = conn.get_hook()
        assert hook.get_uri() == 'postgres://username:password@ec2.compute.com:5432/the_database'
        conn2 = BaseHook.get_connection(conn_id='test_uri_no_creds')
        hook2 = conn2.get_hook()
        assert hook2.get_uri() == 'postgres://ec2.compute.com/the_database'

    def test_dbapi_get_sqlalchemy_engine(self):
        conn = BaseHook.get_connection(conn_id='test_uri')
        hook = conn.get_hook()
        engine = hook.get_sqlalchemy_engine()
        assert isinstance(engine, sqlalchemy.engine.Engine)
        assert str(engine.url) == 'postgres://username:password@ec2.compute.com:5432/the_database'


class WebHDFSHookTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

    def test_simple_init(self):
        from airflow.hooks.webhdfs_hook import WebHDFSHook
        c = WebHDFSHook()
        assert c.proxy_user == None

    def test_init_proxy_user(self):
        from airflow.hooks.webhdfs_hook import WebHDFSHook
        c = WebHDFSHook(proxy_user='someone')
        assert c.proxy_user == 'someone'


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
        assert res == mock_send_email.return_value

    @mock.patch('airflow.utils.email.send_email_smtp')
    def test_custom_backend(self, mock_send_email):
        configuration.set('email', 'EMAIL_BACKEND', 'tests.core.send_email_test')
        utils.email.send_email('to', 'subject', 'content')
        send_email_test.assert_called_with('to', 'subject', 'content', files=None, dryrun=False, cc=None, bcc=None)
        assert not mock_send_email.called


class EmailSmtpTest(unittest.TestCase):
    def setUp(self):
        configuration.set('smtp', 'SMTP_SSL', 'False')

    @mock.patch('airflow.utils.email.send_MIME_email')
    def test_send_smtp(self, mock_send_mime):
        attachment = tempfile.NamedTemporaryFile()
        attachment.write(b'attachment')
        attachment.seek(0)
        utils.email.send_email_smtp('to', 'subject', 'content', files=[attachment.name])
        assert mock_send_mime.called
        call_args = mock_send_mime.call_args[0]
        assert call_args[0] == configuration.get('smtp', 'SMTP_MAIL_FROM')
        assert call_args[1] == ['to']
        msg = call_args[2]
        assert msg['Subject'] == 'subject'
        assert msg['From'] == configuration.get('smtp', 'SMTP_MAIL_FROM')
        assert len(msg.get_payload()) == 2
        mimeapp = MIMEApplication('attachment')
        assert msg.get_payload()[-1].get_payload() == mimeapp.get_payload()

    @mock.patch('airflow.utils.email.send_MIME_email')
    def test_send_bcc_smtp(self, mock_send_mime):
        attachment = tempfile.NamedTemporaryFile()
        attachment.write(b'attachment')
        attachment.seek(0)
        utils.email.send_email_smtp('to', 'subject', 'content', files=[attachment.name], cc='cc', bcc='bcc')
        assert mock_send_mime.called
        call_args = mock_send_mime.call_args[0]
        assert call_args[0] == configuration.get('smtp', 'SMTP_MAIL_FROM')
        assert call_args[1] == ['to', 'cc', 'bcc']
        msg = call_args[2]
        assert msg['Subject'] == 'subject'
        assert msg['From'] == configuration.get('smtp', 'SMTP_MAIL_FROM')
        assert len(msg.get_payload()) == 2
        mimeapp = MIMEApplication('attachment')
        assert msg.get_payload()[-1].get_payload() == mimeapp.get_payload()


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
        assert mock_smtp.return_value.starttls.called
        mock_smtp.return_value.login.assert_called_with(
            configuration.get('smtp', 'SMTP_USER'),
            configuration.get('smtp', 'SMTP_PASSWORD'),
        )
        mock_smtp.return_value.sendmail.assert_called_with('from', 'to', msg.as_string())
        assert mock_smtp.return_value.quit.called

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_ssl(self, mock_smtp, mock_smtp_ssl):
        configuration.set('smtp', 'SMTP_SSL', 'True')
        mock_smtp.return_value = mock.Mock()
        mock_smtp_ssl.return_value = mock.Mock()
        utils.email.send_MIME_email('from', 'to', MIMEMultipart(), dryrun=False)
        assert not mock_smtp.called
        mock_smtp_ssl.assert_called_with(
            configuration.get('smtp', 'SMTP_HOST'),
            configuration.getint('smtp', 'SMTP_PORT'),
        )

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_dryrun(self, mock_smtp, mock_smtp_ssl):
        utils.email.send_MIME_email('from', 'to', MIMEMultipart(), dryrun=True)
        assert not mock_smtp.called
        assert not mock_smtp_ssl.called

if __name__ == '__main__':
    unittest.main()
