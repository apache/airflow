from __future__ import print_function

import doctest
import json
import logging
import os
import re
import unittest
import mock
from datetime import datetime, time, timedelta
from time import sleep

from dateutil.relativedelta import relativedelta

from airflow import configuration
from airflow.executors import SequentialExecutor, LocalExecutor
from airflow.models import Variable

configuration.test_mode()
from airflow import jobs, models, DAG, utils, operators, hooks, macros, settings
from airflow.hooks import BaseHook
from airflow.bin import cli
from airflow.www import app as application
from airflow.settings import Session
from airflow.utils import LoggingMixin, round_time
from lxml import html
from airflow.utils import AirflowException
from airflow.configuration import AirflowConfigException

NUM_EXAMPLE_DAGS = 7
DEV_NULL = '/dev/null'
DEFAULT_DATE = datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = 'unit_tests'
configuration.test_mode()

try:
    import cPickle as pickle
except ImportError:
    # Python 3
    import pickle

class FakeDatetime(datetime):
    "A fake replacement for datetime that can be mocked for testing."
    def __new__(cls, *args, **kwargs):
        return date.__new__(datetime, *args, **kwargs)

def reset(dag_id=TEST_DAG_ID):
    session = Session()
    tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
    tis.delete()
    session.commit()
    session.close()

reset()

class CoreTest(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
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
        dag = DAG(TEST_DAG_ID+'test_schedule_dag_no_previous_runs')
        dag.tasks = [models.BaseOperator(task_id="faketastic", owner='Also fake',
            start_date=datetime(2015, 1, 2, 0, 0))]
        dag_run = jobs.SchedulerJob(test_mode=True).schedule_dag(dag)
        assert dag_run is not None
        assert dag_run.dag_id == dag.dag_id
        assert dag_run.run_id is not None
        assert dag_run.run_id != ''
        assert dag_run.execution_date == datetime(2015, 1, 2, 0, 0), (
                'dag_run.execution_date did not match expectation: {0}'
                .format(dag_run.execution_date))
        assert dag_run.state == models.State.RUNNING
        assert dag_run.external_trigger == False

    def test_schedule_dag_fake_scheduled_previous(self):
        """
        Test scheduling a dag where there is a prior DagRun
        which has the same run_id as the next run should have
        """
        delta = timedelta(hours=1)
        dag = DAG(TEST_DAG_ID+'test_schedule_dag_fake_scheduled_previous',
                schedule_interval=delta,
                start_date=DEFAULT_DATE)
        dag.tasks = [models.BaseOperator(task_id="faketastic",
            owner='Also fake',
            start_date=DEFAULT_DATE)]
        scheduler = jobs.SchedulerJob(test_mode=True)
        trigger = models.DagRun(
                    dag_id=dag.dag_id,
                    run_id=models.DagRun.id_for_date(DEFAULT_DATE),
                    execution_date=DEFAULT_DATE,
                    state=utils.State.SUCCESS,
                    external_trigger=True)
        settings.Session().add(trigger)
        settings.Session().commit()
        dag_run = scheduler.schedule_dag(dag)
        assert dag_run is not None
        assert dag_run.dag_id == dag.dag_id
        assert dag_run.run_id is not None
        assert dag_run.run_id != ''
        assert dag_run.execution_date == DEFAULT_DATE+delta, (
                'dag_run.execution_date did not match expectation: {0}'
                .format(dag_run.execution_date))
        assert dag_run.state == models.State.RUNNING
        assert dag_run.external_trigger == False

    def test_schedule_dag_once(self):
        """
        Tests scheduling a dag scheduled for @once - should be scheduled the first time
        it is called, and not scheduled the second.
        """
        dag = DAG(TEST_DAG_ID+'test_schedule_dag_once')
        dag.schedule_interval = '@once'
        dag.tasks = [models.BaseOperator(task_id="faketastic", owner='Also fake',
            start_date=datetime(2015, 1, 2, 0, 0))]
        dag_run = jobs.SchedulerJob(test_mode=True).schedule_dag(dag)
        dag_run2 = jobs.SchedulerJob(test_mode=True).schedule_dag(dag)

        assert dag_run is not None
        assert dag_run2 is None

    def test_schedule_dag_start_end_dates(self):
        """
        Tests that an attempt to schedule a task after the Dag's end_date
        does not succeed.
        """
        delta = timedelta(hours=1)
        runs = 3
        start_date = DEFAULT_DATE
        end_date = start_date + (runs - 1) * delta
        dag = DAG(TEST_DAG_ID+'test_schedule_dag_start_end_dates',
                  start_date=start_date,
                  end_date=end_date,
                  schedule_interval=delta)

        # Create and schedule the dag runs
        dag_runs = []
        scheduler = jobs.SchedulerJob(test_mode=True)
        for i in range(runs):
            date = dag.start_date + i * delta
            task = models.BaseOperator(task_id='faketastic__%s' % i,
                                       owner='Also fake',
                                       start_date=date)
            dag.tasks.append(task)
            dag_runs.append(scheduler.schedule_dag(dag))

        additional_dag_run = scheduler.schedule_dag(dag)

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
        dag = DAG(TEST_DAG_ID+'test_schedule_dag_no_end_date_up_to_today_only',
                  start_date=start_date,
                  schedule_interval=delta)

        dag_runs = []
        scheduler = jobs.SchedulerJob(test_mode=True)
        for i in range(runs):
            # Create the DagRun
            date = dag.start_date + i * delta
            task = models.BaseOperator(task_id='faketastic__%s' % i,
                                       owner='Also fake',
                                       start_date=date)
            dag.tasks.append(task)

            # Schedule the DagRun
            dag_run = scheduler.schedule_dag(dag)
            dag_runs.append(dag_run)

            # Mark the DagRun as complete
            dag_run.state = utils.State.SUCCESS
            session.merge(dag_run)
            session.commit()

        # Attempt to schedule an additional dag run (for 2016-01-01)
        additional_dag_run = scheduler.schedule_dag(dag)

        for dag_run in dag_runs:
            assert dag_run is not None

        assert additional_dag_run is None

    def test_confirm_unittest_mod(self):
        assert configuration.get('core', 'unit_test_mode')

    def test_backfill_examples(self):
        self.dagbag = models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        dags = [
            dag for dag in self.dagbag.dags.values()
            if dag.dag_id in ('example_bash_operator',)]
        for dag in dags:
            dag.clear(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE)
        for dag in dags:
            job = jobs.BackfillJob(
                dag=dag,
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE)
            job.run()

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
        t = operators.TimeSensor(
            task_id='time_sensor_check',
            target_time=time(0),
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_check_operators(self):

        conn_id = "sqlite_default"

        captainHook = BaseHook.get_hook(conn_id=conn_id)
        captainHook.run("CREATE TABLE operator_test_table (a, b)")
        captainHook.run("insert into operator_test_table values (1,2)")

        t = operators.CheckOperator(
            task_id='check',
            sql="select count(*) from operator_test_table" ,
            conn_id=conn_id,
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        t = operators.ValueCheckOperator(
            task_id='value_check',
            pass_value=95,
            tolerance=0.1,
            conn_id=conn_id,
            sql="SELECT 100",
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        captainHook.run("drop table operator_test_table")

    def test_clear_api(self):
        task = self.dag_bash.tasks[0]
        task.clear(
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
            upstream=True, downstream=True)
        ti = models.TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.are_dependents_done()

    def test_bash_operator(self):
        t = operators.BashOperator(
            task_id='time_sensor_check',
            bash_command="echo success",
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_trigger_dagrun(self):
        def trigga(context, obj):
            if True:
                return obj
        t = operators.TriggerDagRunOperator(
            task_id='test_trigger_dagrun',
            trigger_dag_id='example_bash_operator',
            python_callable=trigga,
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_dryrun(self):
        t = operators.BashOperator(
            task_id='time_sensor_check',
            bash_command="echo success",
            dag=self.dag)
        t.dry_run()

    def test_sqlite(self):
        t = operators.SqliteOperator(
            task_id='time_sqlite',
            sql="CREATE TABLE IF NOT EXISTS unitest (dummy VARCHAR(20))",
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_timedelta_sensor(self):
        t = operators.TimeDeltaSensor(
            task_id='timedelta_sensor_check',
            delta=timedelta(seconds=2),
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_external_task_sensor(self):
        t = operators.ExternalTaskSensor(
            task_id='test_external_task_sensor_check',
            external_dag_id=TEST_DAG_ID,
            external_task_id='time_sensor_check',
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_external_task_sensor_delta(self):
        t = operators.ExternalTaskSensor(
            task_id='test_external_task_sensor_check_delta',
            external_dag_id=TEST_DAG_ID,
            external_task_id='time_sensor_check',
            execution_delta=timedelta(0),
            allowed_states=['success'],
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_timeout(self):
        t = operators.PythonOperator(
            task_id='test_timeout',
            execution_timeout=timedelta(seconds=1),
            python_callable=lambda: sleep(5),
            dag=self.dag)
        self.assertRaises(
            utils.AirflowTaskTimeout,
            t.run,
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_python_op(self):
        def test_py_op(templates_dict, ds, **kwargs):
            if not templates_dict['ds'] == ds:
                raise Exception("failure")
        t = operators.PythonOperator(
            task_id='test_py_op',
            provide_context=True,
            python_callable=test_py_op,
            templates_dict={'ds': "{{ ds }}"},
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_complex_template(self):
        class OperatorSubclass(operators.BaseOperator):
            template_fields = ['some_templated_field']
            def __init__(self, some_templated_field, *args, **kwargs):
                super(OperatorSubclass, self).__init__(*args, **kwargs)
                self.some_templated_field = some_templated_field
            def execute(*args, **kwargs):
                pass
        def test_some_templated_field_template_render(context):
            self.assertEqual(context['ti'].task.some_templated_field['bar'][1], context['ds'])
        t = OperatorSubclass(
            task_id='test_complex_template',
            provide_context=True,
            some_templated_field={
                'foo':'123',
                'bar':['baz', '{{ ds }}']
            },
            on_success_callback=test_some_templated_field_template_render,
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_import_examples(self):
        self.assertEqual(len(self.dagbag.dags), NUM_EXAMPLE_DAGS)

    def test_local_task_job(self):
        TI = models.TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        job = jobs.LocalTaskJob(task_instance=ti, force=True)
        job.run()

    def test_scheduler_job(self):
        job = jobs.SchedulerJob(dag_id='example_bash_operator', test_mode=True)
        job.run()

    def test_raw_job(self):
        TI = models.TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        ti.dag = self.dag_bash
        ti.run(force=True)

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

    def test_config_works_without_original_but_has_fallback(self):
        # initial assumption
        assert configuration.has_option("core", "SQL_ALCHEMY_CONN")
        assert not configuration.has_option("core", "SQL_ALCHEMY_CONN_CMD")

        SQL_ALCHEMY_CONN = configuration.get('core', 'SQL_ALCHEMY_CONN')
        BROKER_URL = configuration.get('celery', 'BROKER_URL')
        CELERY_RESULT_BACKEND = configuration.get('celery',
                                                  'CELERY_RESULT_BACKEND')

        # testing condition
        configuration.set("core", "SQL_ALCHEMY_CONN_CMD",
                          "printf sqlite:///random_string/unittests.db")
        configuration.set("celery", "BROKER_URL_CMD",
                          "printf sqlite:///BROKER_URL_CMD/unittests.db")
        configuration.set("celery", "CELERY_RESULT_BACKEND_CMD",
                          "printf sqlite:///CELERY_RESULT/unittests.db")

        assert configuration.has_option("core", "SQL_ALCHEMY_CONN_CMD")

        configuration.remove_option("core", "SQL_ALCHEMY_CONN")
        configuration.remove_option("celery", "BROKER_URL")
        configuration.remove_option("celery", "CELERY_RESULT_BACKEND")

        FALLBACK_SQL_ALCHEMY = configuration.get("core",
                                                 "SQL_ALCHEMY_CONN")
        FALLBACK_BROKER_URL = configuration.get("celery",
                                                "BROKER_URL")
        FALLBACK_CELERY = configuration.get("celery",
                                            "CELERY_RESULT_BACKEND")

        assert FALLBACK_SQL_ALCHEMY == b"sqlite:///random_string/unittests.db"
        assert FALLBACK_BROKER_URL == b"sqlite:///BROKER_URL_CMD/unittests.db"
        assert FALLBACK_CELERY == b"sqlite:///CELERY_RESULT/unittests.db"

        # restore the conf back to the original state
        configuration.set("core", "SQL_ALCHEMY_CONN", SQL_ALCHEMY_CONN)
        configuration.set("celery", "BROKER_URL", BROKER_URL)
        configuration.set("celery", "CELERY_RESULT_BACKEND",
                          CELERY_RESULT_BACKEND)

        assert configuration.has_option("core", "SQL_ALCHEMY_CONN")

        configuration.remove_option("core", "SQL_ALCHEMY_CONN_CMD")
        configuration.remove_option("celery", "BROKER_URL_CMD")
        configuration.remove_option("celery", "CELERY_RESULT_BACKEND_CMD")

        assert not configuration.has_option("core", "SQL_ALCHEMY_CONN_CMD")

        NEW_SQL_ALCHEMY_CONN = configuration.get("core","SQL_ALCHEMY_CONN")

        assert NEW_SQL_ALCHEMY_CONN == SQL_ALCHEMY_CONN

    def test_config_use_original_when_original_and_fallback_are_present(self):
        assert configuration.has_option("core", "SQL_ALCHEMY_CONN")
        assert not configuration.has_option("core", "SQL_ALCHEMY_CONN_CMD")

        SQL_ALCHEMY_CONN = configuration.get('core', 'SQL_ALCHEMY_CONN')

        configuration.set("core", "SQL_ALCHEMY_CONN_CMD",
                          "printf sqlite:///random_string/unittests.db")

        FALLBACK_SQL_ALCHEMY_CONN = configuration.get(
            "core",
            "SQL_ALCHEMY_CONN"
        )

        assert FALLBACK_SQL_ALCHEMY_CONN == SQL_ALCHEMY_CONN

        # restore the conf back to the original state
        configuration.remove_option("core", "SQL_ALCHEMY_CONN_CMD")

    def test_config_throw_error_when_original_and_fallback_is_absent(self):
        assert configuration.has_option("core", "SQL_ALCHEMY_CONN")
        assert not configuration.has_option("core", "SQL_ALCHEMY_CONN_CMD")

        SQL_ALCHEMY_CONN = configuration.get("core", "SQL_ALCHEMY_CONN")
        configuration.remove_option("core", "SQL_ALCHEMY_CONN")

        with self.assertRaises(AirflowConfigException) as cm:
            configuration.get("core", "SQL_ALCHEMY_CONN")

        exception = str(cm.exception)
        message = "section/key [core/sql_alchemy_conn] not found in config"
        assert exception == message

        # restore the conf back to the original state
        configuration.set("core", "SQL_ALCHEMY_CONN", SQL_ALCHEMY_CONN)
        assert configuration.has_option("core", "SQL_ALCHEMY_CONN")

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
            operators.DummyOperator(
            task_id='test_bad_trigger',
            trigger_rule="non_existant",
            dag=self.dag)

class CliTests(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        app = application.create_app()
        app.config['TESTING'] = True
        self.parser = cli.get_parser()
        self.dagbag = models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)

    def test_cli_list_dags(self):
        args = self.parser.parse_args(['list_dags'])
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

    def test_cli_test(self):
        cli.test(self.parser.parse_args([
            'test', 'example_bash_operator', 'runme_0',
            DEFAULT_DATE.isoformat()]))
        cli.test(self.parser.parse_args([
            'test', 'example_bash_operator', 'runme_0', '--dry_run',
            DEFAULT_DATE.isoformat()]))

    def test_cli_run(self):
        cli.run(self.parser.parse_args([
            'run', 'example_bash_operator', 'runme_0', '-l',
            DEFAULT_DATE.isoformat()]))

    def test_task_state(self):
        cli.task_state(self.parser.parse_args([
            'task_state', 'example_bash_operator', 'runme_0',
            DEFAULT_DATE.isoformat()]))

    def test_pause(self):
        args = self.parser.parse_args([
            'pause', 'example_bash_operator'])
        cli.pause(args)
        assert self.dagbag.dags['example_bash_operator'].is_paused in [True, 1]

        args = self.parser.parse_args([
            'unpause', 'example_bash_operator'])
        cli.unpause(args)
        assert self.dagbag.dags['example_bash_operator'].is_paused in [False, 0]

    def test_backfill(self):
        cli.backfill(self.parser.parse_args([
            'backfill', 'example_bash_operator',
            '-s', DEFAULT_DATE.isoformat()]))

        cli.backfill(self.parser.parse_args([
            'backfill', 'example_bash_operator','-t', 'runme_0', '--dry_run',
            '-s', DEFAULT_DATE.isoformat()]))

        cli.backfill(self.parser.parse_args([
            'backfill', 'example_bash_operator', '--dry_run',
            '-s', DEFAULT_DATE.isoformat()]))

        cli.backfill(self.parser.parse_args([
            'backfill', 'example_bash_operator', '-l',
            '-s', DEFAULT_DATE.isoformat()]))

    def test_process_subdir_path_with_placeholder(self):
        assert cli.process_subdir('DAGS_FOLDER/abc') == os.path.join(configuration.get_dags_folder(), 'abc')


class WebUiTests(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        configuration.conf.set("webserver", "authenticate", "False")
        app = application.create_app()
        app.config['TESTING'] = True
        self.app = app.test_client()

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
        # new Chartkick.LineChart(document.getElementById("chart-0"), [{"data": [["2015-11-17T16:53:08.652950", 9.866944444444444e-06]], "name": "run_after_loop"}, {"data": [["2015-11-17T16:53:08.652950", 0.0002858047222222222], ["2015-11-17T16:56:09.698921", 0.00028737944444444445]], "name": "runme_0"}, {"data": [["2015-11-17T16:53:08.652950", 0.0002863941666666666], ["2015-11-17T16:56:09.698921", 0.00029015249999999996]], "name": "runme_1"}, {"data": [["2015-11-17T16:53:08.652950", 0.0002860847222222222], ["2015-11-17T16:56:09.698921", 0.00029001583333333335]], "name": "runme_2"}, {"data": [["2015-11-17T16:53:08.652950", 8.166944444444444e-06], ["2015-11-17T16:56:09.698921", 1.2806944444444445e-05]], "name": "also_run_this"}], {"library": {"yAxis": {"title": {"text": "hours"}}}, "height": "700px"});

        chartkick_regexp = 'new Chartkick.LineChart\(document.getElementById\("chart-\d+"\),(.+)\)\;'
        response = self.app.get(
            '/admin/airflow/duration?days=30&dag_id=example_bash_operator')
        assert "example_bash_operator" in response.data.decode('utf-8')

        chartkick_matched = re.search(chartkick_regexp,
                                      response.data.decode('utf-8'))
        assert chartkick_matched is not None, "chartkick_matched was none. Expected regex is: %s\nResponse was: %s" % (
                chartkick_regexp,
                response.data.decode('utf-8'))

        # test that parameters to LineChart are well-formed json
        try:
            json.loads('[%s]' % chartkick_matched.group(1))
        except e:
            assert False, "Exception while json parsing LineChart parameters: %s" % e

        response = self.app.get(
            '/admin/airflow/landing_times?'
            'days=30&dag_id=example_bash_operator')
        assert "example_bash_operator" in response.data.decode('utf-8')
        chartkick_matched = re.search(chartkick_regexp,
                                      response.data.decode('utf-8'))
        assert chartkick_matched is not None
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
            "dag_id=example_bash_operator&force=true&deps=true&"
            "execution_date={}&origin=/admin".format(DEFAULT_DATE_DS))
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
            models.Chart).filter(models.Chart.label==chart_label).first()
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

    def tearDown(self):
        pass


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
        configuration.test_mode()
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

    def tearDown(self):
        configuration.test_mode()
        session = Session()
        session.query(models.User).delete()
        session.commit()
        session.close()
        configuration.conf.set("webserver", "authenticate", "False")


if 'MySqlOperator' in dir(operators):
    # Only testing if the operator is installed
    class MySqlTest(unittest.TestCase):

        def setUp(self):
            configuration.test_mode()
            args = {
                'owner': 'airflow',
                'mysql_conn_id': 'airflow_db',
                'start_date': DEFAULT_DATE
            }
            dag = DAG(TEST_DAG_ID, default_args=args)
            self.dag = dag

        def mysql_operator_test(self):
            sql = """
            CREATE TABLE IF NOT EXISTS test_airflow (
                dummy VARCHAR(50)
            );
            """
            t = operators.MySqlOperator(
                task_id='basic_mysql',
                sql=sql,
                mysql_conn_id='airflow_db',
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def mysql_operator_test_multi(self):
            sql = [
                "TRUNCATE TABLE test_airflow",
                "INSERT INTO test_airflow VALUES ('X')",
            ]
            t = operators.MySqlOperator(
                task_id='mysql_operator_test_multi',
                mysql_conn_id='airflow_db',
                sql=sql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_mysql_to_mysql(self):
            sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES LIMIT 100;"
            t = operators.GenericTransfer(
                task_id='test_m2m',
                preoperator=[
                    "DROP TABLE IF EXISTS test_mysql_to_mysql",
                    "CREATE TABLE IF NOT EXISTS "
                        "test_mysql_to_mysql LIKE INFORMATION_SCHEMA.TABLES"
                ],
                source_conn_id='airflow_db',
                destination_conn_id='airflow_db',
                destination_table="test_mysql_to_mysql",
                sql=sql,
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_sql_sensor(self):
            t = operators.SqlSensor(
                task_id='sql_sensor_check',
                conn_id='mysql_default',
                sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)


if 'PostgresOperator' in dir(operators):
    # Only testing if the operator is installed
    class PostgresTest(unittest.TestCase):

        def setUp(self):
            configuration.test_mode()
            args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
            dag = DAG(TEST_DAG_ID, default_args=args)
            self.dag = dag

        def postgres_operator_test(self):
            sql = """
            CREATE TABLE IF NOT EXISTS test_airflow (
                dummy VARCHAR(50)
            );
            """
            t = operators.PostgresOperator(
                task_id='basic_postgres', sql=sql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

            autocommitTask = operators.PostgresOperator(
                task_id='basic_postgres_with_autocommit',
                sql=sql,
                dag=self.dag,
                autocommit=True)
            autocommitTask.run(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE,
                force=True)


class HttpOpSensorTest(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE_ISO}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def test_get(self):
        t = operators.SimpleHttpOperator(
            task_id='get_op',
            method='GET',
            endpoint='/search',
            data={"client": "ubuntu", "q": "airflow"},
            headers={},
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_get_response_check(self):
        t = operators.SimpleHttpOperator(
            task_id='get_op',
            method='GET',
            endpoint='/search',
            data={"client": "ubuntu", "q": "airflow"},
            response_check=lambda response: ("airbnb/airflow" in response.text),
            headers={},
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_sensor(self):
        sensor = operators.HttpSensor(
            task_id='http_sensor_check',
            conn_id='http_default',
            endpoint='/search',
            params={"client": "ubuntu", "q": "airflow"},
            headers={},
            response_check=lambda response: ("airbnb/airflow" in response.text),
            poke_interval=5,
            timeout=15,
            dag=self.dag)
        sensor.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)


class ConnectionTest(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        utils.initdb()
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
        c = hooks.SqliteHook.get_connection(conn_id='test_uri')
        assert c.host == 'ec2.compute.com'
        assert c.schema == 'the_database'
        assert c.login == 'username'
        assert c.password == 'password'
        assert c.port == 5432

    def test_using_unix_socket_env_var(self):
        c = hooks.SqliteHook.get_connection(conn_id='test_uri_no_creds')
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
        c = hooks.SqliteHook.get_connection(conn_id='airflow_db')
        assert c.host != 'ec2.compute.com'

        os.environ['AIRFLOW_CONN_AIRFLOW_DB'] = \
            'postgres://username:password@ec2.compute.com:5432/the_database'
        c = hooks.SqliteHook.get_connection(conn_id='airflow_db')
        assert c.host == 'ec2.compute.com'
        assert c.schema == 'the_database'
        assert c.login == 'username'
        assert c.password == 'password'
        assert c.port == 5432
        del os.environ['AIRFLOW_CONN_AIRFLOW_DB']


class WebHDFSHookTest(unittest.TestCase):
    def setUp(self):
        configuration.test_mode()
    
    def test_simple_init(self):
        from airflow.hooks.webhdfs_hook import WebHDFSHook
        c = WebHDFSHook()
        assert c.proxy_user == None

    def test_init_proxy_user(self):
        from airflow.hooks.webhdfs_hook import WebHDFSHook
        c = WebHDFSHook(proxy_user='someone')
        assert c.proxy_user == 'someone'


@unittest.skipUnless("S3Hook" in dir(hooks),
                     "Skipping test because S3Hook is not installed")
class S3HookTest(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        self.s3_test_url = "s3://test/this/is/not/a-real-key.txt"

    def test_parse_s3_url(self):
        parsed = hooks.S3Hook.parse_s3_url(self.s3_test_url)
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
        configuration.test_mode()
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
            print("Receiving...",)
            response = s.recv(5)
            self.assertEqual(response, b"hello")
            print("Closing connection")
            s.close()
            print("Waiting for listener...")
            output, _ = self.handle.communicate()
            self.assertEqual(self.handle.returncode, 0)
            print("Closing tunnel")


if 'AIRFLOW_RUNALL_TESTS' in os.environ:


    class TransferTests(unittest.TestCase):

        def setUp(self):
            configuration.test_mode()
            args = {'owner': 'airflow', 'start_date': DEFAULT_DATE_ISO}
            dag = DAG(TEST_DAG_ID, default_args=args)
            self.dag = dag

        def test_clear(self):
            self.dag.clear(start_date=DEFAULT_DATE, end_date=datetime.now())

        def test_mysql_to_hive(self):
            sql = "SELECT * FROM task_instance LIMIT 1000;"
            t = operators.MySqlToHiveTransfer(
                task_id='test_m2h',
                mysql_conn_id='airflow_db',
                sql=sql,
                hive_table='airflow.test_mysql_to_hive',
                recreate=True,
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_mysql_to_hive_partition(self):
            sql = "SELECT * FROM task_instance LIMIT 1000;"
            t = operators.MySqlToHiveTransfer(
                task_id='test_m2h',
                mysql_conn_id='airflow_db',
                sql=sql,
                hive_table='airflow.test_mysql_to_hive_part',
                partition={'ds': DEFAULT_DATE_DS},
                recreate=False,
                create=True,
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    class HivePrestoTest(unittest.TestCase):

        def setUp(self):
            configuration.test_mode()
            args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
            dag = DAG(TEST_DAG_ID, default_args=args)
            self.dag = dag
            self.hql = """
            USE airflow;
            DROP TABLE IF EXISTS static_babynames_partitioned;
            CREATE TABLE IF NOT EXISTS static_babynames_partitioned (
                state string,
                year string,
                name string,
                gender string,
                num int)
            PARTITIONED BY (ds string);
            INSERT OVERWRITE TABLE static_babynames_partitioned
                PARTITION(ds='{{ ds }}')
            SELECT state, year, name, gender, num FROM static_babynames;
            """

        def test_hive(self):
            t = operators.HiveOperator(
                task_id='basic_hql', hql=self.hql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive_dryrun(self):
            t = operators.HiveOperator(
                task_id='basic_hql', hql=self.hql, dag=self.dag)
            t.dry_run()

        def test_beeline(self):
            t = operators.HiveOperator(
                task_id='beeline_hql', hive_cli_conn_id='beeline_default',
                hql=self.hql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_presto(self):
            sql = """
            SELECT count(1) FROM airflow.static_babynames_partitioned;
            """
            t = operators.PrestoCheckOperator(
                task_id='presto_check', sql=sql, dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_presto_to_mysql(self):
        t = operators.PrestoToMySqlTransfer(
            task_id='presto_to_mysql_check',
            sql="""
            SELECT name, count(*) as ccount
            FROM airflow.static_babynames
            GROUP BY name
            """,
            mysql_table='test_static_babynames',
            mysql_preoperator='TRUNCATE TABLE test_static_babynames;',
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

    def test_presto_to_mysql(self):
        t = operators.PrestoToMySqlTransfer(
            task_id='presto_to_mysql_check',
            sql="""
            SELECT name, count(*) as ccount
            FROM airflow.static_babynames
            GROUP BY name
            """,
            mysql_table='test_static_babynames',
            mysql_preoperator='TRUNCATE TABLE test_static_babynames;',
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)


        def test_hdfs_sensor(self):
            t = operators.HdfsSensor(
                task_id='hdfs_sensor_check',
                filepath='hdfs://user/hive/warehouse/airflow.db/static_babynames',
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_webhdfs_sensor(self):
            t = operators.WebHdfsSensor(
                task_id='webhdfs_sensor_check',
                filepath='hdfs://user/hive/warehouse/airflow.db/static_babynames',
                timeout=120,
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_sql_sensor(self):
            t = operators.SqlSensor(
                task_id='hdfs_sensor_check',
                conn_id='presto_default',
                sql="SELECT 'x' FROM airflow.static_babynames LIMIT 1;",
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive_stats(self):
            t = operators.HiveStatsCollectionOperator(
                task_id='hive_stats_check',
                table="airflow.static_babynames_partitioned",
                partition={'ds': DEFAULT_DATE_DS},
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive_partition_sensor(self):
            t = operators.HivePartitionSensor(
                task_id='hive_partition_check',
                table='airflow.static_babynames_partitioned',
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive_metastore_sql_sensor(self):
            t = operators.MetastorePartitionSensor(
                task_id='hive_partition_check',
                table='airflow.static_babynames_partitioned',
                partition_name='ds={}'.format(DEFAULT_DATE_DS),
                dag=self.dag)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive2samba(self):
            if 'Hive2SambaOperator' in dir(operators):
                t = operators.Hive2SambaOperator(
                    task_id='hive2samba_check',
                    samba_conn_id='tableau_samba',
                    hql="SELECT * FROM airflow.static_babynames LIMIT 10000",
                    destination_filepath='test_airflow.csv',
                    dag=self.dag)
                t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)

        def test_hive_to_mysql(self):
            t = operators.HiveToMySqlTransfer(
                mysql_conn_id='airflow_db',
                task_id='hive_to_mysql_check',
                create=True,
                sql="""
                SELECT name
                FROM airflow.static_babynames
                LIMIT 100
                """,
                mysql_table='test_static_babynames',
                mysql_preoperator=[
                    'DROP TABLE IF EXISTS test_static_babynames;',
                    'CREATE TABLE test_static_babynames (name VARCHAR(500))',
                ],
                dag=self.dag)
            t.clear(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
            t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, force=True)


if __name__ == '__main__':
    unittest.main()
