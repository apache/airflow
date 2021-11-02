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

import logging
import os
import signal
from datetime import timedelta
from time import sleep
from unittest.mock import MagicMock

import pytest

from airflow import settings
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.hooks.base import BaseHook
from airflow.models import DagBag, TaskFail, TaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.check_operator import CheckOperator, ValueCheckOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_db_task_fail

DEV_NULL = '/dev/null'
DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_tests'


class TestCore:
    @staticmethod
    def clean_db():
        clear_db_task_fail()
        clear_db_dags()
        clear_db_runs()

    default_scheduler_args = {"num_runs": 1}

    def setup_method(self):
        self.clean_db()
        self.dagbag = DagBag(dag_folder=DEV_NULL, include_examples=True, read_dags_from_db=False)
        self.dag_bash = self.dagbag.dags['example_bash_operator']
        self.runme_0 = self.dag_bash.get_task('runme_0')
        self.run_after_loop = self.dag_bash.get_task('run_after_loop')
        self.run_this_last = self.dag_bash.get_task('run_this_last')

    def teardown_method(self):
        self.clean_db()

    def test_check_operators(self, dag_maker):

        conn_id = "sqlite_default"

        captain_hook = BaseHook.get_hook(conn_id=conn_id)  # quite funny :D
        captain_hook.run("CREATE TABLE operator_test_table (a, b)")
        captain_hook.run("insert into operator_test_table values (1,2)")
        with dag_maker(TEST_DAG_ID) as dag:
            op = CheckOperator(
                task_id='check', sql="select count(*) from operator_test_table", conn_id=conn_id
            )
        dag_maker.create_dagrun(run_type=DagRunType.MANUAL)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        op = ValueCheckOperator(
            task_id='value_check',
            pass_value=95,
            tolerance=0.1,
            conn_id=conn_id,
            sql="SELECT 100",
            dag=dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        captain_hook.run("drop table operator_test_table")

    def test_clear_api(self, session):
        task = self.dag_bash.tasks[0]

        dr = self.dag_bash.create_dagrun(
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            execution_date=days_ago(1),
            session=session,
        )
        task.clear(start_date=dr.execution_date, end_date=dr.execution_date, upstream=True, downstream=True)
        ti = dr.get_task_instance(task.task_id, session=session)
        ti.task = task
        ti.are_dependents_done()

    def test_illegal_args(self, dag_maker):
        """
        Tests that Operators reject illegal arguments
        """
        msg = 'Invalid arguments were passed to BashOperator (task_id: test_illegal_args).'
        with conf_vars({('operators', 'allow_illegal_arguments'): 'True'}):
            with pytest.warns(PendingDeprecationWarning) as warnings:
                with dag_maker():
                    BashOperator(
                        task_id='test_illegal_args',
                        bash_command='echo success',
                        illegal_argument_1234='hello?',
                    )
                dag_maker.create_dagrun()
                assert any(msg in str(w) for w in warnings)

    def test_illegal_args_forbidden(self, dag_maker):
        """
        Tests that operators raise exceptions on illegal arguments when
        illegal arguments are not allowed.
        """
        with pytest.raises(AirflowException) as ctx:
            with dag_maker():
                BashOperator(
                    task_id='test_illegal_args',
                    bash_command='echo success',
                    illegal_argument_1234='hello?',
                )
            dag_maker.create_dagrun()
        assert 'Invalid arguments were passed to BashOperator (task_id: test_illegal_args).' in str(ctx.value)

    def test_bash_operator(self, dag_maker):
        with dag_maker():
            op = BashOperator(task_id='test_bash_operator', bash_command="echo success")
        dag_maker.create_dagrun(run_type=DagRunType.MANUAL)

        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_bash_operator_multi_byte_output(self, dag_maker):
        with dag_maker():
            op = BashOperator(
                task_id='test_multi_byte_bash_operator',
                bash_command="echo \u2600",
                output_encoding='utf-8',
            )
        dag_maker.create_dagrun(run_type=DagRunType.MANUAL)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_bash_operator_kill(self, dag_maker):
        import psutil

        sleep_time = "100%d" % os.getpid()
        with dag_maker():
            op = BashOperator(
                task_id='test_bash_operator_kill',
                execution_timeout=timedelta(seconds=1),
                bash_command=f"/bin/bash -c 'sleep {sleep_time}'",
            )
        dag_maker.create_dagrun()
        with pytest.raises(AirflowTaskTimeout):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        sleep(2)
        pid = -1
        for proc in psutil.process_iter():
            if proc.cmdline() == ['sleep', sleep_time]:
                pid = proc.pid
        if pid != -1:
            os.kill(pid, signal.SIGTERM)
            self.fail("BashOperator's subprocess still running after stopping on timeout!")

    def test_on_failure_callback(self, dag_maker):
        mock_failure_callback = MagicMock()

        with dag_maker():
            op = BashOperator(
                task_id='check_on_failure_callback',
                bash_command="exit 1",
                on_failure_callback=mock_failure_callback,
            )
        dag_maker.create_dagrun()
        with pytest.raises(AirflowException):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        mock_failure_callback.assert_called_once()

    def test_dryrun(self, dag_maker):
        with dag_maker():
            op = BashOperator(task_id='test_dryrun', bash_command="echo success")
        dag_maker.create_dagrun()
        op.dry_run()

    def test_sqlite(self, dag_maker):
        import airflow.providers.sqlite.operators.sqlite

        with dag_maker():
            op = airflow.providers.sqlite.operators.sqlite.SqliteOperator(
                task_id='time_sqlite',
                sql="CREATE TABLE IF NOT EXISTS unitest (dummy VARCHAR(20))",
            )
        dag_maker.create_dagrun(run_type=DagRunType.MANUAL)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_timeout(self, dag_maker):
        with dag_maker():
            op = PythonOperator(
                task_id='test_timeout',
                execution_timeout=timedelta(seconds=1),
                python_callable=lambda: sleep(5),
            )
        dag_maker.create_dagrun()
        with pytest.raises(AirflowTaskTimeout):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_python_op(self, dag_maker):
        def test_py_op(templates_dict, ds, **kwargs):
            if not templates_dict['ds'] == ds:
                raise Exception("failure")

        with dag_maker():
            op = PythonOperator(
                task_id='test_py_op', python_callable=test_py_op, templates_dict={'ds': "{{ ds }}"}
            )
        dag_maker.create_dagrun(run_type=DagRunType.MANUAL)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_task_get_template(self, session):
        dr = self.dag_bash.create_dagrun(
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE + timedelta(days=1)),
            session=session,
        )
        ti = TaskInstance(task=self.runme_0, run_id=dr.run_id)
        ti.dag = self.dag_bash
        ti.run(ignore_ti_state=True)
        context = ti.get_template_context()

        # DEFAULT DATE is 2015-01-01
        assert context['ds'] == '2015-01-01'
        assert context['ds_nodash'] == '20150101'

        # next_ds is 2015-01-02 as the dag schedule is daily.
        assert context['next_ds'] == '2015-01-02'
        assert context['next_ds_nodash'] == '20150102'

        assert context['ts'] == '2015-01-01T00:00:00+00:00'
        assert context['ts_nodash'] == '20150101T000000'
        assert context['ts_nodash_with_tz'] == '20150101T000000+0000'

        assert context['data_interval_start'].isoformat() == '2015-01-01T00:00:00+00:00'
        assert context['data_interval_end'].isoformat() == '2015-01-02T00:00:00+00:00'

        # Test deprecated fields.
        expected_deprecated_fields = [
            ("prev_ds", "2014-12-31"),
            ("prev_ds_nodash", "20141231"),
            ("yesterday_ds", "2014-12-31"),
            ("yesterday_ds_nodash", "20141231"),
            ("tomorrow_ds", "2015-01-02"),
            ("tomorrow_ds_nodash", "20150102"),
        ]
        for key, expected_value in expected_deprecated_fields:
            message = (
                f"Accessing {key!r} from the template is deprecated and "
                f"will be removed in a future version."
            )
            with pytest.deprecated_call() as recorder:
                value = str(context[key])  # Simulate template evaluation to trigger warning.
            assert value == expected_value
            assert [str(m.message) for m in recorder] == [message]

    def test_bad_trigger_rule(self, dag_maker):
        with pytest.raises(AirflowException):
            with dag_maker():
                DummyOperator(task_id='test_bad_trigger', trigger_rule="non_existent")
            dag_maker.create_dagrun()

    def test_task_fail_duration(self, dag_maker):
        """If a task fails, the duration should be recorded in TaskFail"""
        with dag_maker() as dag:
            op1 = BashOperator(task_id='pass_sleepy', bash_command='sleep 3')
            op2 = BashOperator(
                task_id='fail_sleepy',
                bash_command='sleep 5',
                execution_timeout=timedelta(seconds=3),
                retry_delay=timedelta(seconds=0),
            )
        dag_maker.create_dagrun()
        session = settings.Session()
        try:
            op1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        except Exception:
            pass
        try:
            op2.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        except Exception:
            pass
        op1_fails = (
            session.query(TaskFail)
            .filter_by(task_id='pass_sleepy', dag_id=dag.dag_id, execution_date=DEFAULT_DATE)
            .all()
        )
        op2_fails = (
            session.query(TaskFail)
            .filter_by(task_id='fail_sleepy', dag_id=dag.dag_id, execution_date=DEFAULT_DATE)
            .all()
        )

        assert 0 == len(op1_fails)
        assert 1 == len(op2_fails)
        assert sum(f.duration for f in op2_fails) >= 3

    def test_externally_triggered_dagrun(self, dag_maker):
        TI = TaskInstance

        # Create the dagrun between two "scheduled" execution dates of the DAG
        execution_date = DEFAULT_DATE + timedelta(days=2)
        execution_ds = execution_date.strftime('%Y-%m-%d')
        execution_ds_nodash = execution_ds.replace('-', '')

        with dag_maker(schedule_interval=timedelta(weeks=1)):
            task = DummyOperator(task_id='test_externally_triggered_dag_context')
        dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=execution_date,
            external_trigger=True,
        )
        task.run(start_date=execution_date, end_date=execution_date)

        ti = TI(task=task, execution_date=execution_date)
        context = ti.get_template_context()

        # next_ds should be the execution date for manually triggered runs
        assert context['next_ds'] == execution_ds
        assert context['next_ds_nodash'] == execution_ds_nodash

    def test_dag_params_and_task_params(self, dag_maker):
        # This test case guards how params of DAG and Operator work together.
        # - If any key exists in either DAG's or Operator's params,
        #   it is guaranteed to be available eventually.
        # - If any key exists in both DAG's params and Operator's params,
        #   the latter has precedence.
        TI = TaskInstance

        with dag_maker(
            schedule_interval=timedelta(weeks=1),
            params={'key_1': 'value_1', 'key_2': 'value_2_old'},
        ):
            task1 = DummyOperator(
                task_id='task1',
                params={'key_2': 'value_2_new', 'key_3': 'value_3'},
            )
            task2 = DummyOperator(task_id='task2')
        dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            external_trigger=True,
        )
        task1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        task2.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        ti1 = TI(task=task1, execution_date=DEFAULT_DATE)
        ti2 = TI(task=task2, execution_date=DEFAULT_DATE)
        context1 = ti1.get_template_context()
        context2 = ti2.get_template_context()

        assert context1['params'] == {'key_1': 'value_1', 'key_2': 'value_2_new', 'key_3': 'value_3'}
        assert context2['params'] == {'key_1': 'value_1', 'key_2': 'value_2_old'}


def test_operator_retries_invalid(dag_maker):
    with pytest.raises(AirflowException) as ctx:
        with dag_maker():
            BashOperator(
                task_id='test_illegal_args',
                bash_command='echo success',
                retries='foo',
            )
        dag_maker.create_dagrun()
    assert str(ctx.value) == "'retries' type must be int, not str"


def test_operator_retries_coerce(caplog, dag_maker):
    with caplog.at_level(logging.WARNING):
        with dag_maker():
            BashOperator(
                task_id='test_illegal_args',
                bash_command='echo success',
                retries='1',
            )
        dag_maker.create_dagrun()
    assert caplog.record_tuples == [
        (
            "airflow.operators.bash.BashOperator",
            logging.WARNING,
            "Implicitly converting 'retries' for <Task(BashOperator): test_illegal_args> from '1' to int",
        ),
    ]


@pytest.mark.parametrize("retries", [None, 5])
def test_operator_retries(caplog, dag_maker, retries):
    with caplog.at_level(logging.WARNING):
        with dag_maker(TEST_DAG_ID + str(retries)):
            BashOperator(
                task_id='test_illegal_args',
                bash_command='echo success',
                retries=retries,
            )
        dag_maker.create_dagrun()
    assert caplog.records == []
