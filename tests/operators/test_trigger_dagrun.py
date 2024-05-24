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
from __future__ import annotations

import tempfile
from datetime import datetime
from unittest import mock

import pendulum
import pytest

from airflow.exceptions import AirflowException, DagRunAlreadyExists, TaskDeferred
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.log import Log
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.triggers.external_task import DagStateTrigger
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.types import DagRunType

pytestmark = pytest.mark.db_test

DEFAULT_DATE = datetime(2019, 1, 1, tzinfo=timezone.utc)
TEST_DAG_ID = "testdag"
TRIGGERED_DAG_ID = "triggerdag"
DAG_SCRIPT = f"""\
from datetime import datetime
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

dag = DAG(
    dag_id='{TRIGGERED_DAG_ID}',
    schedule=None,
    start_date=datetime(2019, 1, 1),
)

task = EmptyOperator(task_id='test', dag=dag)
"""


class TestDagRunOperator:
    def setup_method(self):
        # Airflow relies on reading the DAG from disk when triggering it.
        # Therefore write a temp file holding the DAG to trigger.
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            self._tmpfile = f.name
            f.write(DAG_SCRIPT)
            f.flush()
        self.f_name = f.name

        with create_session() as session:
            session.add(DagModel(dag_id=TRIGGERED_DAG_ID, fileloc=self._tmpfile))
            session.commit()

    def re_sync_triggered_dag_to_db(self, dag, dag_maker):
        TracebackSessionForTests.set_allow_db_access(dag_maker.session, True)
        dagbag = DagBag(self.f_name, read_dags_from_db=False, include_examples=False)
        dagbag.bag_dag(dag)
        dagbag.sync_to_db(session=dag_maker.session)
        TracebackSessionForTests.set_allow_db_access(dag_maker.session, False)

    def teardown_method(self):
        """Cleanup state after testing in DB."""
        with create_session() as session:
            session.query(Log).filter(Log.dag_id == TEST_DAG_ID).delete(synchronize_session=False)
            for dbmodel in [DagModel, DagRun, TaskInstance, SerializedDagModel]:
                session.query(dbmodel).filter(dbmodel.dag_id.in_([TRIGGERED_DAG_ID, TEST_DAG_ID])).delete(
                    synchronize_session=False
                )

        # pathlib.Path(self._tmpfile).unlink()

    def assert_extra_link(self, triggered_dag_run, triggering_task, session):
        """
        Asserts whether the correct extra links url will be created.

        Specifically it tests whether the correct dag id and run id are passed to
        the method which constructs the final url.
        Note: We can't run that method to generate the url itself because the Flask app context
        isn't available within the test logic, so it is mocked here.
        """
        triggering_ti = (
            session.query(TaskInstance)
            .filter_by(
                task_id=triggering_task.task_id,
                dag_id=triggering_task.dag_id,
            )
            .one()
        )
        with mock.patch("airflow.operators.trigger_dagrun.build_airflow_url_with_query") as mock_build_url:
            triggering_task.get_extra_links(triggering_ti, "Triggered DAG")
        assert mock_build_url.called
        args, _ = mock_build_url.call_args
        expected_args = {
            "dag_id": triggered_dag_run.dag_id,
            "dag_run_id": triggered_dag_run.run_id,
        }
        assert expected_args in args

    def test_trigger_dagrun(self, dag_maker):
        """Test TriggerDagRunOperator."""
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(task_id="test_task", trigger_dag_id=TRIGGERED_DAG_ID)
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        dagrun = dag_maker.session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).one()
        assert dagrun.external_trigger
        assert dagrun.run_id == DagRun.generate_run_id(DagRunType.MANUAL, dagrun.logical_date)
        self.assert_extra_link(dagrun, task, dag_maker.session)

    def test_trigger_dagrun_dag_id_from_xcom_args(self):
        get_dag_id_task = PythonOperator(
            task_id="get_dag_id_task", python_callable=lambda: TRIGGERED_DAG_ID, dag=self.dag
        )
        get_dag_id_task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        task = TriggerDagRunOperator(task_id="test_task", trigger_dag_id=get_dag_id_task.output, dag=self.dag)
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagrun = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).one()
            assert dagrun.external_trigger
            assert dagrun.run_id == DagRun.generate_run_id(DagRunType.MANUAL, dagrun.logical_date)
            self.assert_extra_link(dagrun, task, session)

    def test_trigger_dagrun_custom_run_id(self):
        task = TriggerDagRunOperator(
            task_id="test_task",
            trigger_dag_id=TRIGGERED_DAG_ID,
            trigger_run_id="custom_run_id",
            dag=self.dag,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1
            assert dagruns[0].run_id == "custom_run_id"

    def test_trigger_dagrun_with_logical_date(self, dag_maker):
        """Test TriggerDagRunOperator with custom logical_date."""
        custom_logical_date = timezone.datetime(2021, 1, 2, 3, 4, 5)
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_trigger_dagrun_with_logical_date",
                trigger_dag_id=TRIGGERED_DAG_ID,
                logical_date=custom_logical_date,
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagrun = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).one()
            assert dagrun.external_trigger
            assert dagrun.logical_date == custom_logical_date
            assert dagrun.run_id == DagRun.generate_run_id(DagRunType.MANUAL, custom_logical_date)
            self.assert_extra_link(dagrun, task, session)

    @pytest.mark.skip_if_database_isolation_mode  # Known to be broken in db isolation mode
    def test_trigger_dagrun_twice(self, dag_maker):
        """Test TriggerDagRunOperator with custom logical_date."""
        utc_now = timezone.utcnow()
        run_id = f"manual__{utc_now.isoformat()}"
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_trigger_dagrun_with_logical_date",
                trigger_dag_id=TRIGGERED_DAG_ID,
                trigger_run_id=run_id,
                logical_date=utc_now,
                poke_interval=1,
                reset_dag_run=True,
                wait_for_completion=True,
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        dag_run = DagRun(
            dag_id=TRIGGERED_DAG_ID,
            execution_date=utc_now,
            state=State.SUCCESS,
            run_type="manual",
            run_id=run_id,
        )
        dag_maker.session.add(dag_run)
        dag_maker.session.commit()
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        dagruns = dag_maker.session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
        assert len(dagruns) == 1
        triggered_dag_run = dagruns[0]
        assert triggered_dag_run.external_trigger
        assert triggered_dag_run.logical_date == utc_now
        self.assert_extra_link(triggered_dag_run, task, dag_maker.session)

    @pytest.mark.skip_if_database_isolation_mode  # Known to be broken in db isolation mode
    def test_trigger_dagrun_with_scheduled_dag_run(self, dag_maker):
        """Test TriggerDagRunOperator with custom logical_date and scheduled dag_run."""
        utc_now = timezone.utcnow()
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_trigger_dagrun_with_logical_date",
                trigger_dag_id=TRIGGERED_DAG_ID,
                logical_date=utc_now,
                poke_interval=1,
                reset_dag_run=True,
                wait_for_completion=True,
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        run_id = f"scheduled__{utc_now.isoformat()}"
        dag_run = DagRun(
            dag_id=TRIGGERED_DAG_ID,
            execution_date=utc_now,
            state=State.SUCCESS,
            run_type="scheduled",
            run_id=run_id,
        )
        dag_maker.session.add(dag_run)
        dag_maker.session.commit()
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        dagruns = dag_maker.session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
        assert len(dagruns) == 1
        triggered_dag_run = dagruns[0]
        assert triggered_dag_run.external_trigger
        assert triggered_dag_run.logical_date == utc_now
        self.assert_extra_link(triggered_dag_run, task, dag_maker.session)

    def test_trigger_dagrun_with_templated_logical_date(self, dag_maker):
        """Test TriggerDagRunOperator with templated logical_date."""
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_trigger_dagrun_with_str_logical_date",
                trigger_dag_id=TRIGGERED_DAG_ID,
                logical_date="{{ logical_date }}",
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1
            triggered_dag_run = dagruns[0]
            assert triggered_dag_run.external_trigger
            assert triggered_dag_run.logical_date == DEFAULT_DATE
            self.assert_extra_link(triggered_dag_run, task, session)

    def test_trigger_dagrun_operator_conf(self, dag_maker):
        """Test passing conf to the triggered DagRun."""
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_trigger_dagrun_with_str_logical_date",
                trigger_dag_id=TRIGGERED_DAG_ID,
                conf={"foo": "bar"},
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1
            assert dagruns[0].conf == {"foo": "bar"}

    def test_trigger_dagrun_operator_templated_invalid_conf(self, dag_maker):
        """Test passing a conf that is not JSON Serializable raise error."""
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_trigger_dagrun_with_invalid_conf",
                trigger_dag_id=TRIGGERED_DAG_ID,
                conf={"foo": "{{ dag.dag_id }}", "datetime": timezone.utcnow()},
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        with pytest.raises(AirflowException, match="^conf parameter should be JSON Serializable$"):
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_trigger_dagrun_operator_templated_conf(self, dag_maker):
        """Test passing a templated conf to the triggered DagRun."""
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_trigger_dagrun_with_str_logical_date",
                trigger_dag_id=TRIGGERED_DAG_ID,
                conf={"foo": "{{ dag.dag_id }}"},
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1
            assert dagruns[0].conf == {"foo": TEST_DAG_ID}

    def test_trigger_dagrun_with_reset_dag_run_false(self, dag_maker):
        """Test TriggerDagRunOperator without reset_dag_run."""
        logical_date = DEFAULT_DATE
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_task",
                trigger_dag_id=TRIGGERED_DAG_ID,
                trigger_run_id=None,
                logical_date=None,
                reset_dag_run=False,
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        task.run(start_date=logical_date, end_date=logical_date, ignore_ti_state=True)
        task.run(start_date=logical_date, end_date=logical_date, ignore_ti_state=True)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 2

    @pytest.mark.parametrize(
        "trigger_run_id, trigger_logical_date",
        [
            (None, DEFAULT_DATE),
            ("dummy_run_id", None),
            ("dummy_run_id", DEFAULT_DATE),
        ],
    )
    def test_trigger_dagrun_with_reset_dag_run_false_fail(
        self, trigger_run_id, trigger_logical_date, dag_maker
    ):
        """Test TriggerDagRunOperator without reset_dag_run but triggered dag fails."""
        logical_date = DEFAULT_DATE
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_task",
                trigger_dag_id=TRIGGERED_DAG_ID,
                trigger_run_id=trigger_run_id,
                logical_date=trigger_logical_date,
                reset_dag_run=False,
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        task.run(start_date=logical_date, end_date=logical_date, ignore_ti_state=True)

        with pytest.raises(DagRunAlreadyExists):
            task.run(start_date=logical_date, end_date=logical_date, ignore_ti_state=True)

    def test_trigger_dagrun_with_skip_when_already_exists(self, dag_maker):
        """Test TriggerDagRunOperator with skip_when_already_exists."""
        execution_date = DEFAULT_DATE
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_task",
                trigger_dag_id=TRIGGERED_DAG_ID,
                trigger_run_id="dummy_run_id",
                reset_dag_run=False,
                skip_when_already_exists=True,
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dr: DagRun = dag_maker.create_dagrun()
        task.run(start_date=execution_date, end_date=execution_date, ignore_ti_state=True)
        assert dr.get_task_instance("test_task").state == TaskInstanceState.SUCCESS
        task.run(start_date=execution_date, end_date=execution_date, ignore_ti_state=True)
        assert dr.get_task_instance("test_task").state == TaskInstanceState.SKIPPED

    @pytest.mark.skip_if_database_isolation_mode  # Known to be broken in db isolation mode
    @pytest.mark.parametrize(
        "trigger_run_id, trigger_logical_date, expected_dagruns_count",
        [
            (None, DEFAULT_DATE, 1),
            (None, None, 2),
            ("dummy_run_id", DEFAULT_DATE, 1),
            ("dummy_run_id", None, 1),
        ],
    )
    def test_trigger_dagrun_with_reset_dag_run_true(
        self, trigger_run_id, trigger_logical_date, expected_dagruns_count, dag_maker
    ):
        """Test TriggerDagRunOperator with reset_dag_run."""
        logical_date = DEFAULT_DATE
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_task",
                trigger_dag_id=TRIGGERED_DAG_ID,
                trigger_run_id=trigger_run_id,
                logical_date=trigger_logical_date,
                reset_dag_run=True,
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        task.run(start_date=logical_date, end_date=logical_date, ignore_ti_state=True)
        task.run(start_date=logical_date, end_date=logical_date, ignore_ti_state=True)

        with create_session() as session:
            dag_runs = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dag_runs) == expected_dagruns_count
            assert dag_runs[0].external_trigger

    @pytest.mark.skip_if_database_isolation_mode  # Known to be broken in db isolation mode
    def test_trigger_dagrun_with_wait_for_completion_true(self, dag_maker):
        """Test TriggerDagRunOperator with wait_for_completion."""
        logical_date = DEFAULT_DATE
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_task",
                trigger_dag_id=TRIGGERED_DAG_ID,
                logical_date=logical_date,
                wait_for_completion=True,
                poke_interval=10,
                allowed_states=[State.QUEUED],
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        task.run(start_date=logical_date, end_date=logical_date)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1

    @pytest.mark.skip_if_database_isolation_mode  # Known to be broken in db isolation mode
    def test_trigger_dagrun_with_wait_for_completion_true_fail(self, dag_maker):
        """Test TriggerDagRunOperator with wait_for_completion but triggered dag fails."""
        logical_date = DEFAULT_DATE
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_task",
                trigger_dag_id=TRIGGERED_DAG_ID,
                logical_date=logical_date,
                wait_for_completion=True,
                poke_interval=10,
                failed_states=[State.QUEUED],
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        with pytest.raises(AirflowException):
            task.run(start_date=logical_date, end_date=logical_date)

    def test_trigger_dagrun_triggering_itself(self, dag_maker):
        """Test TriggerDagRunOperator that triggers itself"""
        logical_date = DEFAULT_DATE
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_task",
                trigger_dag_id=TEST_DAG_ID,
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        task.run(start_date=logical_date, end_date=logical_date)

        dagruns = (
            dag_maker.session.query(DagRun)
            .filter(DagRun.dag_id == TEST_DAG_ID)
            .order_by(DagRun.execution_date)
            .all()
        )
        assert len(dagruns) == 2
        triggered_dag_run = dagruns[1]
        assert triggered_dag_run.state == State.QUEUED

    def test_trigger_dagrun_triggering_itself_with_logical_date(self, dag_maker):
        """Test TriggerDagRunOperator that triggers itself with logical date,
        fails with DagRunAlreadyExists"""
        logical_date = DEFAULT_DATE
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_task",
                trigger_dag_id=TEST_DAG_ID,
                logical_date=logical_date,
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        with pytest.raises(DagRunAlreadyExists):
            task.run(start_date=logical_date, end_date=logical_date)

    @pytest.mark.skip_if_database_isolation_mode  # Known to be broken in db isolation mode
    def test_trigger_dagrun_with_wait_for_completion_true_defer_false(self, dag_maker):
        """Test TriggerDagRunOperator with wait_for_completion."""
        logical_date = DEFAULT_DATE
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_task",
                trigger_dag_id=TRIGGERED_DAG_ID,
                logical_date=logical_date,
                wait_for_completion=True,
                poke_interval=10,
                allowed_states=[State.QUEUED],
                deferrable=False,
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()
        task.run(start_date=logical_date, end_date=logical_date)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1

    @pytest.mark.skip_if_database_isolation_mode  # Known to be broken in db isolation mode
    def test_trigger_dagrun_with_wait_for_completion_true_defer_true(self, dag_maker):
        """Test TriggerDagRunOperator with wait_for_completion."""
        logical_date = DEFAULT_DATE
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_task",
                trigger_dag_id=TRIGGERED_DAG_ID,
                logical_date=logical_date,
                wait_for_completion=True,
                poke_interval=10,
                allowed_states=[State.QUEUED],
                deferrable=True,
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()

        task.run(start_date=logical_date, end_date=logical_date)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1
        trigger = DagStateTrigger(
            dag_id="down_stream",
            execution_dates=[DEFAULT_DATE],
            poll_interval=20,
            states=["success", "failed"],
        )

        task.execute_complete(context={}, event=trigger.serialize())

    @pytest.mark.skip_if_database_isolation_mode  # Known to be broken in db isolation mode
    def test_trigger_dagrun_with_wait_for_completion_true_defer_true_failure(self, dag_maker):
        """Test TriggerDagRunOperator wait_for_completion dag run in non defined state."""
        logical_date = DEFAULT_DATE
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_task",
                trigger_dag_id=TRIGGERED_DAG_ID,
                logical_date=logical_date,
                wait_for_completion=True,
                poke_interval=10,
                allowed_states=[State.SUCCESS],
                deferrable=True,
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()

        task.run(start_date=logical_date, end_date=logical_date)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1

        trigger = DagStateTrigger(
            dag_id="down_stream",
            execution_dates=[DEFAULT_DATE],
            poll_interval=20,
            states=["success", "failed"],
        )
        with pytest.raises(AirflowException, match="which is not in"):
            task.execute_complete(
                context={},
                event=trigger.serialize(),
            )

    @pytest.mark.skip_if_database_isolation_mode  # Known to be broken in db isolation mode
    def test_trigger_dagrun_with_wait_for_completion_true_defer_true_failure_2(self, dag_maker):
        """Test TriggerDagRunOperator  wait_for_completion dag run in failed state."""
        logical_date = DEFAULT_DATE
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_task",
                trigger_dag_id=TRIGGERED_DAG_ID,
                logical_date=logical_date,
                wait_for_completion=True,
                poke_interval=10,
                allowed_states=[State.SUCCESS],
                failed_states=[State.QUEUED],
                deferrable=True,
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()

        task.run(start_date=logical_date, end_date=logical_date)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1

        trigger = DagStateTrigger(
            dag_id="down_stream",
            execution_dates=[DEFAULT_DATE],
            poll_interval=20,
            states=["success", "failed"],
        )

        with pytest.raises(AirflowException, match="failed with failed state"):
            task.execute_complete(context={}, event=trigger.serialize())

    @pytest.mark.skip_if_database_isolation_mode  # Known to be broken in db isolation mode
    @pytest.mark.parametrize(
        argnames=["trigger_logical_date"],
        argvalues=[
            pytest.param(DEFAULT_DATE, id=f"logical_date={DEFAULT_DATE}"),
            pytest.param(None, id="logical_date=None"),
        ],
    )
    def test_dagstatetrigger_execution_dates(self, trigger_logical_date, dag_maker):
        """Ensure that the DagStateTrigger is called with the triggered DAG's logical date."""
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_task",
                trigger_dag_id=TRIGGERED_DAG_ID,
                logical_date=trigger_logical_date,
                wait_for_completion=True,
                poke_interval=5,
                allowed_states=[DagRunState.QUEUED],
                deferrable=True,
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()

        mock_task_defer = mock.MagicMock(side_effect=task.defer)
        with mock.patch.object(TriggerDagRunOperator, "defer", mock_task_defer), pytest.raises(TaskDeferred):
            task.execute({"task_instance": mock.MagicMock()})

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1

        assert mock_task_defer.call_args_list[0].kwargs["trigger"].execution_dates == [
            pendulum.instance(dagruns[0].logical_date)
        ]

    @pytest.mark.skip_if_database_isolation_mode  # Known to be broken in db isolation mode
    def test_dagstatetrigger_execution_dates_with_clear_and_reset(self, dag_maker):
        """Check DagStateTrigger is called with the triggered DAG's logical date on subsequent defers."""
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_task",
                trigger_dag_id=TRIGGERED_DAG_ID,
                trigger_run_id="custom_run_id",
                wait_for_completion=True,
                poke_interval=5,
                allowed_states=[DagRunState.QUEUED],
                deferrable=True,
                reset_dag_run=True,
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()

        mock_task_defer = mock.MagicMock(side_effect=task.defer)
        with mock.patch.object(TriggerDagRunOperator, "defer", mock_task_defer), pytest.raises(TaskDeferred):
            task.execute({"task_instance": mock.MagicMock()})

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            triggered_logical_date = dagruns[0].logical_date
            assert len(dagruns) == 1

        assert mock_task_defer.call_args_list[0].kwargs["trigger"].execution_dates == [
            pendulum.instance(triggered_logical_date)
        ]

        # Simulate the TriggerDagRunOperator task being cleared (aka executed again). A DagRunAlreadyExists
        # exception should be raised because of the previous DAG run.
        with mock.patch.object(TriggerDagRunOperator, "defer", mock_task_defer), pytest.raises(
            (DagRunAlreadyExists, TaskDeferred)
        ):
            task.execute({"task_instance": mock.MagicMock()})

        # Still only one DAG run should exist for the triggered DAG since the DAG will be cleared since the
        # TriggerDagRunOperator task is configured with `reset_dag_run=True`.
        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1

        # The second DagStateTrigger call should still use the original `logical_date` value.
        assert mock_task_defer.call_args_list[1].kwargs["trigger"].execution_dates == [
            pendulum.instance(triggered_logical_date)
        ]

    def test_trigger_dagrun_with_no_failed_state(self, dag_maker):
        logical_date = DEFAULT_DATE
        with dag_maker(
            TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True
        ) as dag:
            task = TriggerDagRunOperator(
                task_id="test_task",
                trigger_dag_id=TRIGGERED_DAG_ID,
                logical_date=logical_date,
                wait_for_completion=True,
                poke_interval=10,
                failed_states=[],
            )
        self.re_sync_triggered_dag_to_db(dag, dag_maker)
        dag_maker.create_dagrun()

        assert task.failed_states == []
