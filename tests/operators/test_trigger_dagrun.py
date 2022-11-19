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

import pathlib
import tempfile
from datetime import datetime
from unittest import mock

import pytest

from airflow.exceptions import AirflowException, DagRunAlreadyExists
from airflow.models import DAG, DagBag, DagModel, DagRun, Log, TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

DEFAULT_DATE = datetime(2019, 1, 1, tzinfo=timezone.utc)
TEST_DAG_ID = "testdag"
TRIGGERED_DAG_ID = "triggerdag"
DAG_SCRIPT = (
    "from datetime import datetime\n\n"
    "from airflow.models import DAG\n"
    "from airflow.operators.empty import EmptyOperator\n\n"
    "dag = DAG(\n"
    'dag_id="{dag_id}", \n'
    'default_args={{"start_date": datetime(2019, 1, 1)}}, \n'
    "schedule=None,\n"
    ")\n"
    'task = EmptyOperator(task_id="test", dag=dag)'
).format(dag_id=TRIGGERED_DAG_ID)


class TestDagRunOperator:
    def setup_method(self):
        # Airflow relies on reading the DAG from disk when triggering it.
        # Therefore write a temp file holding the DAG to trigger.
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            self._tmpfile = f.name
            f.write(DAG_SCRIPT)
            f.flush()

        with create_session() as session:
            session.add(DagModel(dag_id=TRIGGERED_DAG_ID, fileloc=self._tmpfile))
            session.commit()

        self.dag = DAG(TEST_DAG_ID, default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
        dagbag = DagBag(f.name, read_dags_from_db=False, include_examples=False)
        dagbag.bag_dag(self.dag, root_dag=self.dag)
        dagbag.sync_to_db()

    def teardown_method(self):
        """Cleanup state after testing in DB."""
        with create_session() as session:
            session.query(Log).filter(Log.dag_id == TEST_DAG_ID).delete(synchronize_session=False)
            for dbmodel in [DagModel, DagRun, TaskInstance, SerializedDagModel]:
                session.query(dbmodel).filter(dbmodel.dag_id.in_([TRIGGERED_DAG_ID, TEST_DAG_ID])).delete(
                    synchronize_session=False
                )

        pathlib.Path(self._tmpfile).unlink()

    def assert_extra_link(self, triggered_dag_run, triggering_task, session):
        """
        Asserts whether the correct extra links url will be created.

        Specifically it tests whether the correct dag id and date are passed to
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
            "base_date": triggered_dag_run.execution_date.isoformat(),
        }
        assert expected_args in args

    def test_trigger_dagrun(self):
        """Test TriggerDagRunOperator."""
        task = TriggerDagRunOperator(task_id="test_task", trigger_dag_id=TRIGGERED_DAG_ID, dag=self.dag)
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagrun = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).one()
            assert dagrun.external_trigger
            assert dagrun.run_id == DagRun.generate_run_id(DagRunType.MANUAL, dagrun.execution_date)
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

    def test_trigger_dagrun_with_execution_date(self):
        """Test TriggerDagRunOperator with custom execution_date."""
        custom_execution_date = timezone.datetime(2021, 1, 2, 3, 4, 5)
        task = TriggerDagRunOperator(
            task_id="test_trigger_dagrun_with_execution_date",
            trigger_dag_id=TRIGGERED_DAG_ID,
            execution_date=custom_execution_date,
            dag=self.dag,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagrun = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).one()
            assert dagrun.external_trigger
            assert dagrun.execution_date == custom_execution_date
            assert dagrun.run_id == DagRun.generate_run_id(DagRunType.MANUAL, custom_execution_date)
            self.assert_extra_link(dagrun, task, session)

    def test_trigger_dagrun_with_custom_note(self):
        notes_value = "Custom note for newly created DagRun."
        task = TriggerDagRunOperator(
            task_id="test_task", trigger_dag_id=TRIGGERED_DAG_ID, dag=self.dag, dag_run_notes=notes_value
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagrun = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).one()
            assert dagrun.external_trigger
            assert dagrun.notes == notes_value

    def test_trigger_dagrun_twice(self):
        """Test TriggerDagRunOperator with custom execution_date."""
        utc_now = timezone.utcnow()
        task = TriggerDagRunOperator(
            task_id="test_trigger_dagrun_with_execution_date",
            trigger_dag_id=TRIGGERED_DAG_ID,
            execution_date=utc_now,
            dag=self.dag,
            poke_interval=1,
            reset_dag_run=True,
            wait_for_completion=True,
        )
        run_id = f"manual__{utc_now.isoformat()}"
        with create_session() as session:
            dag_run = DagRun(
                dag_id=TRIGGERED_DAG_ID,
                execution_date=utc_now,
                state=State.SUCCESS,
                run_type="manual",
                run_id=run_id,
            )
            session.add(dag_run)
            session.commit()
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1
            triggered_dag_run = dagruns[0]
            assert triggered_dag_run.external_trigger
            assert triggered_dag_run.execution_date == utc_now
            self.assert_extra_link(triggered_dag_run, task, session)

    def test_trigger_dagrun_with_scheduled_dag_run(self):
        """Test TriggerDagRunOperator with custom execution_date and scheduled dag_run."""
        utc_now = timezone.utcnow()
        task = TriggerDagRunOperator(
            task_id="test_trigger_dagrun_with_execution_date",
            trigger_dag_id=TRIGGERED_DAG_ID,
            execution_date=utc_now,
            dag=self.dag,
            poke_interval=1,
            reset_dag_run=True,
            wait_for_completion=True,
        )
        run_id = f"scheduled__{utc_now.isoformat()}"
        with create_session() as session:
            dag_run = DagRun(
                dag_id=TRIGGERED_DAG_ID,
                execution_date=utc_now,
                state=State.SUCCESS,
                run_type="scheduled",
                run_id=run_id,
            )
            session.add(dag_run)
            session.commit()
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1
            triggered_dag_run = dagruns[0]
            assert triggered_dag_run.external_trigger
            assert triggered_dag_run.execution_date == utc_now
            self.assert_extra_link(triggered_dag_run, task, session)

    def test_trigger_dagrun_with_templated_execution_date(self):
        """Test TriggerDagRunOperator with templated execution_date."""
        task = TriggerDagRunOperator(
            task_id="test_trigger_dagrun_with_str_execution_date",
            trigger_dag_id=TRIGGERED_DAG_ID,
            execution_date="{{ logical_date }}",
            dag=self.dag,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1
            triggered_dag_run = dagruns[0]
            assert triggered_dag_run.external_trigger
            assert triggered_dag_run.execution_date == DEFAULT_DATE
            self.assert_extra_link(triggered_dag_run, task, session)

    def test_trigger_dagrun_operator_conf(self):
        """Test passing conf to the triggered DagRun."""
        task = TriggerDagRunOperator(
            task_id="test_trigger_dagrun_with_str_execution_date",
            trigger_dag_id=TRIGGERED_DAG_ID,
            conf={"foo": "bar"},
            dag=self.dag,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1
            assert dagruns[0].conf == {"foo": "bar"}

    def test_trigger_dagrun_operator_templated_invalid_conf(self):
        """Test passing a conf that is not JSON Serializable raise error."""
        task = TriggerDagRunOperator(
            task_id="test_trigger_dagrun_with_invalid_conf",
            trigger_dag_id=TRIGGERED_DAG_ID,
            conf={"foo": "{{ dag.dag_id }}", "datetime": timezone.utcnow()},
            dag=self.dag,
        )
        with pytest.raises(AirflowException, match="^conf parameter should be JSON Serializable$"):
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_trigger_dagrun_operator_templated_conf(self):
        """Test passing a templated conf to the triggered DagRun."""
        task = TriggerDagRunOperator(
            task_id="test_trigger_dagrun_with_str_execution_date",
            trigger_dag_id=TRIGGERED_DAG_ID,
            conf={"foo": "{{ dag.dag_id }}"},
            dag=self.dag,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1
            assert dagruns[0].conf == {"foo": TEST_DAG_ID}

    def test_trigger_dagrun_with_reset_dag_run_false(self):
        """Test TriggerDagRunOperator with reset_dag_run."""
        execution_date = DEFAULT_DATE
        task = TriggerDagRunOperator(
            task_id="test_task",
            trigger_dag_id=TRIGGERED_DAG_ID,
            execution_date=execution_date,
            reset_dag_run=False,
            dag=self.dag,
        )
        task.run(start_date=execution_date, end_date=execution_date, ignore_ti_state=True)

        with pytest.raises(DagRunAlreadyExists):
            task.run(start_date=execution_date, end_date=execution_date, ignore_ti_state=True)

    def test_trigger_dagrun_with_reset_dag_run_true(self):
        """Test TriggerDagRunOperator with reset_dag_run."""
        execution_date = DEFAULT_DATE
        task = TriggerDagRunOperator(
            task_id="test_task",
            trigger_dag_id=TRIGGERED_DAG_ID,
            execution_date=execution_date,
            reset_dag_run=True,
            dag=self.dag,
        )
        task.run(start_date=execution_date, end_date=execution_date, ignore_ti_state=True)
        task.run(start_date=execution_date, end_date=execution_date, ignore_ti_state=True)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1
            assert dagruns[0].external_trigger

    def test_trigger_dagrun_with_wait_for_completion_true(self):
        """Test TriggerDagRunOperator with wait_for_completion."""
        execution_date = DEFAULT_DATE
        task = TriggerDagRunOperator(
            task_id="test_task",
            trigger_dag_id=TRIGGERED_DAG_ID,
            execution_date=execution_date,
            wait_for_completion=True,
            poke_interval=10,
            allowed_states=[State.QUEUED],
            dag=self.dag,
        )
        task.run(start_date=execution_date, end_date=execution_date)

        with create_session() as session:
            dagruns = session.query(DagRun).filter(DagRun.dag_id == TRIGGERED_DAG_ID).all()
            assert len(dagruns) == 1

    def test_trigger_dagrun_with_wait_for_completion_true_fail(self):
        """Test TriggerDagRunOperator with wait_for_completion but triggered dag fails."""
        execution_date = DEFAULT_DATE
        task = TriggerDagRunOperator(
            task_id="test_task",
            trigger_dag_id=TRIGGERED_DAG_ID,
            execution_date=execution_date,
            wait_for_completion=True,
            poke_interval=10,
            failed_states=[State.QUEUED],
            dag=self.dag,
        )
        with pytest.raises(AirflowException):
            task.run(start_date=execution_date, end_date=execution_date)

    def test_trigger_dagrun_triggering_itself(self):
        """Test TriggerDagRunOperator that triggers itself"""
        execution_date = DEFAULT_DATE
        task = TriggerDagRunOperator(
            task_id="test_task",
            trigger_dag_id=self.dag.dag_id,
            dag=self.dag,
        )
        task.run(start_date=execution_date, end_date=execution_date)

        with create_session() as session:
            dagruns = (
                session.query(DagRun)
                .filter(DagRun.dag_id == self.dag.dag_id)
                .order_by(DagRun.execution_date)
                .all()
            )
            assert len(dagruns) == 2
            triggered_dag_run = dagruns[1]
            assert triggered_dag_run.state == State.QUEUED
            self.assert_extra_link(triggered_dag_run, task, session)

    def test_trigger_dagrun_triggering_itself_with_execution_date(self):
        """Test TriggerDagRunOperator that triggers itself with execution date,
        fails with DagRunAlreadyExists"""
        execution_date = DEFAULT_DATE
        task = TriggerDagRunOperator(
            task_id="test_task",
            trigger_dag_id=self.dag.dag_id,
            execution_date=execution_date,
            dag=self.dag,
        )
        with pytest.raises(DagRunAlreadyExists):
            task.run(start_date=execution_date, end_date=execution_date)
