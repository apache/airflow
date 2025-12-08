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

import itertools
import logging
import re
from datetime import time, timedelta
from unittest import mock

import pytest

from airflow import settings
from airflow.models import DagRun, TaskInstance
from airflow.models.dag import DAG
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.xcom_arg import XComArg
from airflow.providers.common.compat.sdk import (
    AirflowException,
    AirflowSensorTimeout,
    AirflowSkipException,
    TaskDeferred,
)
from airflow.providers.standard.exceptions import (
    DuplicateStateError,
    ExternalDagDeletedError,
    ExternalDagFailedError,
    ExternalDagNotFoundError,
    ExternalTaskFailedError,
    ExternalTaskGroupFailedError,
    ExternalTaskGroupNotFoundError,
    ExternalTaskNotFoundError,
)
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.providers.standard.sensors.time import TimeSensor
from airflow.providers.standard.triggers.external_task import WorkflowTrigger
from airflow.serialization.serialized_objects import SerializedBaseOperator
from airflow.timetables.base import DataInterval
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType

from tests_common.test_utils.dag import create_scheduler_dag, sync_dag_to_db, sync_dags_to_db
from tests_common.test_utils.db import clear_db_runs
from tests_common.test_utils.mock_operators import MockOperator
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_2_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.models.dag_version import DagVersion
    from airflow.sdk import BaseOperator, task as task_deco
    from airflow.utils.types import DagRunTriggeredByType
else:
    from airflow.decorators import task as task_deco  # type: ignore[attr-defined,no-redef]
    from airflow.models import BaseOperator  # type: ignore[assignment,no-redef]

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk import TaskGroup
    from airflow.sdk.timezone import coerce_datetime, datetime
else:
    from airflow.utils.task_group import TaskGroup  # type: ignore[no-redef]
    from airflow.utils.timezone import coerce_datetime, datetime  # type: ignore[attr-defined,no-redef]

if AIRFLOW_V_3_2_PLUS:
    from airflow.dag_processing.dagbag import DagBag
else:
    from airflow.models.dagbag import DagBag  # type: ignore[attr-defined, no-redef]


pytestmark = pytest.mark.db_test

TI = TaskInstance

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = "unit_test_dag"
TEST_TASK_ID = "time_sensor_check"
TEST_TASK_ID_ALTERNATE = "time_sensor_check_alternate"
TEST_TASK_GROUP_ID = "time_sensor_group_id"
DEV_NULL = "/dev/null"
TASK_ID = "external_task_sensor_check"
EXTERNAL_DAG_ID = "child_dag"  # DAG the external task sensor is waiting on
EXTERNAL_TASK_ID = "child_task"  # Task the external task sensor is waiting on
EXTERNAL_ID_AND_IDS_PROVIDE_ERROR = "Only one of `external_task_id` or `external_task_ids` may be provided to ExternalTaskSensor; use external_task_id or external_task_ids or external_task_group_id."
EXTERNAL_IDS_AND_TASK_GROUP_ID_PROVIDE_ERROR = "Only one of `external_task_group_id` or `external_task_ids` may be provided to ExternalTaskSensor; use external_task_id or external_task_ids or external_task_group_id."


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_runs()


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for v3.0+")
class TestExternalTaskSensorV2:
    def setup_method(self):
        self.dagbag = DagBag(dag_folder=DEV_NULL, include_examples=True)
        self.args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, schedule=None, default_args=self.args)
        self.dag_run_id = DagRunType.MANUAL.generate_run_id(DEFAULT_DATE)

    def add_time_sensor(self, task_id=TEST_TASK_ID):
        # TODO: Remove BaseOperator in https://github.com/apache/airflow/issues/47447
        class TimeSensorNew(TimeSensor, BaseOperator):
            def poke(self, context):
                return True

        op = TimeSensorNew(task_id=task_id, target_time=time(0), dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def add_fake_task_group(self, target_states=None):
        target_states = [State.SUCCESS] * 2 if target_states is None else target_states
        with self.dag as dag:
            with TaskGroup(group_id=TEST_TASK_GROUP_ID) as task_group:
                _ = [EmptyOperator(task_id=f"task{i}") for i in range(len(target_states))]
            dag.sync_to_db()
        if AIRFLOW_V_3_0_PLUS:
            SerializedDagModel.write_dag(dag, bundle_name="testing")
        else:
            SerializedDagModel.write_dag(dag)

        for idx, task in enumerate(task_group):
            if AIRFLOW_V_3_0_PLUS:
                dag_version = DagVersion.get_latest_version(task_group[idx].dag_id)
                ti = TaskInstance(task=task, run_id=self.dag_run_id, dag_version_id=dag_version.id)
            else:
                ti = TaskInstance(task=task, run_id=self.dag_run_id)
            ti.run(ignore_ti_state=True, mark_success=True)
            ti.set_state(target_states[idx])

    def add_fake_task_group_with_dynamic_tasks(self, target_state=State.SUCCESS):
        map_indexes = range(5)
        with self.dag as dag:
            with TaskGroup(group_id=TEST_TASK_GROUP_ID) as task_group:

                @task_deco
                def fake_task():
                    pass

                @task_deco
                def fake_mapped_task(x: int):
                    return x

                fake_task()
                fake_mapped_task.expand(x=list(map_indexes))
        dag.sync_to_db()
        if AIRFLOW_V_3_0_PLUS:
            SerializedDagModel.write_dag(dag, bundle_name="testing")
        else:
            SerializedDagModel.write_dag(dag)

        for task in task_group:
            if task.task_id == "fake_mapped_task":
                for map_index in map_indexes:
                    if AIRFLOW_V_3_0_PLUS:
                        dag_version = DagVersion.get_latest_version(dag.dag_id)
                        ti = TaskInstance(task=task, run_id=self.dag_run_id, dag_version_id=dag_version.id)
                    else:
                        ti = TaskInstance(task=task, run_id=self.dag_run_id, map_index=map_index)
                    ti.run(ignore_ti_state=True, mark_success=True)
                    ti.set_state(target_state)
            else:
                if AIRFLOW_V_3_0_PLUS:
                    dag_version = DagVersion.get_latest_version(dag.dag_id)
                    ti = TaskInstance(task=task, run_id=self.dag_run_id, dag_version_id=dag_version.id)
                else:
                    ti = TaskInstance(task=task, run_id=self.dag_run_id)
                ti.run(ignore_ti_state=True, mark_success=True)
                ti.set_state(target_state)

    def test_external_task_sensor(self):
        self.add_time_sensor()
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id=TEST_DAG_ID,
            external_task_id=TEST_TASK_ID,
            dag=self.dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_sensor_multiple_task_ids(self):
        self.add_time_sensor(task_id=TEST_TASK_ID)
        self.add_time_sensor(task_id=TEST_TASK_ID_ALTERNATE)
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check_task_ids",
            external_dag_id=TEST_DAG_ID,
            external_task_ids=[TEST_TASK_ID, TEST_TASK_ID_ALTERNATE],
            dag=self.dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_sensor_with_task_group(self):
        self.add_time_sensor()
        self.add_fake_task_group()
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_task_group",
            external_dag_id=TEST_DAG_ID,
            external_task_group_id=TEST_TASK_GROUP_ID,
            dag=self.dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_raise_with_external_task_sensor_task_id_and_task_ids(self):
        with pytest.raises(
            ValueError,
            match=EXTERNAL_ID_AND_IDS_PROVIDE_ERROR,
        ):
            ExternalTaskSensor(
                task_id="test_external_task_sensor_task_id_with_task_ids_failed_status",
                external_dag_id=TEST_DAG_ID,
                external_task_id=TEST_TASK_ID,
                external_task_ids=TEST_TASK_ID,
                dag=self.dag,
            )

    def test_raise_with_external_task_sensor_task_group_and_task_id(self):
        with pytest.raises(ValueError, match=EXTERNAL_IDS_AND_TASK_GROUP_ID_PROVIDE_ERROR):
            ExternalTaskSensor(
                task_id="test_external_task_sensor_task_group_with_task_id_failed_status",
                external_dag_id=TEST_DAG_ID,
                external_task_id=TEST_TASK_ID,
                external_task_group_id=TEST_TASK_GROUP_ID,
                dag=self.dag,
            )

    def test_raise_with_external_task_sensor_task_group_and_task_ids(self):
        with pytest.raises(ValueError, match=EXTERNAL_IDS_AND_TASK_GROUP_ID_PROVIDE_ERROR):
            ExternalTaskSensor(
                task_id="test_external_task_sensor_task_group_with_task_ids_failed_status",
                external_dag_id=TEST_DAG_ID,
                external_task_ids=TEST_TASK_ID,
                external_task_group_id=TEST_TASK_GROUP_ID,
                dag=self.dag,
            )

    # by default i.e. check_existence=False, if task_group doesn't exist, the sensor will run till timeout,
    # this behaviour is similar to external_task_id doesn't exists
    def test_external_task_group_not_exists_without_check_existence(self):
        self.add_time_sensor()
        self.add_fake_task_group()
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id=TEST_DAG_ID,
            external_task_group_id="fake-task-group",
            timeout=0.001,
            dag=self.dag,
            poke_interval=0.1,
        )
        with pytest.raises(AirflowSensorTimeout, match="Sensor has timed out"):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_group_sensor_success(self):
        self.add_time_sensor()
        self.add_fake_task_group()
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id=TEST_DAG_ID,
            external_task_group_id=TEST_TASK_GROUP_ID,
            failed_states=[State.FAILED],
            dag=self.dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_group_sensor_failed_states(self):
        ti_states = [State.FAILED, State.FAILED]
        self.add_time_sensor()
        self.add_fake_task_group(ti_states)
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id=TEST_DAG_ID,
            external_task_group_id=TEST_TASK_GROUP_ID,
            failed_states=[State.FAILED],
            dag=self.dag,
        )
        with pytest.raises(
            ExternalTaskGroupFailedError,
            match=f"The external task_group '{TEST_TASK_GROUP_ID}' in DAG '{TEST_DAG_ID}' failed.",
        ):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_catch_overlap_allowed_failed_state(self):
        with pytest.raises(DuplicateStateError):
            ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id=TEST_DAG_ID,
                external_task_id=TEST_TASK_ID,
                allowed_states=[State.SUCCESS],
                failed_states=[State.SUCCESS],
                dag=self.dag,
            )

    def test_external_task_sensor_wrong_failed_states(self):
        with pytest.raises(
            ValueError,
            match="Valid values for `allowed_states`, `skipped_states` and `failed_states` when `external_task_id` or `external_task_ids` or `external_task_group_id` is not `None`",
        ):
            ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id=TEST_DAG_ID,
                external_task_id=TEST_TASK_ID,
                failed_states=["invalid_state"],
                dag=self.dag,
            )

    def test_external_task_sensor_failed_states(self):
        self.add_time_sensor()
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id=TEST_DAG_ID,
            external_task_id=TEST_TASK_ID,
            failed_states=["failed"],
            dag=self.dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_sensor_failed_states_as_success(self, caplog):
        self.add_time_sensor()
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id=TEST_DAG_ID,
            external_task_id=TEST_TASK_ID,
            allowed_states=["failed"],
            failed_states=["success"],
            dag=self.dag,
        )
        error_message = rf"Some of the external tasks \['{TEST_TASK_ID}'\] in DAG {TEST_DAG_ID} failed\."
        with caplog.at_level(logging.INFO, logger=op.log.name):
            caplog.clear()
            with pytest.raises(ExternalTaskFailedError, match=error_message):
                op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        assert (
            f"Poking for tasks ['{TEST_TASK_ID}'] in dag {TEST_DAG_ID} on {DEFAULT_DATE.isoformat()} ... "
        ) in caplog.messages

    def test_external_task_sensor_soft_fail_failed_states_as_skipped(self):
        self.add_time_sensor()
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id=TEST_DAG_ID,
            external_task_id=TEST_TASK_ID,
            allowed_states=[State.FAILED],
            failed_states=[State.SUCCESS],
            soft_fail=True,
            dag=self.dag,
        )

        # when
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # then
        session = settings.Session()
        task_instances: list[TI] = session.query(TI).filter(TI.task_id == op.task_id).all()
        assert len(task_instances) == 1, "Unexpected number of task instances"
        assert task_instances[0].state == State.SKIPPED, "Unexpected external task state"

    def test_external_task_sensor_skipped_states_as_skipped(self, session):
        self.add_time_sensor()
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id=TEST_DAG_ID,
            external_task_id=TEST_TASK_ID,
            allowed_states=[State.FAILED],
            skipped_states=[State.SUCCESS],
            dag=self.dag,
        )

        # when
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # then
        task_instances: list[TI] = session.query(TI).filter(TI.task_id == op.task_id).all()
        assert len(task_instances) == 1, "Unexpected number of task instances"
        assert task_instances[0].state == State.SKIPPED, "Unexpected external task state"

    def test_external_task_sensor_external_task_id_param(self, caplog):
        """Test external_task_ids is set properly when external_task_id is passed as a template"""
        self.add_time_sensor()
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id="{{ params.dag_id }}",
            external_task_id="{{ params.task_id }}",
            params={"dag_id": TEST_DAG_ID, "task_id": TEST_TASK_ID},
            dag=self.dag,
        )

        with caplog.at_level(logging.INFO, logger=op.log.name):
            caplog.clear()
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
            assert (
                f"Poking for tasks ['{TEST_TASK_ID}'] in dag {TEST_DAG_ID} on {DEFAULT_DATE.isoformat()} ... "
            ) in caplog.messages

    def test_external_task_sensor_external_task_ids_param(self, caplog):
        """Test external_task_ids rendering when a template is passed."""
        self.add_time_sensor()
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id="{{ params.dag_id }}",
            external_task_ids=["{{ params.task_id }}"],
            params={"dag_id": TEST_DAG_ID, "task_id": TEST_TASK_ID},
            dag=self.dag,
        )

        with caplog.at_level(logging.INFO, logger=op.log.name):
            caplog.clear()
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
            assert (
                f"Poking for tasks ['{TEST_TASK_ID}'] in dag {TEST_DAG_ID} on {DEFAULT_DATE.isoformat()} ... "
            ) in caplog.messages

    def test_external_task_sensor_failed_states_as_success_mulitple_task_ids(self, caplog):
        self.add_time_sensor(task_id=TEST_TASK_ID)
        self.add_time_sensor(task_id=TEST_TASK_ID_ALTERNATE)
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check_task_ids",
            external_dag_id=TEST_DAG_ID,
            external_task_ids=[TEST_TASK_ID, TEST_TASK_ID_ALTERNATE],
            allowed_states=["failed"],
            failed_states=["success"],
            dag=self.dag,
        )
        error_message = (
            rf"Some of the external tasks \['{TEST_TASK_ID}'\, \'{TEST_TASK_ID_ALTERNATE}\'] "
            rf"in DAG {TEST_DAG_ID} failed\."
        )
        with caplog.at_level(logging.INFO, logger=op.log.name):
            caplog.clear()
            with pytest.raises(ExternalTaskFailedError, match=error_message):
                op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        assert (
            f"Poking for tasks ['{TEST_TASK_ID}', '{TEST_TASK_ID_ALTERNATE}'] "
            f"in dag unit_test_dag on {DEFAULT_DATE.isoformat()} ... "
        ) in caplog.messages

    def test_external_dag_sensor(self, dag_maker):
        with dag_maker("other_dag", default_args=self.args, end_date=DEFAULT_DATE, schedule="@once"):
            pass
        dag_maker.create_dagrun(state=DagRunState.SUCCESS)

        op = ExternalTaskSensor(
            task_id="test_external_dag_sensor_check",
            external_dag_id="other_dag",
            external_task_id=None,
            dag=self.dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_dag_sensor_log(self, caplog, dag_maker):
        with dag_maker("other_dag", default_args=self.args, end_date=DEFAULT_DATE, schedule="@once"):
            pass
        dag_maker.create_dagrun(state=DagRunState.SUCCESS)
        op = ExternalTaskSensor(
            task_id="test_external_dag_sensor_check",
            external_dag_id="other_dag",
            dag=self.dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        assert (f"Poking for DAG 'other_dag' on {DEFAULT_DATE.isoformat()} ... ") in caplog.messages

    def test_external_dag_sensor_soft_fail_as_skipped(self, dag_maker, session):
        with dag_maker("other_dag", default_args=self.args, end_date=DEFAULT_DATE, schedule="@once"):
            pass
        dag_maker.create_dagrun(state=DagRunState.SUCCESS)
        op = ExternalTaskSensor(
            task_id="test_external_dag_sensor_check",
            external_dag_id="other_dag",
            external_task_id=None,
            allowed_states=[State.FAILED],
            failed_states=[State.SUCCESS],
            soft_fail=True,
            dag=self.dag,
        )

        # when
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # then
        task_instances: list[TI] = session.query(TI).filter(TI.task_id == op.task_id).all()
        assert len(task_instances) == 1, "Unexpected number of task instances"
        assert task_instances[0].state == State.SKIPPED, "Unexpected external task state"

    def test_external_task_sensor_fn_multiple_logical_dates(self):
        bash_command_code = """
{% set s=logical_date.time().second %}
echo "second is {{ s }}"
if [[ $(( {{ s }} % 60 )) == 1 ]]
    then
        exit 1
fi
exit 0
"""
        dag_external_id = TEST_DAG_ID + "_external"
        dag_external = DAG(dag_external_id, default_args=self.args, schedule=timedelta(seconds=1))
        task_external_with_failure = BashOperator(
            task_id="task_external_with_failure", bash_command=bash_command_code, retries=0, dag=dag_external
        )
        task_external_without_failure = EmptyOperator(
            task_id="task_external_without_failure", retries=0, dag=dag_external
        )

        task_external_without_failure.run(
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE + timedelta(seconds=1), ignore_ti_state=True
        )

        session = settings.Session()
        try:
            task_external_with_failure.run(
                start_date=DEFAULT_DATE, end_date=DEFAULT_DATE + timedelta(seconds=1), ignore_ti_state=True
            )
            # The test_with_failure task is excepted to fail
            # once per minute (the run on the first second of
            # each minute).
        except Exception as e:
            failed_tis = (
                session.query(TI)
                .filter(
                    TI.dag_id == dag_external_id,
                    TI.state == State.FAILED,
                    TI.execution_date == DEFAULT_DATE + timedelta(seconds=1),
                )
                .all()
            )
            if len(failed_tis) == 1 and failed_tis[0].task_id == "task_external_with_failure":
                pass
            else:
                raise e

        dag_id = TEST_DAG_ID
        dag = DAG(dag_id, default_args=self.args, schedule=timedelta(minutes=1))
        task_without_failure = ExternalTaskSensor(
            task_id="task_without_failure",
            external_dag_id=dag_external_id,
            external_task_id="task_external_without_failure",
            execution_date_fn=lambda dt: [dt + timedelta(seconds=i) for i in range(2)],
            allowed_states=["success"],
            retries=0,
            timeout=1,
            poke_interval=1,
            dag=dag,
        )
        task_with_failure = ExternalTaskSensor(
            task_id="task_with_failure",
            external_dag_id=dag_external_id,
            external_task_id="task_external_with_failure",
            execution_date_fn=lambda dt: [dt + timedelta(seconds=i) for i in range(2)],
            allowed_states=["success"],
            retries=0,
            timeout=1,
            poke_interval=1,
            dag=dag,
        )

        task_without_failure.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        with pytest.raises(AirflowSensorTimeout):
            task_with_failure.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # Test to ensure that if one task in a chain of tasks fails, the
        # ExternalTaskSensor will also report a failure and return without
        # waiting for a timeout.
        task_chain_with_failure = ExternalTaskSensor(
            task_id="task_chain_with_failure",
            external_dag_id=dag_external_id,
            external_task_id="task_external_with_failure",
            execution_date_fn=lambda dt: [dt + timedelta(seconds=i) for i in range(3)],
            allowed_states=["success"],
            failed_states=["failed"],
            retries=0,
            timeout=5,
            poke_interval=1,
            dag=dag,
        )

        # We need to test for an ExternalTaskFailedError explicitly since
        # AirflowSensorTimeout is a subclass that will be raised if this does
        # not execute properly.
        with pytest.raises(ExternalTaskFailedError) as ex_ctx:
            task_chain_with_failure.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        assert type(ex_ctx.value) is ExternalTaskFailedError

    def test_external_task_sensor_delta(self):
        self.add_time_sensor()
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check_delta",
            external_dag_id=TEST_DAG_ID,
            external_task_id=TEST_TASK_ID,
            execution_delta=timedelta(0),
            allowed_states=["success"],
            dag=self.dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_sensor_fn(self):
        self.add_time_sensor()
        # check that the execution_fn works
        op1 = ExternalTaskSensor(
            task_id="test_external_task_sensor_check_delta_1",
            external_dag_id=TEST_DAG_ID,
            external_task_id=TEST_TASK_ID,
            execution_date_fn=lambda dt: dt + timedelta(0),
            allowed_states=["success"],
            dag=self.dag,
        )
        op1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        # double check that the execution is being called by failing the test
        op2 = ExternalTaskSensor(
            task_id="test_external_task_sensor_check_delta_2",
            external_dag_id=TEST_DAG_ID,
            external_task_id=TEST_TASK_ID,
            execution_date_fn=lambda dt: dt + timedelta(days=1),
            allowed_states=["success"],
            timeout=1,
            poke_interval=1,
            dag=self.dag,
        )
        with pytest.raises(AirflowSensorTimeout):
            op2.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_sensor_fn_multiple_args(self):
        """Check this task sensor passes multiple args with full context. If no failure, means clean run."""
        self.add_time_sensor()

        def my_func(dt, context):
            assert context["logical_date"] == dt
            return dt + timedelta(0)

        op1 = ExternalTaskSensor(
            task_id="test_external_task_sensor_multiple_arg_fn",
            external_dag_id=TEST_DAG_ID,
            external_task_id=TEST_TASK_ID,
            execution_date_fn=my_func,
            allowed_states=["success"],
            dag=self.dag,
        )
        op1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_sensor_fn_kwargs(self):
        """Check this task sensor passes multiple args with full context. If no failure, means clean run."""
        self.add_time_sensor()

        def my_func(dt, ds_nodash):
            assert ds_nodash == dt.strftime("%Y%m%d")
            return dt + timedelta(0)

        op1 = ExternalTaskSensor(
            task_id="test_external_task_sensor_fn_kwargs",
            external_dag_id=TEST_DAG_ID,
            external_task_id=TEST_TASK_ID,
            execution_date_fn=my_func,
            allowed_states=["success"],
            dag=self.dag,
        )
        op1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_sensor_error_delta_and_fn(self):
        self.add_time_sensor()
        # Test that providing execution_delta and a function raises an error
        with pytest.raises(
            ValueError,
            match="Only one of `execution_delta` or `execution_date_fn` may be provided to ExternalTaskSensor; not both.",
        ):
            ExternalTaskSensor(
                task_id="test_external_task_sensor_check_delta",
                external_dag_id=TEST_DAG_ID,
                external_task_id=TEST_TASK_ID,
                execution_delta=timedelta(0),
                execution_date_fn=lambda dt: dt,
                allowed_states=["success"],
                dag=self.dag,
            )

    def test_external_task_sensor_error_task_id_and_task_ids(self):
        self.add_time_sensor()
        # Test that providing execution_delta and a function raises an error
        with pytest.raises(
            ValueError,
            match=EXTERNAL_ID_AND_IDS_PROVIDE_ERROR,
        ):
            ExternalTaskSensor(
                task_id="test_external_task_sensor_task_id_and_task_ids",
                external_dag_id=TEST_DAG_ID,
                external_task_id=TEST_TASK_ID,
                external_task_ids=[TEST_TASK_ID],
                allowed_states=["success"],
                dag=self.dag,
            )

    def test_external_task_sensor_with_xcom_arg_does_not_fail_on_init(self):
        self.add_time_sensor()
        op1 = MockOperator(task_id="op1", dag=self.dag)
        op2 = ExternalTaskSensor(
            task_id="test_external_task_sensor_with_xcom_arg_does_not_fail_on_init",
            external_dag_id=TEST_DAG_ID,
            external_task_ids=XComArg(op1),
            allowed_states=["success"],
            dag=self.dag,
        )
        assert isinstance(op2.external_task_ids, XComArg)

    def test_catch_duplicate_task_ids(self):
        self.add_time_sensor()
        op1 = ExternalTaskSensor(
            task_id="test_external_task_duplicate_task_ids",
            external_dag_id=TEST_DAG_ID,
            external_task_ids=[TEST_TASK_ID, TEST_TASK_ID],
            allowed_states=["success"],
            dag=self.dag,
        )
        with pytest.raises(ValueError, match="Duplicate task_ids"):
            op1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_catch_duplicate_task_ids_with_xcom_arg(self):
        self.add_time_sensor()
        op1 = PythonOperator(
            python_callable=lambda: ["dupe_value", "dupe_value"],
            task_id="op1",
            do_xcom_push=True,
            dag=self.dag,
        )

        op2 = ExternalTaskSensor(
            task_id="test_external_task_duplicate_task_ids_with_xcom_arg",
            external_dag_id=TEST_DAG_ID,
            external_task_ids=XComArg(op1),
            allowed_states=["success"],
            dag=self.dag,
        )
        op1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        with pytest.raises(ValueError, match="Duplicate task_ids"):
            op2.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_catch_duplicate_task_ids_with_multiple_xcom_args(self):
        self.add_time_sensor()

        op1 = PythonOperator(
            python_callable=lambda: "value",
            task_id="op1",
            do_xcom_push=True,
            dag=self.dag,
        )

        op2 = ExternalTaskSensor(
            task_id="test_external_task_duplicate_task_ids_with_xcom_arg",
            external_dag_id=TEST_DAG_ID,
            external_task_ids=[XComArg(op1), XComArg(op1)],
            allowed_states=["success"],
            dag=self.dag,
        )
        op1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        with pytest.raises(ValueError, match="Duplicate task_ids"):
            op2.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_catch_invalid_allowed_states(self):
        with pytest.raises(
            ValueError,
            match="Valid values for `allowed_states`, `skipped_states` and `failed_states`",
        ):
            ExternalTaskSensor(
                task_id="test_external_task_sensor_check_1",
                external_dag_id=TEST_DAG_ID,
                external_task_id=TEST_TASK_ID,
                allowed_states=["invalid_state"],
                dag=self.dag,
            )

        with pytest.raises(
            ValueError,
            match="Valid values for `allowed_states`, `skipped_states` and `failed_states`",
        ):
            ExternalTaskSensor(
                task_id="test_external_task_sensor_check_2",
                external_dag_id=TEST_DAG_ID,
                external_task_id=None,
                allowed_states=["invalid_state"],
                dag=self.dag,
            )

    def test_external_task_sensor_waits_for_task_check_existence(self):
        self.add_time_sensor()
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id=TEST_DAG_ID,
            external_task_id="non-existing-task",
            check_existence=True,
            dag=self.dag,
        )

        with pytest.raises(ExternalDagNotFoundError):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_sensor_waits_for_dag_check_existence(self):
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id="non-existing-dag",
            external_task_id=None,
            check_existence=True,
            dag=self.dag,
        )

        with pytest.raises(ExternalDagNotFoundError):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_group_with_mapped_tasks_sensor_success(self):
        self.add_time_sensor()
        self.add_fake_task_group_with_dynamic_tasks()
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id=TEST_DAG_ID,
            external_task_group_id=TEST_TASK_GROUP_ID,
            failed_states=[State.FAILED],
            dag=self.dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_group_with_mapped_tasks_failed_states(self):
        self.add_time_sensor()
        self.add_fake_task_group_with_dynamic_tasks(State.FAILED)
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id=TEST_DAG_ID,
            external_task_group_id=TEST_TASK_GROUP_ID,
            failed_states=[State.FAILED],
            dag=self.dag,
        )
        with pytest.raises(
            ExternalTaskGroupFailedError,
            match=f"The external task_group '{TEST_TASK_GROUP_ID}' in DAG '{TEST_DAG_ID}' failed.",
        ):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_group_when_there_is_no_TIs(self):
        """Test that the sensor does not fail when there are no TIs to check."""
        self.add_time_sensor()
        self.add_fake_task_group_with_dynamic_tasks(State.FAILED)
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id=TEST_DAG_ID,
            external_task_group_id=TEST_TASK_GROUP_ID,
            failed_states=[State.FAILED],
            dag=self.dag,
            poke_interval=1,
            timeout=3,
        )
        with pytest.raises(AirflowSensorTimeout):
            op.run(
                start_date=DEFAULT_DATE + timedelta(hours=1),
                end_date=DEFAULT_DATE + timedelta(hours=1),
                ignore_ti_state=True,
            )

    @pytest.mark.parametrize(
        ("kwargs", "expected_message"),
        (
            (
                {
                    "external_task_ids": [TEST_TASK_ID, TEST_TASK_ID_ALTERNATE],
                    "failed_states": [State.FAILED],
                },
                f"Some of the external tasks {re.escape(str([TEST_TASK_ID, TEST_TASK_ID_ALTERNATE]))}"
                f" in DAG {TEST_DAG_ID} failed.",
            ),
            (
                {
                    "external_task_group_id": [TEST_TASK_ID, TEST_TASK_ID_ALTERNATE],
                    "failed_states": [State.FAILED],
                },
                f"The external task_group '{re.escape(str([TEST_TASK_ID, TEST_TASK_ID_ALTERNATE]))}'"
                f" in DAG '{TEST_DAG_ID}' failed.",
            ),
            (
                {"failed_states": [State.FAILED]},
                f"The external DAG {TEST_DAG_ID} failed.",
            ),
        ),
    )
    @pytest.mark.parametrize(
        ("soft_fail", "expected_exception"),
        (
            (
                False,
                ExternalTaskFailedError,
            ),
            (
                True,
                AirflowSkipException,
            ),
        ),
    )
    @mock.patch("airflow.providers.standard.sensors.external_task.ExternalTaskSensor.get_count")
    @mock.patch("airflow.providers.standard.sensors.external_task.ExternalTaskSensor._get_dttm_filter")
    def test_fail_poke(
        self, _get_dttm_filter, get_count, soft_fail, expected_exception, kwargs, expected_message
    ):
        _get_dttm_filter.return_value = [DEFAULT_DATE]
        get_count.return_value = 1
        op = ExternalTaskSensor(
            task_id="test_external_task_duplicate_task_ids",
            external_dag_id=TEST_DAG_ID,
            allowed_states=["success"],
            dag=self.dag,
            soft_fail=soft_fail,
            deferrable=False,
            **kwargs,
        )
        assert op.external_dates_filter is None

        # We need to handle the specific exception types based on kwargs
        if not soft_fail:
            expected_exc = expected_exception
            if "external_task_ids" in kwargs:
                expected_exc = ExternalTaskFailedError
            elif "external_task_group_id" in kwargs:
                expected_exc = ExternalTaskGroupFailedError
            elif "failed_states" in kwargs and not any(
                k in kwargs for k in ["external_task_ids", "external_task_group_id"]
            ):
                expected_exc = ExternalDagFailedError

            with pytest.raises(expected_exc, match=expected_message):
                op.execute(context={})
        else:
            with pytest.raises(expected_exception, match=expected_message):
                op.execute(context={})

        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    @pytest.mark.parametrize(
        ("response_get_current", "response_exists", "kwargs", "expected_message"),
        (
            (None, None, {}, f"The external DAG {TEST_DAG_ID} does not exist."),
            (
                DAG(dag_id="test", schedule=None),
                False,
                {},
                f"The external DAG {TEST_DAG_ID} was deleted.",
            ),
            (
                DAG(dag_id="test", schedule=None),
                True,
                {"external_task_ids": [TEST_TASK_ID, TEST_TASK_ID_ALTERNATE]},
                f"The external task {TEST_TASK_ID} in DAG {TEST_DAG_ID} does not exist.",
            ),
            (
                DAG(dag_id="test", schedule=None),
                True,
                {"external_task_group_id": [TEST_TASK_ID, TEST_TASK_ID_ALTERNATE]},
                f"The external task group '{re.escape(str([TEST_TASK_ID, TEST_TASK_ID_ALTERNATE]))}'"
                f" in DAG '{TEST_DAG_ID}' does not exist.",
            ),
        ),
    )
    @pytest.mark.parametrize(
        ("soft_fail", "expected_exception"),
        (
            (
                False,
                ExternalDagNotFoundError,
            ),
            (
                True,
                ExternalDagNotFoundError,
            ),
        ),
    )
    @mock.patch("airflow.providers.standard.sensors.external_task.ExternalTaskSensor._get_dttm_filter")
    @mock.patch("airflow.models.dagbag.DagBag.get_dag")
    @mock.patch("os.path.exists")
    @mock.patch("airflow.models.dag.DagModel.get_current")
    def test_fail__check_for_existence(
        self,
        get_current,
        exists,
        get_dag,
        _get_dttm_filter,
        soft_fail,
        expected_exception,
        response_get_current,
        response_exists,
        kwargs,
        expected_message,
    ):
        _get_dttm_filter.return_value = []
        get_current.return_value = response_get_current
        exists.return_value = response_exists
        get_dag_response = mock.MagicMock()
        get_dag.return_value = get_dag_response
        get_dag_response.has_task.return_value = False
        get_dag_response.has_task_group.return_value = False
        op = ExternalTaskSensor(
            task_id="test_external_task_duplicate_task_ids",
            external_dag_id=TEST_DAG_ID,
            allowed_states=["success"],
            dag=self.dag,
            soft_fail=soft_fail,
            check_existence=True,
            **kwargs,
        )
        if not hasattr(op, "never_fail"):
            expected_message = "Skipping due to soft_fail is set to True." if soft_fail else expected_message
        specific_exception = expected_exception
        if response_get_current is None:
            specific_exception = ExternalDagNotFoundError
        elif not response_exists:
            specific_exception = ExternalDagDeletedError
        elif "external_task_ids" in kwargs:
            specific_exception = ExternalTaskNotFoundError
        elif "external_task_group_id" in kwargs:
            specific_exception = ExternalTaskGroupNotFoundError

        with pytest.raises(specific_exception, match=expected_message):
            op.execute(context={})

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_deferrable(self, dag_maker):
        context = {"execution_date": DEFAULT_DATE}
        with dag_maker() as dag:
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_id="test_task",
                deferrable=True,
                allowed_states=["success"],
            )
            dr = dag.create_dagrun(
                run_id="abcrhroceuh",
                run_type=DagRunType.MANUAL,
                state=None,
            )
            context.update(dag_run=dr, logical_date=DEFAULT_DATE)

        with pytest.raises(TaskDeferred) as exc:
            op.execute(context=context)
        assert isinstance(exc.value.trigger, WorkflowTrigger)
        assert exc.value.trigger.external_dag_id == "test_dag_parent"
        assert exc.value.trigger.external_task_ids == ["test_task"]
        assert exc.value.trigger.execution_dates == [DEFAULT_DATE]

    def test_get_logical_date(self):
        """For AF 2, we check for execution_date in context."""
        context = {"execution_date": DEFAULT_DATE}
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id="test_dag_parent",
            external_task_id="test_task",
        )
        assert op._get_logical_date(context) == DEFAULT_DATE

    def test_handle_execution_date_fn(self):
        def func(dt, context):
            assert context["execution_date"] == dt
            return dt + timedelta(0)

        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id="test_dag_parent",
            external_task_id="test_task",
            execution_date_fn=func,
        )
        context = {"execution_date": DEFAULT_DATE}
        assert op._handle_execution_date_fn(context) == DEFAULT_DATE


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Different test for AF 2")
@pytest.mark.usefixtures("testing_dag_bundle")
class TestExternalTaskSensorV3:
    def setup_method(self):
        # Create a mock for TaskInstance with get_ti_count method
        mock_ti = mock.MagicMock()
        mock_ti.get_ti_count = mock.MagicMock(return_value=0)  # Default return value

        self.context = {
            "execution_date": DEFAULT_DATE,
            "logical_date": DEFAULT_DATE,
            "ti": mock_ti,
            "task": mock.MagicMock(),
            "run_id": "test_run_id",
        }

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_success(self, dag_maker):
        """Test that the sensor succeeds when the external task succeeds."""
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_id="test_external_task_sensor_success",
                allowed_states=["success"],
            )

        # Mimic DB response to get_ti_count as 1
        self.context["ti"].get_ti_count.return_value = 1

        op.execute(context=self.context)

        self.context["ti"].get_ti_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            states=["success"],
            task_ids=["test_external_task_sensor_success"],
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_failure(self, dag_maker):
        """Test that the sensor fails when the external task fails."""
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_id="test_external_task_sensor_failure",
                failed_states=[State.FAILED],
            )

        self.context["ti"].get_ti_count.return_value = 1

        with pytest.raises(ExternalTaskFailedError):
            op.execute(context=self.context)

        self.context["ti"].get_ti_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            states=[State.FAILED],
            task_ids=["test_external_task_sensor_failure"],
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_soft_fail(self, dag_maker):
        """Test that the sensor skips when soft_fail is True and external task fails."""
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_id="test_external_task_sensor_soft_fail",
                failed_states=[State.FAILED],
                soft_fail=True,
            )

        self.context["ti"].get_ti_count.return_value = 1

        with pytest.raises(AirflowSkipException):
            op.execute(context=self.context)

        self.context["ti"].get_ti_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            states=[State.FAILED],
            task_ids=["test_external_task_sensor_soft_fail"],
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_multiple_task_ids(self, dag_maker):
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_ids=["task1", "task2"],
                allowed_states=["success"],
            )

        self.context["ti"].get_ti_count.return_value = 2
        op.execute(context=self.context)

        self.context["ti"].get_ti_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            states=["success"],
            task_ids=["task1", "task2"],
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_skipped_states(self, dag_maker):
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_id="test_external_task_sensor_skipped_states",
                skipped_states=[State.SKIPPED],
            )

        self.context["ti"].get_ti_count.return_value = 1
        with pytest.raises(AirflowSkipException):
            op.execute(context=self.context)

        self.context["ti"].get_ti_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            states=[State.SKIPPED],
            task_ids=["test_external_task_sensor_skipped_states"],
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    def test_external_task_sensor_invalid_combination(self, dag_maker):
        """Test that the sensor raises an error with invalid parameter combinations."""
        with pytest.raises(
            ValueError,
            match=EXTERNAL_ID_AND_IDS_PROVIDE_ERROR,
        ):
            with dag_maker("test_external_task_sensor_invalid_combination"):
                ExternalTaskSensor(
                    task_id="test_external_task_sensor_check",
                    external_dag_id="test_dag",
                    external_task_id="test_task",
                    external_task_ids=["test_task"],
                )

    def test_external_task_sensor_invalid_state(self, dag_maker):
        with pytest.raises(
            ValueError,
            match="Valid values for `allowed_states`, `skipped_states` and `failed_states` when `external_task_id` or `external_task_ids` or `external_task_group_id` is not `None",
        ):
            with dag_maker("test_external_task_sensor_invalid_state"):
                ExternalTaskSensor(
                    task_id="test_external_task_sensor_check",
                    external_dag_id="test_dag",
                    external_task_id="test_task",
                    allowed_states=["invalid_state"],
                )

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_task_group(self, dag_maker):
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_group_id="test_group",
                allowed_states=["success"],
            )

        self.context["ti"].get_task_states.return_value = {"run_id": {"test_group.task_id": State.SUCCESS}}
        op.execute(context=self.context)

        self.context["ti"].get_task_states.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            task_group_id="test_group",
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_execution_date_fn(self, dag_maker):
        def execution_date_fn(dt):
            return [dt + timedelta(hours=1)]

        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_id="test_task",
                execution_date_fn=execution_date_fn,
                allowed_states=["success"],
            )

        self.context["ti"].get_ti_count.return_value = 1
        op.execute(context=self.context)

        expected_date = DEFAULT_DATE + timedelta(hours=1)
        self.context["ti"].get_ti_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[expected_date],
            states=["success"],
            task_ids=["test_task"],
        )
        assert op.external_dates_filter == expected_date.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_execution_delta(self, dag_maker):
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_id="test_task",
                execution_delta=timedelta(hours=1),
                allowed_states=["success"],
            )

        self.context["ti"].get_ti_count.return_value = 1
        op.execute(context=self.context)

        expected_date = DEFAULT_DATE - timedelta(hours=1)
        self.context["ti"].get_ti_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[expected_date],
            states=["success"],
            task_ids=["test_task"],
        )
        assert op.external_dates_filter == expected_date.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_duplicate_task_ids(self, dag_maker):
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_ids=["task1", "task1"],
                allowed_states=["success"],
            )

        with pytest.raises(ValueError, match="Duplicate task_ids passed in external_task_ids parameter"):
            op.execute(context=self.context)

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_deferrable(self, dag_maker):
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_id="test_task",
                deferrable=True,
                allowed_states=["success"],
            )

        with pytest.raises(TaskDeferred) as exc:
            op.execute(context=self.context)

        assert isinstance(exc.value.trigger, WorkflowTrigger)
        assert exc.value.trigger.external_dag_id == "test_dag_parent"
        assert exc.value.trigger.external_task_ids == ["test_task"]
        assert exc.value.trigger.logical_dates == [DEFAULT_DATE]

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_only_dag_id(self, dag_maker):
        """Test that the sensor works correctly when only external_dag_id is provided."""
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                allowed_states=["success"],
            )

        self.context["ti"].get_dr_count = mock.MagicMock(return_value=1)

        op.execute(context=self.context)

        self.context["ti"].get_dr_count.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            states=["success"],
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    @pytest.mark.execution_timeout(10)
    def test_external_task_sensor_task_group_failed_states(self, dag_maker):
        with dag_maker("test_dag_child"):
            op = ExternalTaskSensor(
                task_id="test_external_task_sensor_check",
                external_dag_id="test_dag_parent",
                external_task_group_id="test_group",
                failed_states=[State.FAILED],
            )

        self.context["ti"].get_task_states.return_value = {"run_id": {"test_group.task_id": State.FAILED}}

        with pytest.raises(ExternalTaskGroupFailedError):
            op.execute(context=self.context)

        self.context["ti"].get_task_states.assert_called_once_with(
            dag_id="test_dag_parent",
            logical_dates=[DEFAULT_DATE],
            task_group_id="test_group",
        )
        assert op.external_dates_filter == DEFAULT_DATE.isoformat()

    def test_get_logical_date(self):
        """For AF 3, we check for logical date or dag_run.run_after  in context."""

        context = {"logical_date": DEFAULT_DATE}
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id="test_dag_parent",
            external_task_id="test_task",
        )
        assert op._get_logical_date(context) == DEFAULT_DATE

    def test_get_logical_date_with_dag_run_after(self):
        """For AF 3, we check for logical date or dag_run.run_after  in context."""
        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id="test_dag_parent",
            external_task_id="test_task",
        )
        mock_dag_run = mock.MagicMock()
        mock_dag_run.run_after = DEFAULT_DATE
        context = {"dag_run": mock_dag_run}
        assert op._get_logical_date(context) == DEFAULT_DATE

    def test_handle_execution_date_fn(self):
        def func(dt, context):
            assert context["logical_date"] == dt
            return dt + timedelta(0)

        op = ExternalTaskSensor(
            task_id="test_external_task_sensor_check",
            external_dag_id="test_dag_parent",
            external_task_id="test_task",
            execution_date_fn=func,
        )
        context = {"logical_date": DEFAULT_DATE}
        assert op._handle_execution_date_fn(context) == DEFAULT_DATE


class TestExternalTaskAsyncSensor:
    TASK_ID = "external_task_sensor_check"
    EXTERNAL_DAG_ID = "child_dag"  # DAG the external task sensor is waiting on
    EXTERNAL_TASK_ID = "child_task"  # Task the external task sensor is waiting on

    def test_defer_and_fire_task_state_trigger(self):
        """
        Asserts that a task is deferred and TaskStateTrigger will be fired
        when the ExternalTaskAsyncSensor is provided with all required arguments
        (i.e. including the external_task_id).
        """
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
        )

        context = {"execution_date": DEFAULT_DATE, "logical_date": DEFAULT_DATE}
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context=context)

        assert isinstance(exc.value.trigger, WorkflowTrigger), "Trigger is not a WorkflowTrigger"

    def test_defer_and_fire_failed_state_trigger(self):
        """Tests that an ExternalTaskNotFoundError is raised in case of error event"""
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
        )

        context = {"execution_date": DEFAULT_DATE, "logical_date": DEFAULT_DATE}
        with pytest.raises(ExternalTaskNotFoundError):
            sensor.execute_complete(
                context=context, event={"status": "error", "message": "test failure message"}
            )

    def test_defer_and_fire_timeout_state_trigger(self):
        """Tests that an ExternalTaskNotFoundError is raised in case of timeout event"""
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
        )

        context = {"execution_date": DEFAULT_DATE, "logical_date": DEFAULT_DATE}
        with pytest.raises(ExternalTaskNotFoundError):
            sensor.execute_complete(
                context=context,
                event={"status": "timeout", "message": "Dag was not started within 1 minute, assuming fail."},
            )

    def test_defer_execute_check_correct_logging(self):
        """Asserts that logging occurs as expected"""
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
        )

        context = {"execution_date": DEFAULT_DATE, "logical_date": DEFAULT_DATE}
        with mock.patch.object(sensor.log, "info") as mock_log_info:
            sensor.execute_complete(
                context=context,
                event={"status": "success"},
            )
        mock_log_info.assert_called_with("External tasks %s has executed successfully.", [EXTERNAL_TASK_ID])

    def test_defer_execute_check_failed_status(self):
        """Tests that the execute_complete method properly handles the 'failed' status from WorkflowTrigger"""
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
        )

        context = {"execution_date": DEFAULT_DATE, "logical_date": DEFAULT_DATE}
        with pytest.raises(ExternalDagFailedError, match="External job has failed."):
            sensor.execute_complete(
                context=context,
                event={"status": "failed"},
            )

    def test_defer_execute_check_failed_status_soft_fail(self):
        """Tests that the execute_complete method properly handles the 'failed' status with soft_fail=True"""
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
            soft_fail=True,
        )

        context = {"execution_date": DEFAULT_DATE, "logical_date": DEFAULT_DATE}
        with pytest.raises(AirflowSkipException, match="External job has failed skipping."):
            sensor.execute_complete(
                context=context,
                event={"status": "failed"},
            )

    def test_defer_with_failed_states(self):
        """Tests that failed_states are properly passed to the WorkflowTrigger when the sensor is deferred"""
        failed_states = ["failed", "upstream_failed"]
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
            failed_states=failed_states,
        )

        context = {"execution_date": datetime(2025, 1, 1), "logical_date": datetime(2025, 1, 1)}
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context=context)

        trigger = exc.value.trigger
        assert isinstance(trigger, WorkflowTrigger), "Trigger is not a WorkflowTrigger"
        assert trigger.failed_states == failed_states, "failed_states not properly passed to WorkflowTrigger"

    def test_defer_execute_complete_re_sets_external_dates_filter_attr(self):
        sensor = ExternalTaskSensor(
            task_id=TASK_ID,
            external_task_id=EXTERNAL_TASK_ID,
            external_dag_id=EXTERNAL_DAG_ID,
            deferrable=True,
        )
        assert sensor.external_dates_filter is None

        context = {"execution_date": DEFAULT_DATE, "logical_date": DEFAULT_DATE}
        sensor.execute_complete(context=context, event={"status": "success"})

        assert sensor.external_dates_filter == DEFAULT_DATE.isoformat()


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Needs Flask app context fixture for AF 2")
@pytest.mark.parametrize(
    argnames=("external_dag_id", "external_task_id", "expected_external_dag_id", "expected_external_task_id"),
    argvalues=[
        ("dag_test", "task_test", "dag_test", "task_test"),
        ("dag_{{ ds }}", "task_{{ ds }}", f"dag_{DEFAULT_DATE.date()}", f"task_{DEFAULT_DATE.date()}"),
    ],
    ids=["not_templated", "templated"],
)
def test_external_task_sensor_extra_link(
    external_dag_id,
    external_task_id,
    expected_external_dag_id,
    expected_external_task_id,
    create_task_instance_of_operator,
):
    ti = create_task_instance_of_operator(
        ExternalTaskSensor,
        dag_id="external_task_sensor_extra_links_dag",
        logical_date=DEFAULT_DATE,
        task_id="external_task_sensor_extra_links_task",
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
    )
    ti.render_templates()

    assert ti.task.external_dag_id == expected_external_dag_id
    assert ti.task.external_task_id == expected_external_task_id
    assert ti.task.external_task_ids == [expected_external_task_id]

    url = ti.task.operator_extra_links[0].get_link(operator=ti.task, ti_key=ti.key)

    assert f"/dags/{expected_external_dag_id}/runs" in url


class TestExternalTaskMarker:
    def test_serialized_fields(self):
        assert {"recursion_depth"}.issubset(ExternalTaskMarker.get_serialized_fields())

    def test_serialized_external_task_marker(self):
        dag = DAG("test_serialized_external_task_marker", schedule=None, start_date=DEFAULT_DATE)
        task = ExternalTaskMarker(
            task_id="parent_task",
            external_dag_id="external_task_marker_child",
            external_task_id="child_task1",
            dag=dag,
        )

        serialized_op = SerializedBaseOperator.serialize_operator(task)
        deserialized_op = SerializedBaseOperator.deserialize_operator(serialized_op)
        assert deserialized_op.task_type == "ExternalTaskMarker"
        assert getattr(deserialized_op, "external_dag_id") == "external_task_marker_child"
        assert getattr(deserialized_op, "external_task_id") == "child_task1"


@pytest.fixture
def dag_bag_ext():
    """
    Create a DagBag with DAGs looking like this. The dotted lines represent external dependencies
    set up using ExternalTaskMarker and ExternalTaskSensor.

    dag_0:   task_a_0 >> task_b_0
                             |
                             |
    dag_1:                   ---> task_a_1 >> task_b_1
                                                  |
                                                  |
    dag_2:                                        ---> task_a_2 >> task_b_2
                                                                       |
                                                                       |
    dag_3:                                                             ---> task_a_3 >> task_b_3
    """
    clear_db_runs()

    dag_bag = DagBag(dag_folder=DEV_NULL, include_examples=False)

    dag_0 = DAG("dag_0", start_date=DEFAULT_DATE, schedule=None)
    task_a_0 = EmptyOperator(task_id="task_a_0", dag=dag_0)
    task_b_0 = ExternalTaskMarker(
        task_id="task_b_0", external_dag_id="dag_1", external_task_id="task_a_1", recursion_depth=3, dag=dag_0
    )
    task_a_0 >> task_b_0

    dag_1 = DAG("dag_1", start_date=DEFAULT_DATE, schedule=None)
    task_a_1 = ExternalTaskSensor(
        task_id="task_a_1", external_dag_id=dag_0.dag_id, external_task_id=task_b_0.task_id, dag=dag_1
    )
    task_b_1 = ExternalTaskMarker(
        task_id="task_b_1", external_dag_id="dag_2", external_task_id="task_a_2", recursion_depth=2, dag=dag_1
    )
    task_a_1 >> task_b_1

    dag_2 = DAG("dag_2", start_date=DEFAULT_DATE, schedule=None)
    task_a_2 = ExternalTaskSensor(
        task_id="task_a_2", external_dag_id=dag_1.dag_id, external_task_id=task_b_1.task_id, dag=dag_2
    )
    task_b_2 = ExternalTaskMarker(
        task_id="task_b_2", external_dag_id="dag_3", external_task_id="task_a_3", recursion_depth=1, dag=dag_2
    )
    task_a_2 >> task_b_2

    dag_3 = DAG("dag_3", start_date=DEFAULT_DATE, schedule=None)
    task_a_3 = ExternalTaskSensor(
        task_id="task_a_3", external_dag_id=dag_2.dag_id, external_task_id=task_b_2.task_id, dag=dag_3
    )
    task_b_3 = EmptyOperator(task_id="task_b_3", dag=dag_3)
    task_a_3 >> task_b_3

    for dag in [dag_0, dag_1, dag_2, dag_3]:
        if AIRFLOW_V_3_0_PLUS:
            dag_bag.bag_dag(dag=dag)
        else:
            dag_bag.bag_dag(dag=dag, root_dag=dag)

    yield dag_bag

    clear_db_runs()


@pytest.fixture
def dag_bag_parent_child():
    """
    Create a DagBag with two DAGs looking like this. task_1 of child_dag_1 on day 1 depends on
    task_0 of parent_dag_0 on day 1. Therefore, when task_0 of parent_dag_0 on day 1 and day 2
    are cleared, parent_dag_0 DagRuns need to be set to running on both days, but child_dag_1
    only needs to be set to running on day 1.

                   day 1   day 2

     parent_dag_0  task_0  task_0
                     |
                     |
                     v
     child_dag_1   task_1  task_1

    """
    clear_db_runs()

    dag_bag = DagBag(dag_folder=DEV_NULL, include_examples=False)

    day_1 = DEFAULT_DATE

    with DAG("parent_dag_0", start_date=day_1, schedule=None) as dag_0:
        task_0 = ExternalTaskMarker(
            task_id="task_0",
            external_dag_id="child_dag_1",
            external_task_id="task_1",
            logical_date=day_1.isoformat(),
            recursion_depth=3,
        )

    with DAG("child_dag_1", start_date=day_1, schedule=None) as dag_1:
        ExternalTaskSensor(
            task_id="task_1",
            external_dag_id=dag_0.dag_id,
            external_task_id=task_0.task_id,
            execution_date_fn=lambda logical_date: day_1 if logical_date == day_1 else [],
            mode="reschedule",
        )

    for dag in [dag_0, dag_1]:
        if AIRFLOW_V_3_0_PLUS:
            dag_bag.bag_dag(dag=dag)
        else:
            dag_bag.bag_dag(dag=dag, root_dag=dag)

    yield dag_bag

    clear_db_runs()


@provide_session
def run_tasks(
    dag_bag: DagBag,
    logical_date=DEFAULT_DATE,
    session=NEW_SESSION,
) -> tuple[dict[str, DagRun], dict[str, TaskInstance]]:
    """
    Run all tasks in the DAGs in the given dag_bag. Return the TaskInstance objects as a dict
    keyed by task_id.
    """
    runs: dict[str, DagRun] = {}
    tis: dict[str, TaskInstance] = {}

    for dag in dag_bag.dags.values():
        data_interval = DataInterval(coerce_datetime(logical_date), coerce_datetime(logical_date))
        if AIRFLOW_V_3_0_PLUS:
            scheduler_dag = create_scheduler_dag(dag)
            runs[dag.dag_id] = dagrun = scheduler_dag.create_dagrun(
                run_id=scheduler_dag.timetable.generate_run_id(
                    run_type=DagRunType.MANUAL,
                    run_after=logical_date,
                    data_interval=data_interval,
                ),
                logical_date=logical_date,
                data_interval=data_interval,
                run_after=logical_date,
                run_type=DagRunType.MANUAL,
                triggered_by=DagRunTriggeredByType.TEST,
                state=DagRunState.RUNNING,
                start_date=logical_date,
                session=session,
            )
        else:
            runs[dag.dag_id] = dagrun = dag.create_dagrun(  # type: ignore[attr-defined,call-arg]
                run_id=dag.timetable.generate_run_id(  # type: ignore[attr-defined,call-arg,union-attr]
                    run_type=DagRunType.MANUAL,
                    logical_date=logical_date,
                    data_interval=data_interval,
                ),
                execution_date=logical_date,
                data_interval=data_interval,
                run_type=DagRunType.MANUAL,
                state=DagRunState.RUNNING,
                start_date=logical_date,
                session=session,
            )
        # we use sorting by task_id here because for the test DAG structure of ours
        # this is equivalent to topological sort. It would not work in general case
        # but it works for our case because we specifically constructed test DAGS
        # in the way that those two sort methods are equivalent
        tasks = sorted(dagrun.task_instances, key=lambda ti: ti.task_id)
        for ti in tasks:
            ti.refresh_from_task(dag.get_task(ti.task_id))
            tis[ti.task_id] = ti
            ti.run(session=session)
            session.flush()
            session.merge(ti)
            assert_ti_state_equal(ti, State.SUCCESS)

    return runs, tis


def assert_ti_state_equal(task_instance, state):
    """
    Assert state of task_instances equals the given state.
    """
    task_instance.refresh_from_db()
    assert task_instance.state == state


@provide_session
def clear_tasks(
    dag_bag,
    dag,
    task,
    session,
    start_date=DEFAULT_DATE,
    end_date=DEFAULT_DATE,
    dry_run=False,
):
    """
    Clear the task and its downstream tasks recursively for the dag in the given dagbag.
    """
    partial: DAG = dag.partial_subset(task_ids_or_regex=[task.task_id], include_downstream=True)
    return partial.clear(
        start_date=start_date,
        end_date=end_date,
        dag_bag=dag_bag,
        dry_run=dry_run,
        session=session,
    )


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for 3.0+")
def test_external_task_marker_transitive(dag_bag_ext):
    """
    Test clearing tasks across DAGs.
    """
    _, tis = run_tasks(dag_bag_ext)
    dag_0 = dag_bag_ext.get_dag("dag_0")
    task_a_0 = dag_0.get_task("task_a_0")
    clear_tasks(dag_bag_ext, dag_0, task_a_0)
    ti_a_0 = tis["task_a_0"]
    ti_b_3 = tis["task_b_3"]
    assert_ti_state_equal(ti_a_0, State.NONE)
    assert_ti_state_equal(ti_b_3, State.NONE)


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for 3.0+")
@provide_session
def test_external_task_marker_clear_activate(dag_bag_parent_child, session):
    """
    Test clearing tasks across DAGs and make sure the right DagRuns are activated.
    """
    dag_bag = dag_bag_parent_child
    day_1 = DEFAULT_DATE
    day_2 = DEFAULT_DATE + timedelta(days=1)

    run_tasks(dag_bag, logical_date=day_1)
    run_tasks(dag_bag, logical_date=day_2)

    # Assert that dagruns of all the affected dags are set to SUCCESS before tasks are cleared.
    for dag, execution_date in itertools.product(dag_bag.dags.values(), [day_1, day_2]):
        dagrun = dag.get_dagrun(execution_date=execution_date, session=session)
        dagrun.set_state(State.SUCCESS)
    session.flush()

    dag_0 = dag_bag.get_dag("parent_dag_0")
    task_0 = dag_0.get_task("task_0")
    clear_tasks(dag_bag, dag_0, task_0, start_date=day_1, end_date=day_2, session=session)

    # Assert that dagruns of all the affected dags are set to QUEUED after tasks are cleared.
    # Unaffected dagruns should be left as SUCCESS.
    dagrun_0_1 = dag_bag.get_dag("parent_dag_0").get_dagrun(execution_date=day_1, session=session)
    dagrun_0_2 = dag_bag.get_dag("parent_dag_0").get_dagrun(execution_date=day_2, session=session)
    dagrun_1_1 = dag_bag.get_dag("child_dag_1").get_dagrun(execution_date=day_1, session=session)
    dagrun_1_2 = dag_bag.get_dag("child_dag_1").get_dagrun(execution_date=day_2, session=session)

    assert dagrun_0_1.state == State.QUEUED
    assert dagrun_0_2.state == State.QUEUED
    assert dagrun_1_1.state == State.QUEUED
    assert dagrun_1_2.state == State.SUCCESS


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for 3.0+")
def test_external_task_marker_future(dag_bag_ext):
    """
    Test clearing tasks with no end_date. This is the case when users clear tasks with
    Future, Downstream and Recursive selected.
    """
    date_0 = DEFAULT_DATE
    date_1 = DEFAULT_DATE + timedelta(days=1)

    _, tis_date_0 = run_tasks(dag_bag_ext, logical_date=date_0)
    _, tis_date_1 = run_tasks(dag_bag_ext, logical_date=date_1)

    dag_0 = dag_bag_ext.get_dag("dag_0")
    task_a_0 = dag_0.get_task("task_a_0")
    # This should clear all tasks on dag_0 to dag_3 on both date_0 and date_1
    clear_tasks(dag_bag_ext, dag_0, task_a_0, end_date=None)

    ti_a_0_date_0 = tis_date_0["task_a_0"]
    ti_b_3_date_0 = tis_date_0["task_b_3"]
    ti_b_3_date_1 = tis_date_1["task_b_3"]
    assert_ti_state_equal(ti_a_0_date_0, State.NONE)
    assert_ti_state_equal(ti_b_3_date_0, State.NONE)
    assert_ti_state_equal(ti_b_3_date_1, State.NONE)


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for 3.0+")
def test_external_task_marker_exception(dag_bag_ext):
    """
    Clearing across multiple DAGs should raise AirflowException if more levels are being cleared
    than allowed by the recursion_depth of the first ExternalTaskMarker being cleared.
    """
    run_tasks(dag_bag_ext)
    dag_0 = dag_bag_ext.get_dag("dag_0")
    task_a_0 = dag_0.get_task("task_a_0")
    task_b_0 = dag_0.get_task("task_b_0")
    task_b_0.recursion_depth = 2
    with pytest.raises(AirflowException, match="Maximum recursion depth 2"):
        clear_tasks(dag_bag_ext, dag_0, task_a_0)


@pytest.fixture
def dag_bag_cyclic():
    """
    Create a DagBag with DAGs having cyclic dependencies set up by ExternalTaskMarker and
    ExternalTaskSensor.

    dag_0:   task_a_0 >> task_b_0
                  ^          |
                  |          |
    dag_1:        |          ---> task_a_1 >> task_b_1
                  |                              ^
                  |                              |
    dag_n:        |                              ---> task_a_n >> task_b_n
                  |                                                   |
                  -----------------------------------------------------
    """

    def _factory(depth: int) -> DagBag:
        dag_bag = DagBag(dag_folder=DEV_NULL, include_examples=False)

        dags = []

        with DAG("dag_0", start_date=DEFAULT_DATE, schedule=None) as dag:
            dags.append(dag)
            task_a_0 = EmptyOperator(task_id="task_a_0")
            task_b_0 = ExternalTaskMarker(
                task_id="task_b_0", external_dag_id="dag_1", external_task_id="task_a_1", recursion_depth=3
            )
            task_a_0 >> task_b_0

        for n in range(1, depth):
            with DAG(f"dag_{n}", start_date=DEFAULT_DATE, schedule=None) as dag:
                dags.append(dag)
                task_a = ExternalTaskSensor(
                    task_id=f"task_a_{n}",
                    external_dag_id=f"dag_{n - 1}",
                    external_task_id=f"task_b_{n - 1}",
                )
                task_b = ExternalTaskMarker(
                    task_id=f"task_b_{n}",
                    external_dag_id=f"dag_{n + 1}",
                    external_task_id=f"task_a_{n + 1}",
                    recursion_depth=3,
                )
                task_a >> task_b

        # Create the last dag which loops back
        with DAG(f"dag_{depth}", start_date=DEFAULT_DATE, schedule=None) as dag:
            dags.append(dag)
            task_a = ExternalTaskSensor(
                task_id=f"task_a_{depth}",
                external_dag_id=f"dag_{depth - 1}",
                external_task_id=f"task_b_{depth - 1}",
            )
            task_b = ExternalTaskMarker(
                task_id=f"task_b_{depth}",
                external_dag_id="dag_0",
                external_task_id="task_a_0",
                recursion_depth=2,
            )
            task_a >> task_b

        for dag in dags:
            if AIRFLOW_V_3_0_PLUS:
                sync_dag_to_db(dag)
                dag_bag.bag_dag(dag=dag)
            else:
                dag_bag.bag_dag(dag=dag, root_dag=dag)  # type: ignore[call-arg]

        return dag_bag

    return _factory


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for 3.0+")
def test_external_task_marker_cyclic_deep(dag_bag_cyclic):
    """
    Tests clearing across multiple DAGs that have cyclic dependencies. AirflowException should be
    raised.
    """
    dag_bag = dag_bag_cyclic(10)
    run_tasks(dag_bag)
    dag_0 = dag_bag.get_dag("dag_0")
    task_a_0 = dag_0.get_task("task_a_0")
    with pytest.raises(AirflowException, match="Maximum recursion depth 3"):
        clear_tasks(dag_bag, dag_0, task_a_0)


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Different test for 3.0+")
def test_external_task_marker_cyclic_shallow(dag_bag_cyclic):
    """
    Tests clearing across multiple DAGs that have cyclic dependencies shallower
    than recursion_depth
    """
    dag_bag = dag_bag_cyclic(2)
    run_tasks(dag_bag)
    dag_0 = dag_bag.get_dag("dag_0")
    task_a_0 = dag_0.get_task("task_a_0")

    tis = clear_tasks(dag_bag, dag_0, task_a_0, dry_run=True)

    assert sorted((ti.dag_id, ti.task_id) for ti in tis) == [
        ("dag_0", "task_a_0"),
        ("dag_0", "task_b_0"),
        ("dag_1", "task_a_1"),
        ("dag_1", "task_b_1"),
        ("dag_2", "task_a_2"),
        ("dag_2", "task_b_2"),
    ]


@pytest.fixture
def dag_bag_multiple(session):
    """
    Create a DagBag containing two DAGs, linked by multiple ExternalTaskMarker.
    """
    dag_bag = DagBag(dag_folder=DEV_NULL, include_examples=False)
    daily_dag = DAG("daily_dag", start_date=DEFAULT_DATE, schedule="@daily")
    agg_dag = DAG("agg_dag", start_date=DEFAULT_DATE, schedule="@daily")
    if AIRFLOW_V_3_0_PLUS:
        dag_bag.bag_dag(dag=daily_dag)
        dag_bag.bag_dag(dag=agg_dag)
    else:
        dag_bag.bag_dag(dag=daily_dag, root_dag=daily_dag)
        dag_bag.bag_dag(dag=agg_dag, root_dag=agg_dag)

    daily_task = EmptyOperator(task_id="daily_tas", dag=daily_dag)

    begin = EmptyOperator(task_id="begin", dag=agg_dag)
    for i in range(8):
        task = ExternalTaskMarker(
            task_id=f"{daily_task.task_id}_{i}",
            external_dag_id=daily_dag.dag_id,
            external_task_id=daily_task.task_id,
            logical_date=f"{{{{ macros.ds_add(ds, -1 * {i}) }}}}",
            dag=agg_dag,
        )
        begin >> task

    if AIRFLOW_V_3_0_PLUS:
        sync_dags_to_db([agg_dag, daily_dag])

    return dag_bag


@pytest.fixture
def dag_bag_head_tail(session):
    """
    Create a DagBag containing one DAG, with task "head" depending on task "tail" of the
    previous logical_date.

    20200501     20200502                 20200510
    +------+     +------+                 +------+
    | head |    -->head |    -->         -->head |
    |  |   |   / |  |   |   /           / |  |   |
    |  v   |  /  |  v   |  /           /  |  v   |
    | body | /   | body | /     ...   /   | body |
    |  |   |/    |  |   |/           /    |  |   |
    |  v   /     |  v   /           /     |  v   |
    | tail/|     | tail/|          /      | tail |
    +------+     +------+                 +------+
    """
    dag_bag = DagBag(dag_folder=DEV_NULL, include_examples=False)

    with DAG("head_tail", start_date=DEFAULT_DATE, schedule="@daily") as dag:
        head = ExternalTaskSensor(
            task_id="head",
            external_dag_id=dag.dag_id,
            external_task_id="tail",
            execution_delta=timedelta(days=1),
            mode="reschedule",
        )
        body = EmptyOperator(task_id="body")
        tail = ExternalTaskMarker(
            task_id="tail",
            external_dag_id=dag.dag_id,
            external_task_id=head.task_id,
            logical_date="{{ macros.ds_add(ds, 1) }}",
        )
        head >> body >> tail

    if AIRFLOW_V_3_0_PLUS:
        dag_bag.bag_dag(dag)
        sync_dag_to_db(dag)
    else:
        dag_bag.bag_dag(dag=dag, root_dag=dag)

    return dag_bag


@pytest.fixture
def dag_bag_head_tail_mapped_tasks(session):
    """
    Create a DagBag containing one DAG, with task "head" depending on task "tail" of the
    previous logical_date.

    20200501     20200502                 20200510
    +------+     +------+                 +------+
    | head |    -->head |    -->         -->head |
    |  |   |   / |  |   |   /           / |  |   |
    |  v   |  /  |  v   |  /           /  |  v   |
    | body | /   | body | /     ...   /   | body |
    |  |   |/    |  |   |/           /    |  |   |
    |  v   /     |  v   /           /     |  v   |
    | tail/|     | tail/|          /      | tail |
    +------+     +------+                 +------+
    """
    dag_bag = DagBag(dag_folder=DEV_NULL, include_examples=False)

    with DAG("head_tail", start_date=DEFAULT_DATE, schedule="@daily") as dag:

        @task_deco
        def dummy_task(x: int):
            return x

        head = ExternalTaskSensor(
            task_id="head",
            external_dag_id=dag.dag_id,
            external_task_id="tail",
            execution_delta=timedelta(days=1),
            mode="reschedule",
        )

        body = dummy_task.expand(x=range(5))
        tail = ExternalTaskMarker(
            task_id="tail",
            external_dag_id=dag.dag_id,
            external_task_id=head.task_id,
            logical_date="{{ macros.ds_add(ds, 1) }}",
        )
        head >> body >> tail

    if AIRFLOW_V_3_0_PLUS:
        sync_dag_to_db(dag)
    else:
        dag_bag.bag_dag(dag=dag, root_dag=dag)

    return dag_bag
