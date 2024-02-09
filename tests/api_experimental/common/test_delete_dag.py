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

import pytest
from sqlalchemy import func, select

from airflow.api.common.delete_dag import delete_dag
from airflow.exceptions import AirflowException, DagNotFound
from airflow.models.dag import DAG, DagModel
from airflow.models.dagrun import DagRun as DR
from airflow.models.errors import ImportError as IE
from airflow.models.log import Log
from airflow.models.taskfail import TaskFail
from airflow.models.taskinstance import TaskInstance as TI
from airflow.models.taskreschedule import TaskReschedule as TR
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.db import (
    clear_db_dags,
    clear_db_import_errors,
    clear_db_logs,
    clear_db_runs,
    clear_db_task_fail,
)

pytestmark = pytest.mark.db_test


class TestDeleteDAGCatchError:
    def test_delete_dag_non_existent_dag(self):
        with pytest.raises(DagNotFound):
            delete_dag("non-existent DAG")


class TestDeleteDAGErrorsOnRunningTI:
    def setup_method(self):
        clear_db_dags()
        clear_db_runs()

    def teardown_method(self):
        clear_db_dags()
        clear_db_runs()

    def test_delete_dag_running_taskinstances(self, session, create_task_instance):
        dag_id = "test-dag"
        ti = create_task_instance(dag_id=dag_id, session=session)

        ti.state = State.RUNNING
        session.commit()
        with pytest.raises(AirflowException):
            delete_dag(dag_id)


class TestDeleteDAGSuccessfulDelete:
    dag_file_path = "/usr/local/airflow/dags/test_dag_8.py"
    key = "test_dag_id"

    def setup_dag_models(self, for_sub_dag=False):
        if for_sub_dag:
            self.key = "test_dag_id.test_subdag"

        task = EmptyOperator(
            task_id="dummy",
            dag=DAG(dag_id=self.key, default_args={"start_date": timezone.datetime(2022, 1, 1)}),
            owner="airflow",
        )

        test_date = timezone.datetime(2022, 1, 1)
        with create_session() as session:
            session.add(DagModel(dag_id=self.key, fileloc=self.dag_file_path, is_subdag=for_sub_dag))
            dr = DR(dag_id=self.key, run_type=DagRunType.MANUAL, run_id="test", execution_date=test_date)
            ti = TI(task=task, state=State.SUCCESS)
            ti.dag_run = dr
            session.add_all((dr, ti))
            # flush to ensure task instance if written before
            # task reschedule because of FK constraint
            session.flush()
            session.add(
                Log(
                    dag_id=self.key,
                    task_id=None,
                    task_instance=None,
                    execution_date=test_date,
                    event="varimport",
                )
            )
            session.add(TaskFail(ti=ti))
            session.add(
                TR(
                    task=ti.task,
                    run_id=ti.run_id,
                    start_date=test_date,
                    end_date=test_date,
                    try_number=1,
                    reschedule_date=test_date,
                )
            )
            session.add(
                IE(
                    timestamp=test_date,
                    filename=self.dag_file_path,
                    stacktrace="NameError: name 'airflow' is not defined",
                )
            )

    def teardown_method(self):
        clear_db_dags()
        clear_db_runs()
        clear_db_task_fail()
        clear_db_logs()
        clear_db_import_errors()

    def check_dag_models_exists(self):
        with create_session() as session:
            assert session.scalar(select(func.count()).where(DagModel.dag_id == self.key)) == 1
            assert session.scalar(select(func.count()).where(DR.dag_id == self.key)) == 1
            assert session.scalar(select(func.count()).where(TI.dag_id == self.key)) == 1
            assert session.scalar(select(func.count()).where(TaskFail.dag_id == self.key)) == 1
            assert session.scalar(select(func.count()).where(TR.dag_id == self.key)) == 1
            assert session.scalar(select(func.count()).where(Log.dag_id == self.key)) == 1
            assert session.scalar(select(func.count()).where(IE.filename == self.dag_file_path)) == 1

    def check_dag_models_removed(self, expect_logs=1):
        with create_session() as session:
            assert session.scalar(select(func.count()).where(DagModel.dag_id == self.key)) == 0
            assert session.scalar(select(func.count()).where(DR.dag_id == self.key)) == 0
            assert session.scalar(select(func.count()).where(TI.dag_id == self.key)) == 0
            assert session.scalar(select(func.count()).where(TaskFail.dag_id == self.key)) == 0
            assert session.scalar(select(func.count()).where(TR.dag_id == self.key)) == 0
            assert session.scalar(select(func.count()).where(Log.dag_id == self.key)) == expect_logs
            assert session.scalar(select(func.count()).where(IE.filename == self.dag_file_path)) == 0

    def test_delete_dag_successful_delete(self):
        self.setup_dag_models()
        self.check_dag_models_exists()
        delete_dag(dag_id=self.key)
        self.check_dag_models_removed(expect_logs=1)

    def test_delete_dag_successful_delete_not_keeping_records_in_log(self):
        self.setup_dag_models()
        self.check_dag_models_exists()
        delete_dag(dag_id=self.key, keep_records_in_log=False)
        self.check_dag_models_removed(expect_logs=0)

    def test_delete_subdag_successful_delete(self):
        self.setup_dag_models(for_sub_dag=True)
        self.check_dag_models_exists()
        delete_dag(dag_id=self.key, keep_records_in_log=False)
        self.check_dag_models_removed(expect_logs=0)

    def test_delete_dag_preserves_other_dags(self):
        self.setup_dag_models()

        with create_session() as session:
            session.add(DagModel(dag_id=self.key + ".other_dag", fileloc=self.dag_file_path))
            session.add(DagModel(dag_id=self.key + ".subdag", fileloc=self.dag_file_path, is_subdag=True))

        delete_dag(self.key)

        with create_session() as session:
            assert session.scalar(select(func.count()).where(DagModel.dag_id == f"{self.key}.other_dag")) == 1
            assert session.scalar(select(func.count()).where(DagModel.dag_id.like(f"{self.key}%"))) == 1
