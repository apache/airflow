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

from airflow import models
from airflow.api.common.delete_dag import delete_dag
from airflow.exceptions import AirflowException, DagNotFound
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_dags, clear_db_runs

DM = models.DagModel
DR = models.DagRun
TI = models.TaskInstance
LOG = models.log.Log
TF = models.taskfail.TaskFail
TR = models.taskreschedule.TaskReschedule
IE = models.ImportError


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
        dag_id = 'test-dag'
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
            task_id='dummy',
            dag=models.DAG(dag_id=self.key, default_args={'start_date': timezone.datetime(2022, 1, 1)}),
            owner='airflow',
        )

        test_date = timezone.datetime(2022, 1, 1)
        with create_session() as session:
            session.add(DM(dag_id=self.key, fileloc=self.dag_file_path, is_subdag=for_sub_dag))
            dr = DR(dag_id=self.key, run_type=DagRunType.MANUAL, run_id="test", execution_date=test_date)
            ti = TI(task=task, state=State.SUCCESS)
            ti.dag_run = dr
            session.add_all((dr, ti))
            # flush to ensure task instance if written before
            # task reschedule because of FK constraint
            session.flush()
            session.add(
                LOG(
                    dag_id=self.key,
                    task_id=None,
                    task_instance=None,
                    execution_date=test_date,
                    event="varimport",
                )
            )
            session.add(TF(ti=ti))
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
        with create_session() as session:
            session.query(TR).filter(TR.dag_id == self.key).delete()
            session.query(TF).filter(TF.dag_id == self.key).delete()
            session.query(TI).filter(TI.dag_id == self.key).delete()
            session.query(DR).filter(DR.dag_id == self.key).delete()
            session.query(DM).filter(DM.dag_id == self.key).delete()
            session.query(LOG).filter(LOG.dag_id == self.key).delete()
            session.query(IE).filter(IE.filename == self.dag_file_path).delete()

    def check_dag_models_exists(self):
        with create_session() as session:
            assert session.query(DM).filter(DM.dag_id == self.key).count() == 1
            assert session.query(DR).filter(DR.dag_id == self.key).count() == 1
            assert session.query(TI).filter(TI.dag_id == self.key).count() == 1
            assert session.query(TF).filter(TF.dag_id == self.key).count() == 1
            assert session.query(TR).filter(TR.dag_id == self.key).count() == 1
            assert session.query(LOG).filter(LOG.dag_id == self.key).count() == 1
            assert session.query(IE).filter(IE.filename == self.dag_file_path).count() == 1

    def check_dag_models_removed(self, expect_logs=1):
        with create_session() as session:
            assert session.query(DM).filter(DM.dag_id == self.key).count() == 0
            assert session.query(DR).filter(DR.dag_id == self.key).count() == 0
            assert session.query(TI).filter(TI.dag_id == self.key).count() == 0
            assert session.query(TF).filter(TF.dag_id == self.key).count() == 0
            assert session.query(TR).filter(TR.dag_id == self.key).count() == 0
            assert session.query(LOG).filter(LOG.dag_id == self.key).count() == expect_logs
            assert session.query(IE).filter(IE.filename == self.dag_file_path).count() == 0

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
            session.add(DM(dag_id=self.key + ".other_dag", fileloc=self.dag_file_path))
            session.add(DM(dag_id=self.key + ".subdag", fileloc=self.dag_file_path, is_subdag=True))

        delete_dag(self.key)

        with create_session() as session:
            assert session.query(DM).filter(DM.dag_id == self.key + ".other_dag").count() == 1
            assert session.query(DM).filter(DM.dag_id.like(self.key + "%")).count() == 1
