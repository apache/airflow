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

import unittest

from airflow import models
from airflow.api.common.experimental.delete_dag import delete_dag
from airflow.exceptions import DagNotFound
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.session import create_session
from airflow.utils.state import State

DM = models.DagModel
DR = models.DagRun
TI = models.TaskInstance
LOG = models.log.Log
TF = models.taskfail.TaskFail
TR = models.taskreschedule.TaskReschedule
IE = models.ImportError


class TestDeleteDAGCatchError(unittest.TestCase):

    def setUp(self):
        self.dagbag = models.DagBag(include_examples=True)
        self.dag_id = 'example_bash_operator'
        self.dag = self.dagbag.dags[self.dag_id]

    def tearDown(self):
        self.dag.clear()

    def test_delete_dag_non_existent_dag(self):
        with self.assertRaises(DagNotFound):
            delete_dag("non-existent DAG")


class TestDeleteDAGSuccessfulDelete(unittest.TestCase):

    def setUp(self):
        self.key = "test_dag_id"
        self.dag_file_path = "/usr/local/airflow/dags/test_dag_8.py"

        task = DummyOperator(task_id='dummy',
                             dag=models.DAG(dag_id=self.key,
                                            default_args={'start_date': days_ago(2)}),
                             owner='airflow')

        test_date = days_ago(1)
        with create_session() as session:
            session.add(DM(dag_id=self.key, fileloc=self.dag_file_path))
            session.add(DR(dag_id=self.key))
            session.add(TI(task=task,
                           execution_date=test_date,
                           state=State.SUCCESS))
            # flush to ensure task instance if written before
            # task reschedule because of FK constraint
            session.flush()
            session.add(LOG(dag_id=self.key, task_id=None, task_instance=None,
                            execution_date=test_date, event="varimport"))
            session.add(TF(task=task, execution_date=test_date,
                           start_date=test_date, end_date=test_date))
            session.add(TR(task=task, execution_date=test_date,
                           start_date=test_date, end_date=test_date,
                           try_number=1, reschedule_date=test_date))
            session.add(IE(timestamp=test_date, filename=self.dag_file_path,
                           stacktrace="NameError: name 'airflow' is not defined"))

    def tearDown(self):
        with create_session() as session:
            session.query(TR).filter(TR.dag_id == self.key).delete()
            session.query(TF).filter(TF.dag_id == self.key).delete()
            session.query(TI).filter(TI.dag_id == self.key).delete()
            session.query(DR).filter(DR.dag_id == self.key).delete()
            session.query(DM).filter(DM.dag_id == self.key).delete()
            session.query(LOG).filter(LOG.dag_id == self.key).delete()
            session.query(IE).filter(IE.filename == self.dag_file_path).delete()

    def test_delete_dag_successful_delete(self):
        with create_session() as session:
            self.assertEqual(session.query(DM).filter(DM.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(DR).filter(DR.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(TI).filter(TI.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(TF).filter(TF.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(TR).filter(TR.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(LOG).filter(LOG.dag_id == self.key).count(), 1)
            self.assertEqual(
                session.query(IE).filter(IE.filename == self.dag_file_path).count(), 1)

        delete_dag(dag_id=self.key)

        with create_session() as session:
            self.assertEqual(session.query(DM).filter(DM.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(DR).filter(DR.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(TI).filter(TI.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(TF).filter(TF.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(TR).filter(TR.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(LOG).filter(LOG.dag_id == self.key).count(), 1)
            self.assertEqual(
                session.query(IE).filter(IE.filename == self.dag_file_path).count(), 0)

    def test_delete_dag_successful_delete_not_keeping_records_in_log(self):

        with create_session() as session:
            self.assertEqual(session.query(DM).filter(DM.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(DR).filter(DR.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(TI).filter(TI.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(TF).filter(TF.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(TR).filter(TR.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(LOG).filter(LOG.dag_id == self.key).count(), 1)
            self.assertEqual(
                session.query(IE).filter(IE.filename == self.dag_file_path).count(), 1)

        delete_dag(dag_id=self.key, keep_records_in_log=False)

        with create_session() as session:
            self.assertEqual(session.query(DM).filter(DM.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(DR).filter(DR.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(TI).filter(TI.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(TF).filter(TF.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(TR).filter(TR.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(LOG).filter(LOG.dag_id == self.key).count(), 0)
            self.assertEqual(
                session.query(IE).filter(IE.filename == self.dag_file_path).count(), 0)


if __name__ == '__main__':
    unittest.main()
