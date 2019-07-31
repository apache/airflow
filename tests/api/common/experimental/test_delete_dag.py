# -*- coding: utf-8 -*-
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
from airflow.exceptions import DagNotFound, DagFileExists
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.db import create_session
from airflow.utils.state import State

DM = models.DagModel
DR = models.DagRun
TI = models.TaskInstance
LOG = models.log.Log
TF = models.TaskFail
TR = models.TaskReschedule


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

    def test_delete_dag_dag_still_in_dagbag(self):
        with create_session() as session:
            models_to_check = ['DagModel', 'DagRun', 'TaskInstance']
            record_counts = {}

            for model_name in models_to_check:
                m = getattr(models, model_name)
                record_counts[model_name] = session.query(m).filter(m.dag_id == self.dag_id).count()

            with self.assertRaises(DagFileExists):
                delete_dag(self.dag_id)

            # No change should happen in DB
            for model_name in models_to_check:
                m = getattr(models, model_name)
                self.assertEqual(
                    session.query(m).filter(
                        m.dag_id == self.dag_id
                    ).count(),
                    record_counts[model_name]
                )


class TestDeleteDAGSuccessfulDelete(unittest.TestCase):

    def setUp(self):
        self.key = "test_dag_id"

        task = DummyOperator(task_id='dummy',
                             dag=models.DAG(dag_id=self.key,
                                            default_args={'start_date': days_ago(2)}),
                             owner='airflow')

        d = days_ago(1)
        with create_session() as session:
            session.add(DM(dag_id=self.key))
            session.add(DR(dag_id=self.key))
            session.add(TI(task=task,
                           execution_date=d,
                           state=State.SUCCESS))
            # flush to ensure task instance if written before
            # task reschedule because of FK constraint
            session.flush()
            session.add(LOG(dag_id=self.key, task_id=None, task_instance=None,
                            execution_date=d, event="varimport"))
            session.add(TF(task=task, execution_date=d,
                           start_date=d, end_date=d))
            session.add(TR(task=task, execution_date=d,
                           start_date=d, end_date=d,
                           try_number=1, reschedule_date=d))

    def tearDown(self):
        with create_session() as session:
            session.query(TR).filter(TR.dag_id == self.key).delete()
            session.query(TF).filter(TF.dag_id == self.key).delete()
            session.query(TI).filter(TI.dag_id == self.key).delete()
            session.query(DR).filter(DR.dag_id == self.key).delete()
            session.query(DM).filter(DM.dag_id == self.key).delete()
            session.query(LOG).filter(LOG.dag_id == self.key).delete()

    def test_delete_dag_successful_delete(self):
        with create_session() as session:
            self.assertEqual(session.query(DM).filter(DM.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(DR).filter(DR.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(TI).filter(TI.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(TF).filter(TF.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(TR).filter(TR.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(LOG).filter(LOG.dag_id == self.key).count(), 1)

        delete_dag(dag_id=self.key)

        with create_session() as session:
            self.assertEqual(session.query(DM).filter(DM.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(DR).filter(DR.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(TI).filter(TI.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(TF).filter(TF.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(TR).filter(TR.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(LOG).filter(LOG.dag_id == self.key).count(), 1)

    def test_delete_dag_successful_delete_not_keeping_records_in_log(self):

        with create_session() as session:
            self.assertEqual(session.query(DM).filter(DM.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(DR).filter(DR.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(TI).filter(TI.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(TF).filter(TF.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(TR).filter(TR.dag_id == self.key).count(), 1)
            self.assertEqual(session.query(LOG).filter(LOG.dag_id == self.key).count(), 1)

        delete_dag(dag_id=self.key, keep_records_in_log=False)

        with create_session() as session:
            self.assertEqual(session.query(DM).filter(DM.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(DR).filter(DR.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(TI).filter(TI.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(TF).filter(TF.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(TR).filter(TR.dag_id == self.key).count(), 0)
            self.assertEqual(session.query(LOG).filter(LOG.dag_id == self.key).count(), 0)


if __name__ == '__main__':
    unittest.main()
