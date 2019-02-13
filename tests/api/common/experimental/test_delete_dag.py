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
from airflow import settings
from airflow.api.common.experimental.delete_dag import delete_dag
from airflow.exceptions import DagNotFound, DagFileExists
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.state import State

DM = models.DagModel
DR = models.DagRun
TI = models.TaskInstance
LOG = models.log.Log


class TestDeleteDAGCatchError(unittest.TestCase):

    def setUp(self):
        self.session = settings.Session()
        self.dagbag = models.DagBag(include_examples=True)
        self.dag_id = 'example_bash_operator'
        self.dag = self.dagbag.dags[self.dag_id]

    def tearDown(self):
        self.dag.clear()
        self.session.close()

    def test_delete_dag_non_existent_dag(self):
        with self.assertRaises(DagNotFound):
            delete_dag("non-existent DAG")

    def test_delete_dag_dag_still_in_dagbag(self):
        models_to_check = ['DagModel', 'DagRun', 'TaskInstance']
        record_counts = {}

        for model_name in models_to_check:
            m = getattr(models, model_name)
            record_counts[model_name] = self.session.query(m).filter(m.dag_id == self.dag_id).count()

        with self.assertRaises(DagFileExists):
            delete_dag(self.dag_id)

        # No change should happen in DB
        for model_name in models_to_check:
            m = getattr(models, model_name)
            self.assertEqual(
                self.session.query(m).filter(
                    m.dag_id == self.dag_id
                ).count(),
                record_counts[model_name]
            )


class TestDeleteDAGSuccessfulDelete(unittest.TestCase):

    def setUp(self):
        self.session = settings.Session()
        self.key = "test_dag_id"

        task = DummyOperator(task_id='dummy',
                             dag=models.DAG(dag_id=self.key,
                                            default_args={'start_date': days_ago(2)}),
                             owner='airflow')

        self.session.add(DM(dag_id=self.key))
        self.session.add(DR(dag_id=self.key))
        self.session.add(TI(task=task,
                            execution_date=days_ago(1),
                            state=State.SUCCESS))
        self.session.add(LOG(dag_id=self.key, task_id=None, task_instance=None,
                             execution_date=days_ago(1), event="varimport"))

        self.session.commit()

    def tearDown(self):
        self.session.query(DM).filter(DM.dag_id == self.key).delete()
        self.session.query(DR).filter(DR.dag_id == self.key).delete()
        self.session.query(TI).filter(TI.dag_id == self.key).delete()
        self.session.query(LOG).filter(LOG.dag_id == self.key).delete()
        self.session.commit()

        self.session.close()

    def test_delete_dag_successful_delete(self):

        self.assertEqual(self.session.query(DM).filter(DM.dag_id == self.key).count(), 1)
        self.assertEqual(self.session.query(DR).filter(DR.dag_id == self.key).count(), 1)
        self.assertEqual(self.session.query(TI).filter(TI.dag_id == self.key).count(), 1)
        self.assertEqual(self.session.query(LOG).filter(LOG.dag_id == self.key).count(), 1)

        delete_dag(dag_id=self.key)

        self.assertEqual(self.session.query(DM).filter(DM.dag_id == self.key).count(), 0)
        self.assertEqual(self.session.query(DR).filter(DR.dag_id == self.key).count(), 0)
        self.assertEqual(self.session.query(TI).filter(TI.dag_id == self.key).count(), 0)
        self.assertEqual(self.session.query(LOG).filter(LOG.dag_id == self.key).count(), 1)

    def test_delete_dag_successful_delete_not_keeping_records_in_log(self):

        self.assertEqual(self.session.query(DM).filter(DM.dag_id == self.key).count(), 1)
        self.assertEqual(self.session.query(DR).filter(DR.dag_id == self.key).count(), 1)
        self.assertEqual(self.session.query(TI).filter(TI.dag_id == self.key).count(), 1)
        self.assertEqual(self.session.query(LOG).filter(LOG.dag_id == self.key).count(), 1)

        delete_dag(dag_id=self.key, keep_records_in_log=False)

        self.assertEqual(self.session.query(DM).filter(DM.dag_id == self.key).count(), 0)
        self.assertEqual(self.session.query(DR).filter(DR.dag_id == self.key).count(), 0)
        self.assertEqual(self.session.query(TI).filter(TI.dag_id == self.key).count(), 0)
        self.assertEqual(self.session.query(LOG).filter(LOG.dag_id == self.key).count(), 0)


if __name__ == '__main__':
    unittest.main()
