# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
from os.path import dirname, realpath
import unittest
from datetime import timedelta, datetime

from airflow.models import DagBag, TaskInstance, DagRun
from airflow.settings import Session
from airflow.utils.state import State
from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.utils.timezone import convert_to_utc

DEFAULT_DATE = convert_to_utc(datetime(2017, 1, 1))
TEST_DAG_ID = 'test_dagrun_sensor_dag'
TEST_DAG_FOLDER = os.path.join(
    dirname(dirname(dirname(realpath(__file__)))), 'dags')


class TestDagRunSensor(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        self.default_scheduler_args = {
            "file_process_interval": 0,
            "processor_poll_interval": 0.5,
            "num_runs": 1
        }
        self.dagbag = DagBag(dag_folder=TEST_DAG_FOLDER)

    def test_poke(self):
        dag_parent = self.dagbag.get_dag(TEST_DAG_ID + '_parent_clean')
        dag_parent.run(
            start_date=DEFAULT_DATE + timedelta(seconds=0),
            end_date=DEFAULT_DATE + timedelta(seconds=8),
        )

        dag_child = self.dagbag.get_dag(TEST_DAG_ID + '_child_clean')

        # One of the following two runs should succeed (00:00:00), while the
        # other (00:00:05) should have its sensor time out, since 00:00:09
        # will never be run for the parent dag.

        # first (safe) run
        dag_child.run(
            start_date=DEFAULT_DATE + timedelta(seconds=0),
            end_date=DEFAULT_DATE + timedelta(seconds=0),
        )

        sess = Session()
        TI = TaskInstance
        sensor_tis = sess.query(TI).filter(
            TI.dag_id == TEST_DAG_ID + '_child_clean',
            TI.task_id == 'sense_parent',
            TI.state == State.SUCCESS,
        ).all()
        self.assertEqual(len(sensor_tis), 1)

        do_stuff_tis = sess.query(TI).filter(
            TI.dag_id == TEST_DAG_ID + '_child_clean',
            TI.task_id == 'do_stuff',
            TI.state == State.SUCCESS,
        ).all()
        self.assertEqual(len(do_stuff_tis), 1)

        DR = DagRun
        drs = sess.query(DR).filter(
            DR.dag_id == TEST_DAG_ID + '_child_clean',
            DR.state == State.SUCCESS,
            DR.execution_date == DEFAULT_DATE,
        ).all()
        self.assertEqual(len(drs), 1)

        # second run
        with self.assertRaises(AirflowException):
            # the AirflowTaskTimeout raised by the sensor is caught by
            # the executor, and what we see is an AirflowException for
            # the dependent task which fails because of a failed upstream
            # task.
            dag_child.run(
                start_date=DEFAULT_DATE + timedelta(seconds=5),
                end_date=DEFAULT_DATE + timedelta(seconds=5),
            )

        failed_tis = sess.query(TI).filter(
            TI.state == State.FAILED,
        ).all()
        self.assertEqual(len(failed_tis), 1)
        failed_ti = failed_tis[0]
        self.assertEqual(failed_ti.task_id, 'sense_parent')
        self.assertEqual(failed_ti.dag_id, TEST_DAG_ID + '_child_clean')
        self.assertEqual(failed_ti.execution_date,
                         DEFAULT_DATE + timedelta(seconds=5))

        failed_drs = sess.query(DR).filter(
            DR.dag_id == TEST_DAG_ID + '_child_clean',
            DR.state == State.FAILED,
        ).all()
        self.assertEqual(len(failed_drs), 1)

        self.assertEqual(failed_drs[0].execution_date,
                         DEFAULT_DATE + timedelta(seconds=5))


if __name__ == '__main__':
    unittest.main()
