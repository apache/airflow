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
#

import unittest

from airflow import configuration
from airflow import models, DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.settings import Session
from airflow.utils.timezone import datetime

TEST_DAG_ID = 'unit_tests'
DEFAULT_DATE = datetime(2015, 1, 1)
configuration.load_test_config()


def reset(dag_id=TEST_DAG_ID):
    session = Session()
    tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
    tis.delete()
    session.commit()
    session.close()


reset()


class FileSensorTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        from airflow.contrib.hooks.fs_hook import FSHook
        hook = FSHook()
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
            'provide_context': True
        }
        dag = DAG(TEST_DAG_ID + 'test_schedule_dag_once', default_args=args)
        dag.schedule_interval = '@once'
        self.hook = hook
        self.dag = dag

    def test_simple(self):
        task = FileSensor(
            task_id="test",
            filepath="etc/hosts",
            fs_conn_id='fs_default',
            _hook=self.hook,
            dag=self.dag,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)


if __name__ == '__main__':
    unittest.main()
