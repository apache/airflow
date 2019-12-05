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
#

import unittest
import shutil
import tempfile

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.exceptions import AirflowSensorTimeout
from airflow.utils.timezone import datetime

TEST_DAG_ID = 'unit_tests_file_sensor'
DEFAULT_DATE = datetime(2015, 1, 1)


class FileSensorTest(unittest.TestCase):
    def setUp(self):
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
        with tempfile.NamedTemporaryFile() as tmp:
            task = FileSensor(
                task_id="test",
                filepath=tmp.name[1:],
                fs_conn_id='fs_default',
                dag=self.dag,
                timeout=0,
            )
            task._hook = self.hook
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                     ignore_ti_state=True)

    def test_file_in_nonexistent_dir(self):
        dir = tempfile.mkdtemp()
        task = FileSensor(
            task_id="test",
            filepath=dir[1:] + "/file",
            fs_conn_id='fs_default',
            dag=self.dag,
            timeout=0,
        )
        task._hook = self.hook
        try:
            with self.assertRaises(AirflowSensorTimeout):
                task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                         ignore_ti_state=True)
        finally:
            shutil.rmtree(dir)

    def test_empty_dir(self):
        dir = tempfile.mkdtemp()
        task = FileSensor(
            task_id="test",
            filepath=dir[1:],
            fs_conn_id='fs_default',
            dag=self.dag,
            timeout=0,
        )
        task._hook = self.hook
        try:
            with self.assertRaises(AirflowSensorTimeout):
                task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                         ignore_ti_state=True)
        finally:
            shutil.rmtree(dir)

    def test_file_in_dir(self):
        dir = tempfile.mkdtemp()
        task = FileSensor(
            task_id="test",
            filepath=dir[1:],
            fs_conn_id='fs_default',
            dag=self.dag,
            timeout=0,
        )
        task._hook = self.hook
        try:
            # `touch` the dir
            open(dir + "/file", "a").close()
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                     ignore_ti_state=True)
        finally:
            shutil.rmtree(dir)

    def test_default_fs_conn_id(self):
        with tempfile.NamedTemporaryFile() as tmp:
            task = FileSensor(
                task_id="test",
                filepath=tmp.name[1:],
                dag=self.dag,
                timeout=0,
            )
            task._hook = self.hook
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                     ignore_ti_state=True)


if __name__ == '__main__':
    unittest.main()
