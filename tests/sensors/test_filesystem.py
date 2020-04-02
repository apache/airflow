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

import os.path
import shutil
import tempfile
import unittest

from airflow.exceptions import AirflowSensorTimeout
from airflow.models.dag import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.utils.timezone import datetime

TEST_DAG_ID = 'unit_tests_file_sensor'
DEFAULT_DATE = datetime(2015, 1, 1)


class TestFileSensor(unittest.TestCase):
    def setUp(self):
        from airflow.hooks.filesystem import FSHook
        hook = FSHook()
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
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
        temp_dir = tempfile.mkdtemp()
        task = FileSensor(
            task_id="test",
            filepath=temp_dir[1:] + "/file",
            fs_conn_id='fs_default',
            dag=self.dag,
            timeout=0,
            poke_interval=1
        )
        task._hook = self.hook
        try:
            with self.assertRaises(AirflowSensorTimeout):
                task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                         ignore_ti_state=True)
        finally:
            shutil.rmtree(temp_dir)

    def test_empty_dir(self):
        temp_dir = tempfile.mkdtemp()
        task = FileSensor(
            task_id="test",
            filepath=temp_dir[1:],
            fs_conn_id='fs_default',
            dag=self.dag,
            timeout=0,
            poke_interval=1
        )
        task._hook = self.hook
        try:
            with self.assertRaises(AirflowSensorTimeout):
                task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                         ignore_ti_state=True)
        finally:
            shutil.rmtree(temp_dir)

    def test_file_in_dir(self):
        temp_dir = tempfile.mkdtemp()
        task = FileSensor(
            task_id="test",
            filepath=temp_dir[1:],
            fs_conn_id='fs_default',
            dag=self.dag,
            timeout=0,
        )
        task._hook = self.hook
        try:
            # `touch` the dir
            open(temp_dir + "/file", "a").close()
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                     ignore_ti_state=True)
        finally:
            shutil.rmtree(temp_dir)

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

    def test_wildcard_file(self):
        suffix = '.txt'
        with tempfile.NamedTemporaryFile(suffix=suffix) as tmp:
            fileglob = os.path.join(os.path.dirname(tmp.name), '*' + suffix)
            task = FileSensor(
                task_id='test',
                filepath=fileglob,
                fs_conn_id='fs_default',
                dag=self.dag,
                timeout=0,
            )
            task._hook = self.hook
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                     ignore_ti_state=True)

    def test_subdirectory_not_empty(self):
        suffix = '.txt'
        temp_dir = tempfile.mkdtemp()
        subdir = tempfile.mkdtemp(dir=temp_dir)

        with tempfile.NamedTemporaryFile(suffix=suffix, dir=subdir):
            task = FileSensor(
                task_id='test',
                filepath=temp_dir,
                fs_conn_id='fs_default',
                dag=self.dag,
                timeout=0,
            )
            task._hook = self.hook
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                     ignore_ti_state=True)
        shutil.rmtree(temp_dir)

    def test_subdirectory_empty(self):
        temp_dir = tempfile.mkdtemp()
        tempfile.mkdtemp(dir=temp_dir)
        task = FileSensor(
            task_id='test',
            filepath=temp_dir,
            fs_conn_id='fs_default',
            dag=self.dag,
            timeout=0,
            poke_interval=1
        )
        task._hook = self.hook

        with self.assertRaises(AirflowSensorTimeout):
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                     ignore_ti_state=True)
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
