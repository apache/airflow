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

import os
import shutil
import tempfile
from datetime import timedelta

import pytest

from airflow.exceptions import AirflowSensorTimeout, TaskDeferred
from airflow.models.dag import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.triggers.file import FileTrigger
from airflow.utils.timezone import datetime

pytestmark = pytest.mark.db_test


TEST_DAG_ID = "unit_tests_file_sensor"
DEFAULT_DATE = datetime(2015, 1, 1)


@pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
class TestFileSensor:
    def setup_method(self):
        from airflow.hooks.filesystem import FSHook

        hook = FSHook()
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID + "test_schedule_dag_once", schedule=timedelta(days=1), default_args=args)
        self.hook = hook
        self.dag = dag

    def test_simple(self):
        with tempfile.NamedTemporaryFile() as tmp:
            task = FileSensor(
                task_id="test",
                filepath=tmp.name[1:],
                fs_conn_id="fs_default",
                dag=self.dag,
                timeout=0,
            )
            task._hook = self.hook
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_file_in_nonexistent_dir(self):
        temp_dir = tempfile.mkdtemp()
        task = FileSensor(
            task_id="test",
            filepath=temp_dir[1:] + "/file",
            fs_conn_id="fs_default",
            dag=self.dag,
            timeout=0,
            poke_interval=1,
        )
        task._hook = self.hook
        try:
            with pytest.raises(AirflowSensorTimeout):
                task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        finally:
            shutil.rmtree(temp_dir)

    def test_empty_dir(self):
        temp_dir = tempfile.mkdtemp()
        task = FileSensor(
            task_id="test",
            filepath=temp_dir[1:],
            fs_conn_id="fs_default",
            dag=self.dag,
            timeout=0,
            poke_interval=1,
        )
        task._hook = self.hook
        try:
            with pytest.raises(AirflowSensorTimeout):
                task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        finally:
            shutil.rmtree(temp_dir)

    def test_file_in_dir(self):
        temp_dir = tempfile.mkdtemp()
        task = FileSensor(
            task_id="test",
            filepath=temp_dir[1:],
            fs_conn_id="fs_default",
            dag=self.dag,
            timeout=0,
        )
        task._hook = self.hook
        try:
            # `touch` the dir
            open(temp_dir + "/file", "a").close()
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
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
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_wildcard_file(self):
        suffix = ".txt"
        with tempfile.NamedTemporaryFile(suffix=suffix) as tmp:
            fileglob = os.path.join(os.path.dirname(tmp.name), "*" + suffix)
            task = FileSensor(
                task_id="test",
                filepath=fileglob,
                fs_conn_id="fs_default",
                dag=self.dag,
                timeout=0,
            )
            task._hook = self.hook
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_wildcard_empty_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with tempfile.TemporaryDirectory(suffix="subdir", dir=temp_dir):
                task = FileSensor(
                    task_id="test",
                    filepath=os.path.join(temp_dir, "*dir"),
                    fs_conn_id="fs_default",
                    dag=self.dag,
                    timeout=0,
                )
                task._hook = self.hook

                # No files in dir
                with pytest.raises(AirflowSensorTimeout):
                    task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_wildcard_directory_with_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with tempfile.TemporaryDirectory(suffix="subdir", dir=temp_dir) as subdir:
                task = FileSensor(
                    task_id="test",
                    filepath=os.path.join(temp_dir, "*dir"),
                    fs_conn_id="fs_default",
                    dag=self.dag,
                    timeout=0,
                )
                task._hook = self.hook

                # `touch` the file in subdir
                open(os.path.join(subdir, "file"), "a").close()
                task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_wildcared_directory(self):
        temp_dir = tempfile.mkdtemp()
        subdir = tempfile.mkdtemp(dir=temp_dir)
        task = FileSensor(
            task_id="test",
            filepath=temp_dir + "/**",
            fs_conn_id="fs_default",
            dag=self.dag,
            timeout=0,
            poke_interval=1,
            recursive=True,
        )
        task._hook = self.hook

        try:
            # `touch` the file in subdir
            open(subdir + "/file", "a").close()
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        finally:
            shutil.rmtree(temp_dir)

    def test_subdirectory_not_empty(self):
        suffix = ".txt"
        temp_dir = tempfile.mkdtemp()
        subdir = tempfile.mkdtemp(dir=temp_dir)

        with tempfile.NamedTemporaryFile(suffix=suffix, dir=subdir):
            task = FileSensor(
                task_id="test",
                filepath=temp_dir,
                fs_conn_id="fs_default",
                dag=self.dag,
                timeout=0,
            )
            task._hook = self.hook
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        shutil.rmtree(temp_dir)

    def test_subdirectory_empty(self, tmp_path):
        (tmp_path / "subdir").mkdir()

        task = FileSensor(
            task_id="test",
            filepath=tmp_path.as_posix(),
            fs_conn_id="fs_default",
            dag=self.dag,
            timeout=0,
            poke_interval=1,
        )
        task._hook = self.hook

        with pytest.raises(AirflowSensorTimeout):
            task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_task_defer(self):
        task = FileSensor(
            task_id="test",
            filepath="temp_dir",
            fs_conn_id="fs_default",
            deferrable=True,
            dag=self.dag,
        )

        with pytest.raises(TaskDeferred) as exc:
            task.execute(None)

        assert isinstance(exc.value.trigger, FileTrigger), "Trigger is not a FileTrigger"
