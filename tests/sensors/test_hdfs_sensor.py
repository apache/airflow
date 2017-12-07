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
import unittest

from datetime import timedelta

from airflow import configuration
from airflow.exceptions import AirflowSensorTimeout
from airflow.sensors.hdfs_sensor import HdfsSensor
from airflow.utils.timezone import datetime
from tests.core import FakeHDFSHook

configuration.load_test_config()

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_dag'


class HdfsSensorTests(unittest.TestCase):

    def setUp(self):
        self.hook = FakeHDFSHook

    def test_legacy_file_exist(self):
        """
        Test the legacy behaviour
        :return:
        """
        # When
        task = HdfsSensor(task_id='Should_be_file_legacy',
                          filepath='/datadirectory/datafile',
                          timeout=1,
                          retry_delay=timedelta(seconds=1),
                          poke_interval=1,
                          hook=self.hook)
        task.execute(None)

        # Then
        # Nothing happens, nothing is raised exec is ok

    def test_legacy_file_exist_but_filesize(self):
        """
        Test the legacy behaviour with the filesize
        :return:
        """
        # When
        task = HdfsSensor(task_id='Should_be_file_legacy',
                          filepath='/datadirectory/datafile',
                          timeout=1,
                          file_size=20,
                          retry_delay=timedelta(seconds=1),
                          poke_interval=1,
                          hook=self.hook)

        # When
        # Then
        with self.assertRaises(AirflowSensorTimeout):
            task.execute(None)

    def test_legacy_file_does_not_exists(self):
        """
        Test the legacy behaviour
        :return:
        """
        task = HdfsSensor(task_id='Should_not_be_file_legacy',
                          filepath='/datadirectory/not_existing_file_or_directory',
                          timeout=1,
                          retry_delay=timedelta(seconds=1),
                          poke_interval=1,
                          hook=self.hook)

        # When
        # Then
        with self.assertRaises(AirflowSensorTimeout):
            task.execute(None)
