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
import logging
import sys
import unittest
import re
from datetime import timedelta
from airflow.contrib.sensors.hdfs_sensors import HdfsSensorFolder, HdfsSensorRegex
from airflow.exceptions import AirflowSensorTimeout


class HdfsSensorFolderTests(unittest.TestCase):
    def setUp(self):
        if sys.version_info[0] == 3:
            raise unittest.SkipTest('HdfsSensor won\'t work with python3. No need to test anything here')
        from tests.core import FakeHDFSHook
        self.hook = FakeHDFSHook
        self.log = logging.getLogger()
        self.log.setLevel(logging.DEBUG)

    def test_should_be_empty_directory(self):
        """
        test the empty directory behaviour
        :return:
        """
        # Given
        self.log.debug('#' * 10)
        self.log.debug('Running %s', self._testMethodName)
        self.log.debug('#' * 10)
        task = HdfsSensorFolder(task_id='Should_be_empty_directory',
                                filepath='/datadirectory/empty_directory',
                                be_empty=True,
                                timeout=1,
                                retry_delay=timedelta(seconds=1),
                                poke_interval=1,
                                hook=self.hook)

        # When
        task.execute(None)

        # Then
        # Nothing happens, nothing is raised exec is ok

    def test_should_be_empty_directory_fail(self):
        """
        test the empty directory behaviour
        :return:
        """
        # Given
        self.log.debug('#' * 10)
        self.log.debug('Running %s', self._testMethodName)
        self.log.debug('#' * 10)
        task = HdfsSensorFolder(task_id='Should_be_empty_directory_fail',
                                filepath='/datadirectory/not_empty_directory',
                                be_empty=True,
                                timeout=1,
                                retry_delay=timedelta(seconds=1),
                                poke_interval=1,
                                hook=self.hook)

        # When
        # Then
        with self.assertRaises(AirflowSensorTimeout):
            task.execute(None)

    def test_should_be_a_non_empty_directory(self):
        """
        test the empty directory behaviour
        :return:
        """
        # Given
        self.log.debug('#' * 10)
        self.log.debug('Running %s', self._testMethodName)
        self.log.debug('#' * 10)
        task = HdfsSensorFolder(task_id='Should_be_non_empty_directory',
                                filepath='/datadirectory/not_empty_directory',
                                timeout=1,
                                retry_delay=timedelta(seconds=1),
                                poke_interval=1,
                                hook=self.hook)

        # When
        task.execute(None)

        # Then
        # Nothing happens, nothing is raised exec is ok

    def test_should_be_non_empty_directory_fail(self):
        """
        test the empty directory behaviour
        :return:
        """
        # Given
        self.log.debug('#' * 10)
        self.log.debug('Running %s', self._testMethodName)
        self.log.debug('#' * 10)
        task = HdfsSensorFolder(task_id='Should_be_empty_directory_fail',
                                filepath='/datadirectory/empty_directory',
                                timeout=1,
                                retry_delay=timedelta(seconds=1),
                                poke_interval=1,
                                hook=self.hook)

        # When
        # Then
        with self.assertRaises(AirflowSensorTimeout):
            task.execute(None)


class HdfsSensorRegexTests(unittest.TestCase):
    def setUp(self):
        if sys.version_info[0] == 3:
            raise unittest.SkipTest('HdfsSensor won\'t work with python3. No need to test anything here')
        from tests.core import FakeHDFSHook
        self.hook = FakeHDFSHook
        self.log = logging.getLogger()
        self.log.setLevel(logging.DEBUG)

    def test_should_match_regex(self):
        """
        test the empty directory behaviour
        :return:
        """
        # Given
        self.log.debug('#' * 10)
        self.log.debug('Running %s', self._testMethodName)
        self.log.debug('#' * 10)
        compiled_regex = re.compile("test[1-2]file")
        task = HdfsSensorRegex(task_id='Should_match_the_regex',
                               filepath='/datadirectory/regex_dir',
                               regex=compiled_regex,
                               timeout=1,
                               retry_delay=timedelta(seconds=1),
                               poke_interval=1,
                               hook=self.hook)

        # When
        task.execute(None)

        # Then
        # Nothing happens, nothing is raised exec is ok

    def test_should_not_match_regex(self):
        """
        test the empty directory behaviour
        :return:
        """
        # Given
        self.log.debug('#' * 10)
        self.log.debug('Running %s', self._testMethodName)
        self.log.debug('#' * 10)
        compiled_regex = re.compile("^IDoNotExist")
        task = HdfsSensorRegex(task_id='Should_not_match_the_regex',
                               filepath='/datadirectory/regex_dir',
                               regex=compiled_regex,
                               timeout=1,
                               retry_delay=timedelta(seconds=1),
                               poke_interval=1,
                               hook=self.hook)

        # When
        # Then
        with self.assertRaises(AirflowSensorTimeout):
            task.execute(None)

    def test_should_match_regex_and_filesize(self):
        """
        test the file size behaviour with regex
        :return:
        """
        # Given
        self.log.debug('#' * 10)
        self.log.debug('Running %s', self._testMethodName)
        self.log.debug('#' * 10)
        compiled_regex = re.compile("test[1-2]file")
        task = HdfsSensorRegex(task_id='Should_match_the_regex_and_filesize',
                               filepath='/datadirectory/regex_dir',
                               regex=compiled_regex,
                               ignore_copying=True,
                               ignored_ext=['_COPYING_', 'sftp'],
                               file_size=10,
                               timeout=1,
                               retry_delay=timedelta(seconds=1),
                               poke_interval=1,
                               hook=self.hook)

        # When
        task.execute(None)

        # Then
        # Nothing happens, nothing is raised exec is ok

    def test_should_match_regex_but_filesize(self):
        """
        test the file size behaviour with regex
        :return:
        """
        # Given
        self.log.debug('#' * 10)
        self.log.debug('Running %s', self._testMethodName)
        self.log.debug('#' * 10)
        compiled_regex = re.compile("test[1-2]file")
        task = HdfsSensorRegex(task_id='Should_match_the_regex_but_filesize',
                               filepath='/datadirectory/regex_dir',
                               regex=compiled_regex,
                               file_size=20,
                               timeout=1,
                               retry_delay=timedelta(seconds=1),
                               poke_interval=1,
                               hook=self.hook)

        # When
        # Then
        with self.assertRaises(AirflowSensorTimeout):
            task.execute(None)

    def test_should_match_regex_but_copyingext(self):
        """
        test the file size behaviour with regex
        :return:
        """
        # Given
        self.log.debug('#' * 10)
        self.log.debug('Running %s', self._testMethodName)
        self.log.debug('#' * 10)
        compiled_regex = re.compile("copying_file_\d+.txt")
        task = HdfsSensorRegex(task_id='Should_match_the_regex_but_filesize',
                               filepath='/datadirectory/regex_dir',
                               regex=compiled_regex,
                               ignored_ext=['_COPYING_', 'sftp'],
                               file_size=20,
                               timeout=1,
                               retry_delay=timedelta(seconds=1),
                               poke_interval=1,
                               hook=self.hook)

        # When
        # Then
        with self.assertRaises(AirflowSensorTimeout):
            task.execute(None)
