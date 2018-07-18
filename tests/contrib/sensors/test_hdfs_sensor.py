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

import logging
import mock
import unittest

import re
from datetime import timedelta

from airflow import models
from airflow.contrib.sensors.hdfs_sensor import HdfsSensorFolder, HdfsSensorRegex
from airflow.exceptions import AirflowSensorTimeout
from airflow.hooks.hdfs_hook import HdfsHook

from tests.sensors.test_hdfs_sensor import MockHdfs3Client


class HdfsSensorRegexTests(unittest.TestCase):
    """Tests for the HdfsSensorRegex class."""

    def setUp(self):
        file_details = [
            {
                'kind': 'directory',
                'name': '/data',
                'size': 0
            },
            {
                'kind': 'file',
                'name': '/data/test1file',
                'size': 2000000
            },
            {
                'kind': 'file',
                'name': '/data/copying._COPYING_',
                'size': 2000000
            }
        ]

        self._mock_client, self._mock_params = \
            MockHdfs3Client.from_file_details(file_details, test_instance=self)

        self._default_task_kws = {
            'timeout': 1,
            'retry_delay': timedelta(seconds=1),
            'poke_interval': 1
        }

    def test_should_match_regex(self):
        """Tests example where files should match regex."""

        regex = re.compile("test[1-2]file")
        task = HdfsSensorRegex(task_id='should_match_the_regex',
                               file_pattern='/data/*',
                               regex=regex,
                               **self._default_task_kws)
        self.assertTrue(task.poke(context={}))

    def test_should_match_regex_dir(self):
        """Tests example where files should match regex with dir path."""

        regex = re.compile("test[1-2]file")
        task = HdfsSensorRegex(task_id='should_match_the_regex',
                               file_pattern='/data',
                               regex=regex,
                               **self._default_task_kws)
        self.assertTrue(task.poke(context={}))

    def test_should_not_match_regex(self):
        """Tests example where files should match regex."""

        regex = re.compile("^IDoNotExist")
        task = HdfsSensorRegex(task_id='should_not_match_the_regex',
                               file_pattern='/data/*',
                               regex=regex,
                               **self._default_task_kws)
        self.assertFalse(task.poke(context={}))

    def test_should_match_regex_and_size(self):
        """Tests example with matching regex and sufficient file size."""

        regex = re.compile("test[1-2]file")
        task = HdfsSensorRegex(task_id='should_match_the_regex_and_size',
                               file_pattern='/data/*',
                               regex=regex,
                               min_size=1,
                               **self._default_task_kws)
        self.assertTrue(task.poke(context={}))

    def test_should_match_regex_not_size(self):
        """Tests example with matching regex but too small file size."""

        regex = re.compile("test[1-2]file")
        task = HdfsSensorRegex(task_id='should_match_the_regex_but_not_size',
                               file_pattern='/data/*',
                               regex=regex,
                               min_size=10,
                               **self._default_task_kws)
        self.assertFalse(task.poke(context={}))

    def test_should_match_regex_not_ext(self):
        """Tests example with matching regex but wrong ext."""

        regex = re.compile("test[1-2]file")
        task = HdfsSensorRegex(task_id='should_match_the_regex_but_not_size',
                               file_pattern='/data/*',
                               regex=regex,
                               min_size=10,
                               **self._default_task_kws)
        self.assertFalse(task.poke(context={}))

        compiled_regex = re.compile("copying.*")
        task = HdfsSensorRegex(task_id='should_match_the_regex_but_not_ext',
                               file_pattern='/data/*',
                               regex=compiled_regex,
                               ignore_exts=['_COPYING_'],
                               **self._default_task_kws)
        self.assertFalse(task.poke(context={}))


class HdfsSensorFolderTests(unittest.TestCase):

    def setUp(self):
        file_details = [
            {
                'kind': 'directory',
                'name': '/empty',
                'size': 0
            },
            {
                'kind': 'directory',
                'name': '/not_empty',
                'size': 0
            },
            {
                'kind': 'file',
                'name': '/not_empty/test1file',
                'size': 2000000
            }
        ]

        self._mock_client, self._mock_params = \
            MockHdfs3Client.from_file_details(file_details, test_instance=self)

        self._default_task_kws = {
            'timeout': 1,
            'retry_delay': timedelta(seconds=1),
            'poke_interval': 1
        }

        self._mock_client = MockHdfs3Client(file_details)
        self._mock_params = models.Connection(conn_id='hdfs_default')

    def test_should_be_empty_directory(self):
        """Tests example with empty directory."""

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

#     def test_should_be_empty_directory_fail(self):
#         """
#         test the empty directory behaviour
#         :return:
#         """
#         # Given
#         self.log.debug('#' * 10)
#         self.log.debug('Running %s', self._testMethodName)
#         self.log.debug('#' * 10)
#         task = HdfsSensorFolder(task_id='Should_be_empty_directory_fail',
#                                 filepath='/datadirectory/not_empty_directory',
#                                 be_empty=True,
#                                 timeout=1,
#                                 retry_delay=timedelta(seconds=1),
#                                 poke_interval=1,
#                                 hook=self.hook)

#         # When
#         # Then
#         with self.assertRaises(AirflowSensorTimeout):
#             task.execute(None)

#     def test_should_be_a_non_empty_directory(self):
#         """
#         test the empty directory behaviour
#         :return:
#         """
#         # Given
#         self.log.debug('#' * 10)
#         self.log.debug('Running %s', self._testMethodName)
#         self.log.debug('#' * 10)
#         task = HdfsSensorFolder(task_id='Should_be_non_empty_directory',
#                                 filepath='/datadirectory/not_empty_directory',
#                                 timeout=1,
#                                 retry_delay=timedelta(seconds=1),
#                                 poke_interval=1,
#                                 hook=self.hook)

#         # When
#         task.execute(None)

#         # Then
#         # Nothing happens, nothing is raised exec is ok

#     def test_should_be_non_empty_directory_fail(self):
#         """
#         test the empty directory behaviour
#         :return:
#         """
#         # Given
#         self.log.debug('#' * 10)
#         self.log.debug('Running %s', self._testMethodName)
#         self.log.debug('#' * 10)
#         task = HdfsSensorFolder(task_id='Should_be_empty_directory_fail',
#                                 filepath='/datadirectory/empty_directory',
#                                 timeout=1,
#                                 retry_delay=timedelta(seconds=1),
#                                 poke_interval=1,
#                                 hook=self.hook)

#         # When
#         # Then
#         with self.assertRaises(AirflowSensorTimeout):
#             task.execute(None)


if __name__ == '__main__':
    unittest.main()
