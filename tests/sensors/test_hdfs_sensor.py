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

from datetime import timedelta
import fnmatch
import unittest

import mock

from airflow import models
from airflow.sensors.hdfs_sensor import HdfsSensor, HdfsHook


class MockHdfs3Client(object):
    """Mock hdfs3 client for testing purposes."""

    def __init__(self, file_details):
        self._file_details = {
            entry['name']: entry for entry in file_details
        }

    @classmethod
    def from_file_details(cls, file_details, test_instance,
                          conn_id='hdfs_default'):
        """Builds mock hdsf3 client using file_details."""

        mock_client = cls(file_details)
        mock_params = models.Connection(conn_id=conn_id)

        # Setup mock for get_connection.
        patcher = mock.patch.object(
            HdfsHook, 'get_connection', return_value=mock_params)
        test_instance.addCleanup(patcher.stop)
        patcher.start()

        # Setup mock for get_conn.
        patcher = mock.patch.object(
            HdfsHook, 'get_conn', return_value=mock_client)
        test_instance.addCleanup(patcher.stop)
        patcher.start()

        return mock_client, mock_params

    def glob(self, pattern):
        """Returns glob of files matching pattern."""
        return fnmatch.filter(self._file_details.keys(), pattern)

    def info(self, file_path):
        """Returns info for given file path."""

        try:
            return self._file_details[file_path]
        except KeyError:
            raise IOError()


class HdfsSensorTests(unittest.TestCase):
    """Tests for the HdfsSensor class."""

    def setUp(self):
        file_details = [
            {
                'kind': 'directory',
                'name': '/data/empty',
                'size': 0
            },
            {
                'kind': 'directory',
                'name': '/data/not_empty',
                'size': 0
            },
            {
                'kind': 'file',
                'name': '/data/not_empty/small.txt',
                'size': 10
            },
            {
                'kind': 'file',
                'name': '/data/not_empty/large.txt',
                'size': 10000000
            },
            {
                'kind': 'file',
                'name': '/data/not_empty/file.txt._COPYING_'
            }
        ]

        self._mock_client, self._mock_params = \
            MockHdfs3Client.from_file_details(file_details, test_instance=self)

        self._default_task_kws = {
            'timeout': 1,
            'retry_delay': timedelta(seconds=1),
            'poke_interval': 1
        }

    def test_existing_file(self):
        """Tests poking for existing file."""

        task = HdfsSensor(task_id='existing_file',
                          file_path='/data/not_empty/small.txt',
                          **self._default_task_kws)
        self.assertTrue(task.poke(context={}))

    def test_existing_file_glob(self):
        """Tests poking for existing file with glob."""

        task = HdfsSensor(task_id='existing_file',
                          file_path='/data/not_empty/*.txt',
                          **self._default_task_kws)
        self.assertTrue(task.poke(context={}))

    def test_nonexisting_file(self):
        """Tests poking for non-existing file."""

        task = HdfsSensor(task_id='nonexisting_file',
                          file_path='/data/not_empty/random.txt',
                          **self._default_task_kws)
        self.assertFalse(task.poke(context={}))

    def test_nonexisting_file_glob(self):
        """Tests poking for non-existing file with glob."""

        task = HdfsSensor(task_id='existing_file',
                          file_path='/data/not_empty/*.xml',
                          **self._default_task_kws)
        self.assertFalse(task.poke(context={}))

    def test_nonexisting_path(self):
        """Tests poking for non-existing path."""

        task = HdfsSensor(task_id='nonexisting_path',
                          file_path='/data/not_empty/small.txt',
                          **self._default_task_kws)

        with mock.patch.object(self._mock_client, 'glob', side_effect=IOError):
            self.assertFalse(task.poke(context={}))

    def test_file_filter_size_small(self):
        """Tests poking for file while filtering for file size (too small)."""

        task = HdfsSensor(task_id='existing_file_too_small',
                          file_path='/data/not_empty/small.txt',
                          min_size=1,
                          **self._default_task_kws)
        self.assertFalse(task.poke(context={}))

    def test_file_filter_size_large(self):
        """Tests poking for file while filtering for file size (large)."""

        task = HdfsSensor(task_id='existing_file_large',
                          file_path='/data/not_empty/large.txt',
                          min_size=1,
                          **self._default_task_kws)
        self.assertTrue(task.poke(context={}))

    def test_file_filter_ext(self):
        """Tests poking for file while filtering for extension."""

        task = HdfsSensor(task_id='existing_file_large',
                          file_path='/data/not_empty/f*',
                          ignored_ext=('_COPYING_', ),
                          **self._default_task_kws)
        self.assertFalse(task.poke(context={}))


if __name__ == '__main__':
    unittest.main()
