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
import posixpath
import unittest

import mock

from airflow import models
from airflow.sensors.hdfs_sensor import HdfsFileSensor, HdfsFolderSensor, HdfsHook


class MockHdfs3Client(object):
    """Mock hdfs3 client for testing purposes."""

    def __init__(self, file_details):
        self._file_details = {entry["name"]: entry for entry in file_details}

    @classmethod
    def from_file_details(cls, file_details, test_instance, conn_id="hdfs_default"):
        """Builds mock hdsf3 client using file_details."""

        mock_client = cls(file_details)
        mock_params = models.Connection(conn_id=conn_id)

        # Setup mock for get_connection.
        patcher = mock.patch.object(
            HdfsHook, "get_connection", return_value=mock_params
        )
        test_instance.addCleanup(patcher.stop)
        patcher.start()

        # Setup mock for get_conn.
        patcher = mock.patch.object(HdfsHook, "get_conn", return_value=mock_client)
        test_instance.addCleanup(patcher.stop)
        patcher.start()

        return mock_client, mock_params

    def glob(self, pattern):
        """Returns glob of files matching pattern."""

        # Implements a non-recursive glob on file names.
        pattern_dir = posixpath.dirname(pattern)
        pattern_base = posixpath.basename(pattern)

        # Ensure pattern_dir ends with '/'.
        pattern_dir = posixpath.join(pattern_dir, "")

        for file_path in self._file_details.keys():
            file_basename = posixpath.basename(file_path)
            if file_path.startswith(pattern_dir) and \
                    fnmatch.fnmatch(file_basename, pattern_base):
                yield file_path

    def isdir(self, path):
        """Returns true if path is a directory."""

        try:
            details = self._file_details[path]
        except KeyError:
            raise IOError()

        return details["kind"] == "directory"

    def info(self, path):
        """Returns info for given file path."""

        try:
            return self._file_details[path]
        except KeyError:
            raise IOError()


class HdfsFileSensorTests(unittest.TestCase):
    """Tests for the HdfsFileSensor class."""

    def setUp(self):
        file_details = [
            {"kind": "directory", "name": "/data/empty", "size": 0},
            {"kind": "directory", "name": "/data/not_empty", "size": 0},
            {"kind": "file", "name": "/data/not_empty/small.txt", "size": 10},
            {"kind": "file", "name": "/data/not_empty/large.txt", "size": 10000000},
            {"kind": "file", "name": "/data/not_empty/file.txt._COPYING_"},
        ]

        self._mock_client, self._mock_params = MockHdfs3Client.from_file_details(
            file_details, test_instance=self
        )

        self._default_task_kws = {
            "timeout": 1,
            "retry_delay": timedelta(seconds=1),
            "poke_interval": 1,
        }

    def test_existing_file(self):
        """Tests poking for existing file."""

        task = HdfsFileSensor(
            task_id="existing_file",
            pattern="/data/not_empty/small.txt",
            **self._default_task_kws
        )
        self.assertTrue(task.poke(context={}))

    def test_existing_file_glob(self):
        """Tests poking for existing file with glob."""

        task = HdfsFileSensor(
            task_id="existing_file",
            pattern="/data/not_empty/*.txt",
            **self._default_task_kws
        )
        self.assertTrue(task.poke(context={}))

    def test_nonexisting_file(self):
        """Tests poking for non-existing file."""

        task = HdfsFileSensor(
            task_id="nonexisting_file",
            pattern="/data/not_empty/random.txt",
            **self._default_task_kws
        )
        self.assertFalse(task.poke(context={}))

    def test_nonexisting_file_glob(self):
        """Tests poking for non-existing file with glob."""

        task = HdfsFileSensor(
            task_id="existing_file",
            pattern="/data/not_empty/*.xml",
            **self._default_task_kws
        )
        self.assertFalse(task.poke(context={}))

    def test_nonexisting_path(self):
        """Tests poking for non-existing path."""

        task = HdfsFileSensor(
            task_id="nonexisting_path",
            pattern="/data/not_empty/small.txt",
            **self._default_task_kws
        )

        with mock.patch.object(self._mock_client, "glob", side_effect=IOError):
            self.assertFalse(task.poke(context={}))

    def test_file_filter_size_small(self):
        """Tests poking for file while filtering for file size (too small)."""

        task = HdfsFileSensor(
            task_id="existing_file_too_small",
            pattern="/data/not_empty/small.txt",
            min_size=1,
            **self._default_task_kws
        )
        self.assertFalse(task.poke(context={}))

    def test_file_filter_size_large(self):
        """Tests poking for file while filtering for file size (large)."""

        task = HdfsFileSensor(
            task_id="existing_file_large",
            pattern="/data/not_empty/large.txt",
            min_size=1,
            **self._default_task_kws
        )
        self.assertTrue(task.poke(context={}))

    def test_file_filter_ext(self):
        """Tests poking for file while filtering for extension."""

        task = HdfsFileSensor(
            task_id="existing_file_large",
            pattern="/data/not_empty/f*",
            ignore_exts=("_COPYING_",),
            **self._default_task_kws
        )
        self.assertFalse(task.poke(context={}))


class HdfsFolderSensorTests(unittest.TestCase):
    def setUp(self):
        file_details = [
            {"kind": "directory", "name": "/empty", "size": 0},
            {"kind": "directory", "name": "/not_empty", "size": 0},
            {"kind": "file", "name": "/not_empty/a.tmp", "size": 2000000},
            {"kind": "directory", "name": "/nested", "size": 0},
            {"kind": "directory", "name": "/nested/a", "size": 0}
        ]

        self._mock_client, self._mock_params = MockHdfs3Client.from_file_details(
            file_details, test_instance=self
        )

        self._default_task_kws = {
            "timeout": 1,
            "retry_delay": timedelta(seconds=1),
            "poke_interval": 1,
        }

        self._mock_client = MockHdfs3Client(file_details)
        self._mock_params = models.Connection(conn_id="hdfs_default")

    def test_empty_directory(self):
        """Tests example with an empty directory."""

        task = HdfsFolderSensor(task_id="test_empty_directory", pattern="/empty")
        self.assertTrue(task.poke(context={}))

    def test_non_empty_directory(self):
        """Tests example with a non-empty directory."""

        task = HdfsFolderSensor(task_id="test_non_empty_directory", pattern="/empty")
        self.assertTrue(task.poke(context={}))

    def test_required_empty_directory(self):
        """Tests example with an empty directory and require_empty = True."""

        task = HdfsFolderSensor(
            task_id="test_empty_directory", pattern="/empty", require_empty=True
        )
        self.assertTrue(task.poke(context={}))

    def test_required_empty_directory_fail(self):
        """Tests example with a non-empty directory and require_empty = True."""

        task = HdfsFolderSensor(
            task_id="test_non_empty_directory", pattern="/not_empty", require_empty=True
        )
        self.assertFalse(task.poke(context={}))

    def test_required_not_empty_directory(self):
        """Tests example with a non-empty directory and
           require_not_empty = True.
        """

        task = HdfsFolderSensor(
            task_id="test_required_not_empty_directory",
            pattern="/not_empty",
            require_not_empty=True,
        )
        self.assertTrue(task.poke(context={}))

    def test_required_not_empty_directory_fail(self):
        """Tests example with an empty directory and
           require_not_empty = True.
        """

        task = HdfsFolderSensor(
            task_id="test_required_not_empty_directory",
            pattern="/empty",
            require_not_empty=True,
        )
        self.assertFalse(task.poke(context={}))

    def test_glob(self):
        """Tests globbing for directories."""

        task = HdfsFolderSensor(task_id="test_glob", pattern="/*")
        self.assertTrue(task.poke(context={}))

    def test_glob_non_existing(self):
        """Tests globbing for non-existing directories."""

        task = HdfsFolderSensor(
            task_id="test_glob_non_existing", pattern="/non-existing/*"
        )
        self.assertFalse(task.poke(context={}))

    def test_glob_require_empty(self):
        """Tests globbing with only empty dir and require_empty = True."""

        task = HdfsFolderSensor(
            task_id="test_glob_require_empty", pattern="/e*", require_empty=True
        )
        self.assertTrue(task.poke(context={}))

    def test_glob_require_empty_fail(self):
        """Tests globbing with non-empty dir and require_empty = True."""

        task = HdfsFolderSensor(
            task_id="test_glob_require_empty_fail", pattern="/*", require_empty=True
        )
        self.assertFalse(task.poke(context={}))

    def test_glob_require_not_empty(self):
        """Tests globbing with only non-empty dir and
           require_not_empty = True.
        """

        task = HdfsFolderSensor(
            task_id="test_glob_require_not_empty", pattern="/n*", require_not_empty=True
        )
        self.assertTrue(task.poke(context={}))

    def test_glob_require_not_empty_fail(self):
        """Tests globbing with empty dir and require_not_empty = True."""

        task = HdfsFolderSensor(
            task_id="test_glob_require_not_empty_fail",
            pattern="/*",
            require_not_empty=True,
        )
        self.assertFalse(task.poke(context={}))

    def test_sub_pattern(self):
        """Tests filtering directory with matching sub_pattern."""

        task = HdfsFolderSensor(
            task_id="test_sub_pattern",
            pattern="/not_empty",
            require_not_empty=True,
            sub_pattern="*.tmp"
        )
        self.assertTrue(task.poke(context={}))

    def test_sub_pattern_no_match(self):
        """Tests filtering directory with sub_pattern that doesn't match."""

        task = HdfsFolderSensor(
            task_id="test_sub_pattern_no_match",
            pattern="/not_empty",
            require_not_empty=True,
            sub_pattern="*.txt"
        )
        self.assertFalse(task.poke(context={}))

    def test_sub_pattern_subdir(self):
        """Tests filtering directory with sub_pattern that matches subdir."""

        task = HdfsFolderSensor(
            task_id="test_sub_pattern_subdir",
            pattern="/nested",
            require_not_empty=True,
            sub_pattern="a"
        )
        self.assertTrue(task.poke(context={}))

    def test_sub_pattern_subdir_no_match(self):
        """Tests filtering directory with sub_pattern that doesn't
           match subdir.
        """

        task = HdfsFolderSensor(
            task_id="test_sub_pattern_subdir_no_match",
            pattern="/nested",
            require_not_empty=True,
            sub_pattern="b"
        )
        self.assertFalse(task.poke(context={}))


if __name__ == "__main__":
    unittest.main()
