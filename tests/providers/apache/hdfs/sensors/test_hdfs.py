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

import logging
import re
from datetime import timedelta

import pytest

from airflow.exceptions import AirflowSensorTimeout
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsFolderSensor, HdfsRegexSensor, HdfsSensor
from airflow.utils.timezone import datetime
from tests.test_utils.hdfs_utils import FakeHDFSHook

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = "unit_test_dag"


class TestHdfsSensor:
    def setup_method(self):
        self.hook = FakeHDFSHook

    def test_legacy_file_exist(self):
        """
        Test the legacy behaviour
        :return:
        """
        # When
        task = HdfsSensor(
            task_id="Should_be_file_legacy",
            filepath="/datadirectory/datafile",
            timeout=1,
            retry_delay=timedelta(seconds=1),
            poke_interval=1,
            hook=self.hook,
        )
        task.execute(None)

        # Then
        # Nothing happens, nothing is raised exec is ok

    def test_legacy_file_exist_but_filesize(self):
        """
        Test the legacy behaviour with the filesize
        :return:
        """
        # When
        task = HdfsSensor(
            task_id="Should_be_file_legacy",
            filepath="/datadirectory/datafile",
            timeout=1,
            file_size=20,
            retry_delay=timedelta(seconds=1),
            poke_interval=1,
            hook=self.hook,
        )

        # When
        # Then
        with pytest.raises(AirflowSensorTimeout):
            task.execute(None)

    def test_legacy_file_does_not_exists(self):
        """
        Test the legacy behaviour
        :return:
        """
        task = HdfsSensor(
            task_id="Should_not_be_file_legacy",
            filepath="/datadirectory/not_existing_file_or_directory",
            timeout=1,
            retry_delay=timedelta(seconds=1),
            poke_interval=1,
            hook=self.hook,
        )

        # When
        # Then
        with pytest.raises(AirflowSensorTimeout):
            task.execute(None)


class TestHdfsSensorFolder:
    def setup_method(self, method):
        self.hook = FakeHDFSHook

        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        logger.debug("#" * 10)
        logger.debug("Running test case: %s.%s", self.__class__.__name__, method.__name__)
        logger.debug("#" * 10)

    def test_should_be_empty_directory(self):
        """
        test the empty directory behaviour
        :return:
        """
        # Given
        task = HdfsFolderSensor(
            task_id="Should_be_empty_directory",
            filepath="/datadirectory/empty_directory",
            be_empty=True,
            timeout=1,
            retry_delay=timedelta(seconds=1),
            poke_interval=1,
            hook=self.hook,
        )

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
        task = HdfsFolderSensor(
            task_id="Should_be_empty_directory_fail",
            filepath="/datadirectory/not_empty_directory",
            be_empty=True,
            timeout=1,
            retry_delay=timedelta(seconds=1),
            poke_interval=1,
            hook=self.hook,
        )

        # When
        # Then
        with pytest.raises(AirflowSensorTimeout):
            task.execute(None)

    def test_should_be_a_non_empty_directory(self):
        """
        test the empty directory behaviour
        :return:
        """
        # Given
        task = HdfsFolderSensor(
            task_id="Should_be_non_empty_directory",
            filepath="/datadirectory/not_empty_directory",
            timeout=1,
            retry_delay=timedelta(seconds=1),
            poke_interval=1,
            hook=self.hook,
        )

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
        task = HdfsFolderSensor(
            task_id="Should_be_empty_directory_fail",
            filepath="/datadirectory/empty_directory",
            timeout=1,
            retry_delay=timedelta(seconds=1),
            poke_interval=1,
            hook=self.hook,
        )

        # When
        # Then
        with pytest.raises(AirflowSensorTimeout):
            task.execute(None)


class TestHdfsSensorRegex:
    def setup_method(self, method):
        self.hook = FakeHDFSHook

        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        logger.debug("#" * 10)
        logger.debug("Running test case: %s.%s", self.__class__.__name__, method.__name__)
        logger.debug("#" * 10)

    def test_should_match_regex(self):
        """
        test the empty directory behaviour
        :return:
        """
        # Given
        compiled_regex = re.compile("test[1-2]file")
        task = HdfsRegexSensor(
            task_id="Should_match_the_regex",
            filepath="/datadirectory/regex_dir",
            regex=compiled_regex,
            timeout=1,
            retry_delay=timedelta(seconds=1),
            poke_interval=1,
            hook=self.hook,
        )

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
        compiled_regex = re.compile("^IDoNotExist")
        task = HdfsRegexSensor(
            task_id="Should_not_match_the_regex",
            filepath="/datadirectory/regex_dir",
            regex=compiled_regex,
            timeout=1,
            retry_delay=timedelta(seconds=1),
            poke_interval=1,
            hook=self.hook,
        )

        # When
        # Then
        with pytest.raises(AirflowSensorTimeout):
            task.execute(None)

    def test_should_match_regex_and_filesize(self):
        """
        test the file size behaviour with regex
        :return:
        """
        # Given
        compiled_regex = re.compile("test[1-2]file")
        task = HdfsRegexSensor(
            task_id="Should_match_the_regex_and_filesize",
            filepath="/datadirectory/regex_dir",
            regex=compiled_regex,
            ignore_copying=True,
            ignored_ext=["_COPYING_", "sftp"],
            file_size=10,
            timeout=1,
            retry_delay=timedelta(seconds=1),
            poke_interval=1,
            hook=self.hook,
        )

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
        compiled_regex = re.compile("test[1-2]file")
        task = HdfsRegexSensor(
            task_id="Should_match_the_regex_but_filesize",
            filepath="/datadirectory/regex_dir",
            regex=compiled_regex,
            file_size=20,
            timeout=1,
            retry_delay=timedelta(seconds=1),
            poke_interval=1,
            hook=self.hook,
        )

        # When
        # Then
        with pytest.raises(AirflowSensorTimeout):
            task.execute(None)

    def test_should_match_regex_but_copyingext(self):
        """
        test the file size behaviour with regex
        :return:
        """
        # Given
        compiled_regex = re.compile(r"copying_file_\d+.txt")
        task = HdfsRegexSensor(
            task_id="Should_match_the_regex_but_filesize",
            filepath="/datadirectory/regex_dir",
            regex=compiled_regex,
            ignored_ext=["_COPYING_", "sftp"],
            file_size=20,
            timeout=1,
            retry_delay=timedelta(seconds=1),
            poke_interval=1,
            hook=self.hook,
        )

        # When
        # Then
        with pytest.raises(AirflowSensorTimeout):
            task.execute(None)
