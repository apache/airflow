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
from unittest import mock

import pytest

from airflow.utils import file as file_utils
from airflow.utils.file import (
    correct_maybe_zipped,
    open_maybe_zipped,
)

from unit.models import TEST_DAGS_FOLDER


class TestCorrectMaybeZipped:
    @mock.patch("zipfile.is_zipfile")
    def test_correct_maybe_zipped_normal_file(self, mocked_is_zipfile):
        path = "/path/to/some/file.txt"
        mocked_is_zipfile.return_value = False

        dag_folder = correct_maybe_zipped(path)

        assert dag_folder == path

    @mock.patch("zipfile.is_zipfile")
    def test_correct_maybe_zipped_normal_file_with_zip_in_name(self, mocked_is_zipfile):
        path = "/path/to/fakearchive.zip.other/file.txt"
        mocked_is_zipfile.return_value = False

        dag_folder = correct_maybe_zipped(path)

        assert dag_folder == path

    @mock.patch("zipfile.is_zipfile")
    def test_correct_maybe_zipped_archive(self, mocked_is_zipfile):
        path = "/path/to/archive.zip/deep/path/to/file.txt"
        mocked_is_zipfile.return_value = True

        dag_folder = correct_maybe_zipped(path)

        assert mocked_is_zipfile.call_count == 1
        (args, kwargs) = mocked_is_zipfile.call_args_list[0]
        assert args[0] == "/path/to/archive.zip"

        assert dag_folder == "/path/to/archive.zip"


class TestOpenMaybeZipped:
    def test_open_maybe_zipped_normal_file(self):
        test_file_path = os.path.join(TEST_DAGS_FOLDER, "no_dags.py")
        with open_maybe_zipped(test_file_path, "r") as test_file:
            content = test_file.read()
        assert isinstance(content, str)

    def test_open_maybe_zipped_normal_file_with_zip_in_name(self):
        path = "/path/to/fakearchive.zip.other/file.txt"
        with mock.patch("builtins.open", mock.mock_open(read_data="data")) as mock_file:
            open_maybe_zipped(path)
            mock_file.assert_called_once_with(path, mode="r")

    def test_open_maybe_zipped_archive(self, test_zip_path):
        test_file_path = os.path.join(test_zip_path, "test_zip.py")
        with open_maybe_zipped(test_file_path, "r") as test_file:
            content = test_file.read()
        assert isinstance(content, str)


@pytest.mark.parametrize(
    ("edge_filename", "expected_modification"),
    [
        ("test_dag.py", "unusual_prefix_mocked_path_hash_sha1_test_dag"),
        ("test-dag.py", "unusual_prefix_mocked_path_hash_sha1_test_dag"),
        ("test-dag-1.py", "unusual_prefix_mocked_path_hash_sha1_test_dag_1"),
        ("test-dag_1.py", "unusual_prefix_mocked_path_hash_sha1_test_dag_1"),
        ("test-dag.dev.py", "unusual_prefix_mocked_path_hash_sha1_test_dag_dev"),
        ("test_dag.prod.py", "unusual_prefix_mocked_path_hash_sha1_test_dag_prod"),
    ],
)
def test_get_unique_dag_module_name(edge_filename, expected_modification):
    with mock.patch("hashlib.sha1") as mocked_sha1:
        mocked_sha1.return_value.hexdigest.return_value = "mocked_path_hash_sha1"
        modify_module_name = file_utils.get_unique_dag_module_name(edge_filename)
        assert modify_module_name == expected_modification
