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
import os.path
import unittest
from pathlib import Path
from unittest import mock

import pytest

from airflow.utils.file import correct_maybe_zipped, find_path_from_directory, open_maybe_zipped
from tests.models import TEST_DAGS_FOLDER


class TestCorrectMaybeZipped(unittest.TestCase):
    @mock.patch("zipfile.is_zipfile")
    def test_correct_maybe_zipped_normal_file(self, mocked_is_zipfile):
        path = '/path/to/some/file.txt'
        mocked_is_zipfile.return_value = False

        dag_folder = correct_maybe_zipped(path)

        assert dag_folder == path

    @mock.patch("zipfile.is_zipfile")
    def test_correct_maybe_zipped_normal_file_with_zip_in_name(self, mocked_is_zipfile):
        path = '/path/to/fakearchive.zip.other/file.txt'
        mocked_is_zipfile.return_value = False

        dag_folder = correct_maybe_zipped(path)

        assert dag_folder == path

    @mock.patch("zipfile.is_zipfile")
    def test_correct_maybe_zipped_archive(self, mocked_is_zipfile):
        path = '/path/to/archive.zip/deep/path/to/file.txt'
        mocked_is_zipfile.return_value = True

        dag_folder = correct_maybe_zipped(path)

        assert mocked_is_zipfile.call_count == 1
        (args, kwargs) = mocked_is_zipfile.call_args_list[0]
        assert '/path/to/archive.zip' == args[0]

        assert dag_folder == '/path/to/archive.zip'


class TestOpenMaybeZipped(unittest.TestCase):
    def test_open_maybe_zipped_normal_file(self):
        test_file_path = os.path.join(TEST_DAGS_FOLDER, "no_dags.py")
        with open_maybe_zipped(test_file_path, 'r') as test_file:
            content = test_file.read()
        assert isinstance(content, str)

    def test_open_maybe_zipped_normal_file_with_zip_in_name(self):
        path = '/path/to/fakearchive.zip.other/file.txt'
        with mock.patch('builtins.open', mock.mock_open(read_data="data")) as mock_file:
            open_maybe_zipped(path)
            mock_file.assert_called_once_with(path, mode='r')

    def test_open_maybe_zipped_archive(self):
        test_file_path = os.path.join(TEST_DAGS_FOLDER, "test_zip.zip", "test_zip.py")
        with open_maybe_zipped(test_file_path, 'r') as test_file:
            content = test_file.read()
        assert isinstance(content, str)


class TestListPyFilesPath:
    @pytest.fixture()
    def test_dir(self, tmp_path):
        # create test tree with symlinks
        source = os.path.join(tmp_path, "folder")
        target = os.path.join(tmp_path, "symlink")
        py_file = os.path.join(source, "hello_world.py")
        ignore_file = os.path.join(tmp_path, ".airflowignore")
        os.mkdir(source)
        os.symlink(source, target)
        # write ignore files
        with open(ignore_file, 'w') as f:
            f.write("folder")
        # write sample pyfile
        with open(py_file, 'w') as f:
            f.write("print('hello world')")
        return tmp_path

    def test_find_path_from_directory_regex_ignore(self):
        should_ignore = [
            "test_invalid_cron.py",
            "test_invalid_param.py",
            "test_ignore_this.py",
        ]
        files = find_path_from_directory(TEST_DAGS_FOLDER, ".airflowignore")

        assert files
        assert all(os.path.basename(file) not in should_ignore for file in files)

    def test_find_path_from_directory_glob_ignore(self):
        should_ignore = [
            "test_invalid_cron.py",
            "test_invalid_param.py",
            "test_ignore_this.py",
            "test_prev_dagrun_dep.py",
            "test_retry_handling_job.py",
            "test_nested_dag.py",
            ".airflowignore",
        ]
        should_not_ignore = [
            "test_on_kill.py",
            "test_dont_ignore_this.py",
        ]
        files = list(find_path_from_directory(TEST_DAGS_FOLDER, ".airflowignore_glob", "glob"))

        assert files
        assert all(os.path.basename(file) not in should_ignore for file in files)
        assert len(list(filter(lambda file: os.path.basename(file) in should_not_ignore, files))) == len(
            should_not_ignore
        )

    def test_find_path_from_directory_respects_symlinks_regexp_ignore(self, test_dir):
        ignore_list_file = ".airflowignore"
        found = list(find_path_from_directory(test_dir, ignore_list_file))

        assert os.path.join(test_dir, "symlink", "hello_world.py") in found
        assert os.path.join(test_dir, "folder", "hello_world.py") not in found

    def test_find_path_from_directory_respects_symlinks_glob_ignore(self, test_dir):
        ignore_list_file = ".airflowignore"
        found = list(find_path_from_directory(test_dir, ignore_list_file, ignore_file_syntax="glob"))

        assert os.path.join(test_dir, "symlink", "hello_world.py") in found
        assert os.path.join(test_dir, "folder", "hello_world.py") not in found

    def test_find_path_from_directory_fails_on_recursive_link(self, test_dir):
        # add a recursive link
        recursing_src = os.path.join(test_dir, "folder2", "recursor")
        recursing_tgt = os.path.join(test_dir, "folder2")
        os.mkdir(recursing_tgt)
        os.symlink(recursing_tgt, recursing_src)

        ignore_list_file = ".airflowignore"

        try:
            list(find_path_from_directory(test_dir, ignore_list_file, ignore_file_syntax="glob"))
            assert False, "Walking a self-recursive tree should fail"
        except RuntimeError as err:
            assert str(err) == (
                f"Detected recursive loop when walking DAG directory {test_dir}: "
                f"{Path(recursing_tgt).resolve()} has appeared more than once."
            )
