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
import zipfile
from pathlib import Path
from unittest import mock

import pytest

from airflow.utils import file as file_utils
from airflow.utils.file import correct_maybe_zipped, find_path_from_directory, open_maybe_zipped
from tests.models import TEST_DAGS_FOLDER

from dev.tests_common.test_utils.config import conf_vars


def might_contain_dag(file_path: str, zip_file: zipfile.ZipFile | None = None):
    return False


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
        assert "/path/to/archive.zip" == args[0]

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

    def test_open_maybe_zipped_archive(self):
        test_file_path = os.path.join(TEST_DAGS_FOLDER, "test_zip.zip", "test_zip.py")
        with open_maybe_zipped(test_file_path, "r") as test_file:
            content = test_file.read()
        assert isinstance(content, str)


class TestListPyFilesPath:
    @pytest.fixture
    def test_dir(self, tmp_path):
        # create test tree with symlinks
        source = os.path.join(tmp_path, "folder")
        target = os.path.join(tmp_path, "symlink")
        py_file = os.path.join(source, "hello_world.py")
        ignore_file = os.path.join(tmp_path, ".airflowignore")
        os.mkdir(source)
        os.symlink(source, target)
        # write ignore files
        with open(ignore_file, "w") as f:
            f.write("folder")
        # write sample pyfile
        with open(py_file, "w") as f:
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
        assert sum(1 for file in files if os.path.basename(file) in should_not_ignore) == len(
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

        error_message = (
            f"Detected recursive loop when walking DAG directory {test_dir}: "
            f"{Path(recursing_tgt).resolve()} has appeared more than once."
        )
        with pytest.raises(RuntimeError, match=error_message):
            list(find_path_from_directory(test_dir, ignore_list_file, ignore_file_syntax="glob"))

    def test_might_contain_dag_with_default_callable(self):
        file_path_with_dag = os.path.join(TEST_DAGS_FOLDER, "test_scheduler_dags.py")

        assert file_utils.might_contain_dag(file_path=file_path_with_dag, safe_mode=True)

    @conf_vars({("core", "might_contain_dag_callable"): "tests.utils.test_file.might_contain_dag"})
    def test_might_contain_dag(self):
        """Test might_contain_dag_callable"""
        file_path_with_dag = os.path.join(TEST_DAGS_FOLDER, "test_scheduler_dags.py")

        # There is a DAG defined in the file_path_with_dag, however, the might_contain_dag_callable
        # returns False no matter what, which is used to test might_contain_dag_callable actually
        # overrides the default function
        assert not file_utils.might_contain_dag(file_path=file_path_with_dag, safe_mode=True)

        # With safe_mode is False, the user defined callable won't be invoked
        assert file_utils.might_contain_dag(file_path=file_path_with_dag, safe_mode=False)

    def test_get_modules(self):
        file_path = os.path.join(TEST_DAGS_FOLDER, "test_imports.py")

        modules = list(file_utils.iter_airflow_imports(file_path))

        assert len(modules) == 4
        assert "airflow.utils" in modules
        assert "airflow.decorators" in modules
        assert "airflow.models" in modules
        assert "airflow.sensors" in modules
        # this one is a local import, we don't want it.
        assert "airflow.local_import" not in modules
        # this one is in a comment, we don't want it
        assert "airflow.in_comment" not in modules
        # we don't want imports under conditions
        assert "airflow.if_branch" not in modules
        assert "airflow.else_branch" not in modules

    def test_get_modules_from_invalid_file(self):
        file_path = os.path.join(TEST_DAGS_FOLDER, "README.md")  # just getting a non-python file

        # should not error
        modules = list(file_utils.iter_airflow_imports(file_path))

        assert len(modules) == 0
