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
from pprint import pformat
from unittest import mock

import pytest

from airflow.utils import file as file_utils
from airflow.utils.file import (
    correct_maybe_zipped,
    find_path_from_directory,
    list_py_file_paths,
    open_maybe_zipped,
)

from tests_common.test_utils.config import conf_vars
from unit.models import TEST_DAGS_FOLDER

TEST_DAG_FOLDER = os.environ["AIRFLOW__CORE__DAGS_FOLDER"]


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
        should_ignore = {
            "should_ignore_this.py",
            "test_explicit_ignore.py",
            "test_invalid_cron.py",
            "test_invalid_param.py",
            "test_ignore_this.py",
            "test_prev_dagrun_dep.py",
            "test_nested_dag.py",
            ".airflowignore",
        }
        should_not_ignore = {
            "test_on_kill.py",
            "test_negate_ignore.py",
            "test_dont_ignore_this.py",
            "test_nested_negate_ignore.py",
            "test_explicit_dont_ignore.py",
        }
        actual_files = list(find_path_from_directory(TEST_DAGS_FOLDER, ".airflowignore_glob", "glob"))

        assert actual_files
        assert all(os.path.basename(file) not in should_ignore for file in actual_files)
        actual_included_filenames = set(
            [os.path.basename(f) for f in actual_files if os.path.basename(f) in should_not_ignore]
        )
        assert actual_included_filenames == should_not_ignore, (
            f"actual_included_filenames: {pformat(actual_included_filenames)}\nexpected_included_filenames: {pformat(should_not_ignore)}"
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

    def test_airflowignore_negation_unignore_subfolder_file_glob(self, tmp_path):
        """Ensure negation rules can unignore a subfolder and a file inside it when using glob syntax.

        Patterns:
          *                     -> ignore everything
          !subfolder/           -> unignore the subfolder (must match directory rule)
          !subfolder/keep.py    -> unignore a specific file inside the subfolder
        """
        dags_root = tmp_path / "dags"
        (dags_root / "subfolder").mkdir(parents=True)
        # files
        (dags_root / "drop.py").write_text("raise Exception('ignored')\n")
        (dags_root / "subfolder" / "keep.py").write_text("# should be discovered\n")
        (dags_root / "subfolder" / "drop.py").write_text("raise Exception('ignored')\n")

        (dags_root / ".airflowignore").write_text(
            "\n".join(
                [
                    "*",
                    "!subfolder/",
                    "!subfolder/keep.py",
                ]
            )
        )

        detected = set()
        for raw in find_path_from_directory(dags_root, ".airflowignore", "glob"):
            p = Path(raw)
            if p.is_file() and p.suffix == ".py":
                detected.add(p.relative_to(dags_root).as_posix())

        assert detected == {"subfolder/keep.py"}

    def test_airflowignore_negation_nested_with_globstar(self, tmp_path):
        """Negation with ** should work for nested subfolders."""
        dags_root = tmp_path / "dags"
        nested = dags_root / "a" / "b" / "subfolder"
        nested.mkdir(parents=True)

        # files
        (dags_root / "ignore_top.py").write_text("raise Exception('ignored')\n")
        (nested / "keep.py").write_text("# should be discovered\n")
        (nested / "drop.py").write_text("raise Exception('ignored')\n")

        (dags_root / ".airflowignore").write_text(
            "\n".join(
                [
                    "*",
                    "!a/",
                    "!a/b/",
                    "!**/subfolder/",
                    "!**/subfolder/keep.py",
                    "drop.py",
                ]
            )
        )

        detected = set()
        for raw in find_path_from_directory(dags_root, ".airflowignore", "glob"):
            p = Path(raw)
            if p.is_file() and p.suffix == ".py":
                detected.add(p.relative_to(dags_root).as_posix())

        assert detected == {"a/b/subfolder/keep.py"}

    @conf_vars({("core", "might_contain_dag_callable"): "unit.utils.test_file.might_contain_dag"})
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

    def test_list_py_file_paths(self, test_zip_path):
        detected_files = set()
        expected_files = set()
        # No_dags is empty, _invalid_ is ignored by .airflowignore
        ignored_files = {
            "no_dags.py",
            "should_ignore_this.py",
            "test_explicit_ignore.py",
            "test_invalid_cron.py",
            "test_invalid_dup_task.py",
            "test_ignore_this.py",
            "test_invalid_param.py",
            "test_invalid_param2.py",
            "test_invalid_param3.py",
            "test_invalid_param4.py",
            "test_nested_dag.py",
            "test_imports.py",
            "test_nested_negate_ignore.py",
            "file_no_airflow_dag.py",  # no_dag test case in test_zip folder
            "test.py",  # no_dag test case in test_zip_module folder
            "__init__.py",
        }
        for root, _, files in os.walk(TEST_DAG_FOLDER):
            for file_name in files:
                if file_name.endswith((".py", ".zip")):
                    if file_name not in ignored_files:
                        expected_files.add(f"{root}/{file_name}")
        detected_files = set(list_py_file_paths(TEST_DAG_FOLDER))
        assert detected_files == expected_files, (
            f"Detected files mismatched expected files:\ndetected_files: {pformat(detected_files)}\nexpected_files: {pformat(expected_files)}"
        )


@pytest.mark.parametrize(
    "edge_filename, expected_modification",
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
