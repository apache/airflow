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

import sys
from tempfile import NamedTemporaryFile

import pytest

from tests.compat import mock

from airflow.upgrade.rules.import_changes import ImportChange, ImportChangesRule

OLD_CLASS = "OldOperator"
NEW_CLASS = "NewOperator"
PROVIDER = "dummy"
OLD_PATH = "airflow.contrib." + OLD_CLASS
NEW_PATH = "airflow.providers." + PROVIDER + "." + NEW_CLASS
OLD_PATH_WITHOUT_CLASS_NAME = "airflow.contrib"


class TestImportChange:
    def test_info(self):
        change = ImportChange(
            old_path=OLD_PATH, new_path=NEW_PATH, providers_package=PROVIDER
        )
        assert change.info(
            "file.py"
        ) == "Using `{}` should be replaced by `{}`. Affected file: file.py".format(OLD_PATH, NEW_PATH)
        assert change.old_path_without_classname == OLD_PATH_WITHOUT_CLASS_NAME
        assert change.new_class == NEW_CLASS

    def test_from_new_old_paths(self):
        paths_tuple = (NEW_PATH, OLD_PATH)
        change = ImportChange.from_new_old_paths(*paths_tuple)
        assert change.info() == "Using `{}` should be replaced by `{}`".format(OLD_PATH, NEW_PATH)


class TestImportChangesRule:
    @mock.patch("airflow.upgrade.rules.import_changes.list_py_file_paths")
    @mock.patch(
        "airflow.upgrade.rules.import_changes.ImportChangesRule.ALL_CHANGES",
        [ImportChange.from_new_old_paths(NEW_PATH, OLD_PATH)],
    )
    def test_check(self, mock_list_files):
        with NamedTemporaryFile("w+", suffix=".py") as temp:
            mock_list_files.return_value = [temp.name]

            temp.write("from airflow.contrib import %s" % OLD_CLASS)
            temp.flush()
            msgs = list(ImportChangesRule().check())

        assert len(msgs) == 2
        msg = msgs[0]
        assert msg == 'Please install `apache-airflow-backport-providers-dummy`'
        msg = msgs[1]
        assert temp.name in msg
        assert OLD_PATH in msg
        assert OLD_CLASS in msg

    @mock.patch("airflow.upgrade.rules.import_changes.list_py_file_paths")
    @mock.patch(
        "airflow.upgrade.rules.import_changes.ImportChangesRule.ALL_CHANGES",
        [ImportChange.from_new_old_paths(NEW_PATH, OLD_PATH)],
    )
    def test_non_py_files_are_ignored(self, mock_list_files):
        with NamedTemporaryFile("w+", suffix=".txt") as temp:
            mock_list_files.return_value = [temp.name]

            temp.write("from airflow.contrib import %s" % OLD_CLASS)
            temp.flush()
            msgs = list(ImportChangesRule().check())
        assert len(msgs) == 0

    @mock.patch("airflow.upgrade.rules.import_changes.list_py_file_paths")
    @mock.patch(
        "airflow.upgrade.rules.import_changes.ImportChangesRule.ALL_CHANGES",
        [ImportChange.from_new_old_paths(NEW_PATH, OLD_PATH)],
    )
    @pytest.mark.skipif(
        sys.version_info.major == 2,
        reason="Test is irrelevant in Python 2.7 because of unicode differences"
    )
    def test_decode_error_are_handled(self, mock_list_files):
        with NamedTemporaryFile("wb+", suffix=".py") as temp:
            mock_list_files.return_value = [temp.name]

            temp.write(b"from airflow \x03\x96")
            temp.flush()
            msgs = list(ImportChangesRule().check())
        assert msgs[0] == "Unable to read python file {}".format(temp.name)
