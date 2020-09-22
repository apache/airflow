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

from tempfile import NamedTemporaryFile
from tests.compat import mock

from airflow.upgrade.rules.import_changes import ImportChange, ImportChangesRule

OLD_CLASS = "OldOperator"
NEW_CLASS = "NewOperator"
PROVIDER = "dummy"
OLD_PATH = "airflow.contrib." + OLD_CLASS
NEW_PATH = "airflow.providers." + PROVIDER + "." + NEW_CLASS


class TestImportChange:
    def test_info(self):
        change = ImportChange(
            old_path=OLD_PATH, new_path=NEW_PATH, providers_package=PROVIDER
        )
        assert change.info(
            "file.py"
        ) == "Using `{}` will be replaced by `{}` and requires `{}` providers package. " \
             "Affected file: file.py".format(OLD_PATH, NEW_PATH, PROVIDER)
        assert change.old_class == OLD_CLASS
        assert change.new_class == NEW_CLASS

    def test_from_new_old_paths(self):
        paths_tuple = (NEW_PATH, OLD_PATH)
        change = ImportChange.from_new_old_paths(*paths_tuple)
        assert change.info() == "Using `{}` will be replaced by `{}` and requires `{}` " \
                                "providers package".format(OLD_PATH, NEW_PATH, PROVIDER)


class TestImportChangesRule:
    @mock.patch("airflow.upgrade.rules.import_changes.list_py_file_paths")
    @mock.patch(
        "airflow.upgrade.rules.import_changes.ImportChangesRule.ALL_CHANGES",
        [ImportChange.from_new_old_paths(NEW_PATH, OLD_PATH)],
    )
    def test_check(self, mock_list_files):
        with NamedTemporaryFile("w+") as temp:
            mock_list_files.return_value = [temp.name]

            temp.write("from airflow.contrib import %s" % OLD_CLASS)
            temp.flush()
            msgs = ImportChangesRule().check()

        assert len(msgs) == 1
        msg = msgs[0]
        assert temp.name in msg
        assert OLD_PATH in msg
        assert OLD_CLASS in msg
        assert "requires `{}`".format(PROVIDER) in msg
