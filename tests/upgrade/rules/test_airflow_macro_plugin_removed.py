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
from contextlib import contextmanager
from unittest import TestCase

from tempfile import NamedTemporaryFile
from tests.compat import mock

from airflow.upgrade.rules.airflow_macro_plugin_removed import AirflowMacroPluginRemovedRule


@contextmanager
def create_temp_file(mock_list_files, lines):
    with NamedTemporaryFile("w+") as temp_file:
        mock_list_files.return_value = [temp_file.name]
        temp_file.writelines("\n".join(lines))
        temp_file.flush()
        yield temp_file


@mock.patch("airflow.upgrade.rules.airflow_macro_plugin_removed.list_py_file_paths")
class TestAirflowMacroPluginRemovedRule(TestCase):
    def test_valid_check(self, mock_list_files):
        lines = ["import foo.bar.baz"]
        with create_temp_file(mock_list_files, lines):
            rule = AirflowMacroPluginRemovedRule()
            assert isinstance(rule.description, str)
            assert isinstance(rule.title, str)

            msgs = rule.check()
            assert 0 == len(msgs)

    def test_invalid_check(self, mock_list_files):
        lines = [
            "import airflow.AirflowMacroPlugin",
            "from airflow import AirflowMacroPlugin",
        ]
        with create_temp_file(mock_list_files, lines) as temp_file:

            rule = AirflowMacroPluginRemovedRule()

            assert isinstance(rule.description, str)
            assert isinstance(rule.title, str)

            msgs = rule.check()
            assert 2 == len(msgs)

            base_message = "airflow.AirflowMacroPlugin will be removed. Affected file: {}".format(
                temp_file.name
            )
            expected_messages = [
                "{} (line {})".format(base_message, line_number) for line_number in [1, 2]
            ]
            assert expected_messages == msgs
