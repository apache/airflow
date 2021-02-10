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

from airflow.upgrade.rules.chain_between_dag_and_operator_not_allowed_rule import \
    ChainBetweenDAGAndOperatorNotAllowedRule


@contextmanager
def create_temp_file(mock_list_files, lines):
    with NamedTemporaryFile("w+") as temp_file:
        mock_list_files.return_value = [temp_file.name]
        temp_file.writelines("\n".join(lines))
        temp_file.flush()
        yield temp_file


@mock.patch("airflow.upgrade.rules.chain_between_dag_and_operator_not_allowed_rule.list_py_file_paths")
class TestChainBetweenDAGAndOperatorNotAllowedRule(TestCase):
    msg_template = "{} Affected file: {} (line {})"

    def test_rule_metadata(self, _):
        rule = ChainBetweenDAGAndOperatorNotAllowedRule()
        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

    def test_valid_check(self, mock_list_files):
        lines = ["with DAG('my_dag') as dag:",
                 "    dummy1 = DummyOperator(task_id='dummy1')",
                 "    dummy2 = DummyOperator(task_id='dummy2')",
                 "    dummy1 >> dummy2"]

        with create_temp_file(mock_list_files, lines):
            rule = ChainBetweenDAGAndOperatorNotAllowedRule()
            msgs = rule.check()
            assert 0 == len(msgs)

    def test_invalid_check(self, mock_list_files):
        lines = ["my_dag1 = DAG('my_dag')",
                 "dummy = DummyOperator(task_id='dummy')",
                 "my_dag1 >> dummy"]

        with create_temp_file(mock_list_files, lines) as temp_file:
            rule = ChainBetweenDAGAndOperatorNotAllowedRule()
            msgs = rule.check()
            expected_messages = [self.msg_template.format(rule.title, temp_file.name, 3)]
            assert expected_messages == msgs

    def test_invalid_check_no_var_rshift(self, mock_list_files):
        lines = ["DAG('my_dag') >> DummyOperator(task_id='dummy')"]

        with create_temp_file(mock_list_files, lines) as temp_file:
            rule = ChainBetweenDAGAndOperatorNotAllowedRule()
            msgs = rule.check()
            expected_messages = [self.msg_template.format(rule.title, temp_file.name, 1)]
            assert expected_messages == msgs

    def test_invalid_check_no_var_lshift(self, mock_list_files):
        lines = ["DummyOperator(",
                 "task_id='dummy') << DAG('my_dag')"]

        with create_temp_file(mock_list_files, lines) as temp_file:
            rule = ChainBetweenDAGAndOperatorNotAllowedRule()
            msgs = rule.check()
            expected_messages = [self.msg_template.format(rule.title, temp_file.name, 2)]
            assert expected_messages == msgs

    def test_invalid_check_multiline(self, mock_list_files):
        lines = ["dag = \\",
                 "    DAG('my_dag')",
                 "dummy = DummyOperator(task_id='dummy')",
                 "",
                 "dag >> \\",
                 "dummy"]

        with create_temp_file(mock_list_files, lines) as temp_file:
            rule = ChainBetweenDAGAndOperatorNotAllowedRule()
            msgs = rule.check()
            expected_messages = [self.msg_template.format(rule.title, temp_file.name, 5)]
            assert expected_messages == msgs

    def test_invalid_check_multiline_lshift(self, mock_list_files):
        lines = ["dag = \\",
                 "    DAG('my_dag')",
                 "dummy = DummyOperator(task_id='dummy')",
                 "",
                 "dummy << \\",
                 "dag"]

        with create_temp_file(mock_list_files, lines) as temp_file:
            rule = ChainBetweenDAGAndOperatorNotAllowedRule()
            msgs = rule.check()
            expected_messages = [self.msg_template.format(rule.title, temp_file.name, 6)]
            assert expected_messages == msgs
