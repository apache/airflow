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
import json
from tempfile import NamedTemporaryFile
from tests.compat import mock

import pytest

from airflow.bin import cli
from airflow.upgrade.rules.base_rule import BaseRule

MESSAGES = ["msg1", "msg2"]


class MockRule(BaseRule):
    title = "title"
    description = "description"

    def check(self):
        return MESSAGES


class TestJSONFormatter:
    @mock.patch("airflow.upgrade.checker.ALL_RULES", [MockRule()])
    @mock.patch("airflow.upgrade.checker.logging.disable")  # mock to avoid side effects
    def test_output(self, _):
        expected = [
            {
                "rule": MockRule.__name__,
                "title": MockRule.title,
                "messages": MESSAGES,
            }
        ]
        parser = cli.CLIFactory.get_parser()
        with NamedTemporaryFile("w+", suffix=".json") as temp:
            with pytest.raises(SystemExit):
                args = parser.parse_args(['upgrade_check', '-s', temp.name])
                args.func(args)
            content = temp.read()

        assert json.loads(content) == expected
