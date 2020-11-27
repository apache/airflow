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

import tempfile
from unittest import mock

import pytest

from airflow.upgrade.config import UpgradeConfig

dummy_config = """\
ignored_rules:
- MockRule1
- MockRule2
custom_rules:
- custom.rule.one
"""


class MockRule1(object):
    pass


class MockRule2(object):
    pass


class MockRule3(object):
    pass


@pytest.fixture(scope="class")
def config():
    with tempfile.NamedTemporaryFile("w+") as f:
        f.write(dummy_config)
        f.flush()
        yield UpgradeConfig.read(f.name)


class TestUpgradeConfig:
    def test_read(self, config):
        assert "ignored_rules" in config._raw_config
        assert "custom_rules" in config._raw_config

        assert len(config._raw_config["ignored_rules"]) == 2
        assert len(config._raw_config["custom_rules"]) == 1

    def test_ignore(self, config):
        rules = [MockRule1(), MockRule2(), MockRule3()]
        assert config.remove_ignored_rules(rules) == [rules[-1]]

    @mock.patch("airflow.upgrade.config.import_string")
    def test_register_custom_rules(self, mock_import, config):
        rules = [MockRule1, MockRule2]
        mock_import.return_value = MockRule3
        rules = config.register_custom_rules(rules)
        mock_import.assert_called_once_with("custom.rule.one")
        assert len(rules) == 3
        assert isinstance(rules[-1], MockRule3)
