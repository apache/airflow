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

from tests.compat import mock

import pytest

from airflow.plugins_manager import AirflowPlugin
from airflow.upgrade.rules.stat_name_handler_not_supported import StatNameHandlerNotSupportedRule
from tests.plugins.test_plugin import MockPluginA, MockPluginB
from tests.test_utils.config import conf_vars


class MockPluginWithStatNameHandler(AirflowPlugin):
    stat_name_handler = "stat_name_handler"


PLUGINS_WITH_STAT_NAME_HANDLER = [MockPluginWithStatNameHandler, MockPluginA]
PLUGINS_WITHOUT_STAT_NAME_HANDLER = [MockPluginA, MockPluginB]
CHECK_ERR_MSG = (
    'stat_name_handler field is no longer supported in AirflowTestPlugin.'
    ' stat_name_handler option in [scheduler] section in airflow.cfg should'
    ' be used to achieve the same effect.'
)
TEST_CONFIG = {("scheduler", "stat_name_handler"): "SCHEDULER"}


class TestStatNameHandlerNotSupportedRule:
    @pytest.mark.parametrize(
        'plugins, test_config, expected',
        [
            (PLUGINS_WITH_STAT_NAME_HANDLER, {}, CHECK_ERR_MSG),
            (PLUGINS_WITHOUT_STAT_NAME_HANDLER, {}, None),
            (PLUGINS_WITHOUT_STAT_NAME_HANDLER, TEST_CONFIG, None),
            (PLUGINS_WITH_STAT_NAME_HANDLER, TEST_CONFIG, None),
        ]
    )
    @mock.patch("airflow.upgrade.rules.stat_name_handler_not_supported.plugins_manager")
    def test_check_failed(self, mocked_plugins_manager, plugins, test_config, expected):
        mocked_plugins_manager.plugins = plugins

        with conf_vars(test_config):
            rule = StatNameHandlerNotSupportedRule()

            assert isinstance(rule.description, str)
            assert isinstance(rule.title, str)
            msg = rule.check()

            assert msg == expected
