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
from unittest import TestCase

from airflow.upgrade.rules.logging_configuration import LoggingConfigurationRule
from airflow.configuration import conf
from tests.test_utils.config import conf_vars


class TestLoggingConfigurationRule(TestCase):
    @conf_vars({("core", "logging_level"): "INFO"})
    def test_invalid_check(self):
        rule = LoggingConfigurationRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        # Remove the fallback option
        conf.remove_option("logging", "logging_level")
        msg = "The logging configurations have been to moved from [core] to the new [logging] section."
        response = rule.check()
        assert response == msg

    @conf_vars({("logging", "logging_level"): "INFO"})
    def test_valid_check(self):
        rule = LoggingConfigurationRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        response = rule.check()
        assert response is None
