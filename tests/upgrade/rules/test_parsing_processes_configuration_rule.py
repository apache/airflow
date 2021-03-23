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
from unittest.mock import patch

from airflow.upgrade.rules.parsing_processes_configuration_rule import ParsingProcessesConfigurationRule


class TestParsingProcessesConfigurationRule(TestCase):
    @patch('airflow.configuration.conf.has_option')
    def test_check_new_config(self, conf_has_option):
        rule = ParsingProcessesConfigurationRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        conf_has_option.side_effect = [False, True]

        response = rule.check()
        assert response is None

    @patch('airflow.configuration.conf.get')
    @patch('airflow.configuration.conf.has_option')
    def test_check_old_config(self, conf_has_option, conf_get):
        rule = ParsingProcessesConfigurationRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        conf_has_option.side_effect = [True, False]
        conf_get.side_effect = ["DUMMY"]

        response = rule.check()
        assert response == \
               ["Please rename the max_threads configuration in the "
                "[scheduler] section to parsing_processes."]
