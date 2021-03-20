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

from airflow.configuration import conf
from airflow.upgrade.rules.parsing_processes_configuration_rule import ParsingProcessesConfigurationRule
from tests.test_utils.config import conf_vars


class TestParsingProcessesConfigurationRule(TestCase):
    @conf_vars(
        {
            ("scheduler", "parsing_processes"): "DUMMY",
        }
    )
    def test_check_new_config(self):
        rule = ParsingProcessesConfigurationRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        # Remove the fallback option
        conf.deprecated_options.get("scheduler", {}).pop("parsing_processes", "")
        conf.remove_option("scheduler", "parsing_processes")

        response = rule.check()
        assert response is None

    @conf_vars(
        {
            ("scheduler", "max_threads"): "DUMMY",
        }
    )
    def test_check_old_config(self):
        rule = ParsingProcessesConfigurationRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        # Remove the fallback option
        conf.deprecated_options.get("scheduler", {}).pop("parsing_processes", "")
        conf.remove_option("scheduler", "parsing_processes")

        response = rule.check()
        assert response == \
               ["Please rename the max_threads configuration in the "
                "[scheduler] section to parsing_processes."]
