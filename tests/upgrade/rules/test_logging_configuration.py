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
    @conf_vars(
        {
            ("core", "base_log_folder"): "DUMMY",
            ("core", "remote_logging"): "DUMMY",
            ("core", "remote_log_conn_id"): "DUMMY",
            ("core", "remote_base_log_folder"): "DUMMY",
            ("core", "encrypt_s3_logs"): "DUMMY",
            ("core", "logging_level"): "DUMMY",
            ("core", "fab_logging_level"): "DUMMY",
            ("core", "logging_config_class"): "DUMMY",
            ("core", "colored_console_log"): "DUMMY",
            ("core", "simple_log_format"): "DUMMY",
            ("core", "task_log_prefix_template"): "DUMMY",
            ("core", "log_processor_filename_template"): "DUMMY",
            ("core", "task_log_reader"): "DUMMY",
        }
    )
    def test_check(self):
        rule = LoggingConfigurationRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        # Remove the fallback option
        conf.remove_option("logging", "base_log_folder")
        conf.remove_option("logging", "remote_logging")
        conf.remove_option("logging", "remote_log_conn_id")
        conf.remove_option("logging", "remote_base_log_folder")
        conf.remove_option("logging", "encrypt_s3_logs")
        conf.remove_option("logging", "logging_level")
        conf.remove_option("logging", "fab_logging_level")
        conf.remove_option("logging", "logging_config_class")
        conf.remove_option("logging", "colored_console_log")
        conf.remove_option("logging", "simple_log_format")
        conf.remove_option("logging", "task_log_prefix_template")
        conf.remove_option("logging", "log_processor_filename_template")
        conf.remove_option("logging", "task_log_reader")
        msg = [
            "base_log_folder has been moved from [core] to a the new [logging] section.",
            "remote_logging has been moved from [core] to a the new [logging] section.",
            "remote_log_conn_id has been moved from [core] to a the new [logging] section.",
            "remote_base_log_folder has been moved from [core] to a the new [logging] section.",
            "encrypt_s3_logs has been moved from [core] to a the new [logging] section.",
            "logging_level has been moved from [core] to a the new [logging] section.",
            "fab_logging_level has been moved from [core] to a the new [logging] section.",
            "logging_config_class has been moved from [core] to a the new [logging] section.",
            "colored_console_log has been moved from [core] to a the new [logging] section.",
            "simple_log_format has been moved from [core] to a the new [logging] section.",
            "task_log_prefix_template has been moved from [core] to a the new [logging] section.",
            "log_processor_filename_template has been moved from [core] to a the new [logging] section.",
            "task_log_reader has been moved from [core] to a the new [logging] section.",
        ]
        response = rule.check()
        assert response == msg
