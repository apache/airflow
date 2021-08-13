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

from __future__ import absolute_import

from airflow.configuration import conf, AIRFLOW_HOME
from airflow.upgrade.rules.base_rule import BaseRule


class LoggingConfigurationRule(BaseRule):
    title = "Logging configuration has been moved to new section"

    description = "The logging configurations have been moved from [core] to the new [logging] section."

    def check(self):
        logging_configs = [
            ("base_log_folder", "{}/logs".format(AIRFLOW_HOME)),
            ("remote_logging", "False"),
            ("remote_log_conn_id", ""),
            ("remote_base_log_folder", ""),
            ("encrypt_s3_logs", "False"),
            ("logging_level", "INFO"),
            ("fab_logging_level", "WARN"),
            ("logging_config_class", ""),
            ("colored_console_log", "True"),
            (
                "colored_log_format",
                "[%(blue)s%(asctime)s%(reset)s] {%(blue)s%(filename)s:%(reset)s%(lineno)d} "
                "%(log_color)s%(levelname)s%(reset)s - %(log_color)s%(message)s%(reset)s",
            ),
            (
                "colored_formatter_class",
                "airflow.utils.log.colored_log.CustomTTYColoredFormatter",
            ),
            (
                "log_format",
                "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
            ),
            ("simple_log_format", "%(asctime)s %(levelname)s - %(message)s"),
            ("task_log_prefix_template", ""),
            (
                "log_filename_template",
                "{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log",
            ),
            ("log_processor_filename_template", "{{ filename }}.log"),
            (
                "dag_processor_manager_log_location",
                "{}/logs/dag_processor_manager/dag_processor_manager.log".format(
                    AIRFLOW_HOME
                ),
            ),
            ("task_log_reader", "task"),
        ]

        mismatches = []
        for logging_config, default in logging_configs:
            if not conf.has_option("logging", logging_config) and conf.has_option(
                "core", logging_config
            ):
                existing_config = conf.get("core", logging_config)
                if existing_config != default:
                    mismatches.append(
                        "{} has been moved from [core] to a the new [logging] section.".format(
                            logging_config
                        )
                    )

        return mismatches
