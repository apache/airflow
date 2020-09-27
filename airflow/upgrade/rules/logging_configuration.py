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

from airflow.configuration import conf
from airflow.upgrade.rules.base_rule import BaseRule


class LoggingConfigurationRule(BaseRule):
    title = "Logging configuration has been moved to new section"

    description = "The logging configurations have been moved from [core] to the new [logging] section."

    def check(self):
        if not conf.has_option("logging", "logging_level"):
            return (
                "The following configurations have been to moved from [core] to the new [logging] section. \n"
                "- base_log_folder \n"
                "- remote_logging \n"
                "- remote_log_conn_id \n"
                "- remote_base_log_folder \n"
                "- encrypt_s3_logs \n"
                "- logging_level \n"
                "- fab_logging_level \n"
                "- logging_config_class \n"
                "- colored_console_log \n"
                "- colored_log_format \n"
                "- colored_formatter_class \n"
                "- log_format \n"
                "- simple_log_format \n"
                "- task_log_prefix_template \n"
                "- log_filename_template \n"
                "- log_processor_filename_template \n"
                "- dag_processor_manager_log_location \n"
                "- task_log_reader \n"
            )
