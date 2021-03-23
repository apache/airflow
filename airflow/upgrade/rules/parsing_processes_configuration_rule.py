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


class ParsingProcessesConfigurationRule(BaseRule):
    title = "Rename max_threads to parsing_processes"

    description = "The max_threads configuration in the [scheduler] section was renamed to parsing_processes."

    def check(self):
        default = 2

        old_config_exists = conf.has_option("scheduler", "max_threads")
        new_config_exists = conf.has_option("scheduler", "parsing_processes")

        if old_config_exists and not new_config_exists:
            old_config_value = conf.get("scheduler", "max_threads")
            if old_config_value == default:
                return None
            return ["Please rename the max_threads configuration in the "
                    "[scheduler] section to parsing_processes."]
        return None
