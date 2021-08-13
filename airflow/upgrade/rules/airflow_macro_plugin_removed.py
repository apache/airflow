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

from airflow import conf
from airflow.upgrade.rules.base_rule import BaseRule
from airflow.utils.dag_processing import list_py_file_paths


class AirflowMacroPluginRemovedRule(BaseRule):

    title = "Remove airflow.AirflowMacroPlugin class"

    description = "The airflow.AirflowMacroPlugin class has been removed."

    MACRO_PLUGIN_CLASS = "airflow.AirflowMacroPlugin"

    def _change_info(self, file_path, line_number):
        return "{} will be removed. Affected file: {} (line {})".format(
            self.MACRO_PLUGIN_CLASS, file_path, line_number
        )

    def _check_file(self, file_path):
        problems = []
        class_name_to_check = self.MACRO_PLUGIN_CLASS.split(".")[-1]
        with open(file_path, "r") as file_pointer:
            try:
                for line_number, line in enumerate(file_pointer, 1):
                    if class_name_to_check in line:
                        problems.append(self._change_info(file_path, line_number))
            except UnicodeDecodeError:
                problems.append("Unable to read python file {}".format(file_path))
        return problems

    def check(self):
        dag_folder = conf.get("core", "dags_folder")
        file_paths = list_py_file_paths(directory=dag_folder, include_examples=False)
        problems = []
        for file_path in file_paths:
            if not file_path.endswith(".py"):
                continue
            problems.extend(self._check_file(file_path))
        return problems
