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

from airflow.upgrade.rules.base_rule import BaseRule
from airflow import conf
from airflow.utils.dag_processing import list_py_file_paths


class ProperlyImportConfFromAirflow(BaseRule):
    """
      ProperlyImportConfFromAirflow class to ensure proper import of conf to work in Airflow 2.0
      """
    title = "Ensure Users Properly Import conf from Airflow"
    description = """\
    In Airflow-2.0, it's not possible to import `conf` from airflow by using `import conf from airflow`
    To ensure your code works in Airflow 2.0, you should use `from airflow.configuration import conf`.
                      """

    wrong_conf_import = "from airflow import conf"
    proper_conf_import = "from airflow.configuration import conf"

    @staticmethod
    def _conf_import_info(file_path, line_number):
        return "Affected file: {} (line {})".format(file_path, line_number)

    def _check_file(self, file_path):
        problems = []
        conf_import_check = self.wrong_conf_import
        with open(file_path, "r") as file_pointer:
            try:
                for line_number, line in enumerate(file_pointer, 1):
                    if conf_import_check in line:
                        problems.append(self._conf_import_info(file_path, line_number))
            except UnicodeDecodeError:
                problems.append("Unable to read python file {}".format(file_path))
        return problems

    def check(self):
        dag_folder = conf.get("core", "dags_folder")
        files = list_py_file_paths(directory=dag_folder, include_examples=False)
        problems = []
        for file in files:
            if not file.endswith(".py"):
                continue
            problems.extend(self._check_file(file))
        return problems
