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
#

import os

import pytest

from tests.test_utils import AIRFLOW_MAIN_FOLDER
from tests.test_utils.system_tests_class import SystemTest

BEAM_DAG_FOLDER = os.path.join(AIRFLOW_MAIN_FOLDER, "airflow", "providers", "apache", "beam", "example_dags")


@pytest.mark.system("apache.beam")
class BeamExampleDagsSystemTest(SystemTest):
    def test_run_example_dag_beam_python(self):
        self.run_dag('example_beam_native_python', BEAM_DAG_FOLDER)

    def test_run_example_dag_beam_python_dataflow_async(self):
        self.run_dag('example_beam_native_python_dataflow_async', BEAM_DAG_FOLDER)

    def test_run_example_dag_beam_java_direct_runner(self):
        self.run_dag('example_beam_native_java_direct_runner', BEAM_DAG_FOLDER)

    def test_run_example_dag_beam_java_dataflow_runner(self):
        self.run_dag('example_beam_native_java_dataflow_runner', BEAM_DAG_FOLDER)

    def test_run_example_dag_beam_java_spark_runner(self):
        self.run_dag('example_beam_native_java_spark_runner', BEAM_DAG_FOLDER)

    def test_run_example_dag_beam_java_flink_runner(self):
        self.run_dag('example_beam_native_java_flink_runner', BEAM_DAG_FOLDER)
