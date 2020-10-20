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

import os
import tempfile
import unittest

from airflow.cli import cli_parser
from airflow.cli.commands import kubernetes_command


class TestGeneratteDagYamlCommand(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    def test_generate_dag_yaml(self):
        with tempfile.TemporaryDirectory("airflow_dry_run_test/") as directory:
            file_name = "example_bash_operator_run_after_loop_2020-11-03T00_00_00_plus_00_00.yml"
            kubernetes_command.generate_pod_yaml(self.parser.parse_args([
                'kubernetes', 'generate-dag-yaml',
                'example_bash_operator', "2020-11-03", "--output-path", directory]))
            self.assertEqual(len(os.listdir(directory)), 1)
            out_dir = directory + "/airflow_yaml_output/"
            self.assertEqual(len(os.listdir(out_dir)), 6)
            self.assertTrue(os.path.isfile(out_dir + file_name))
            self.assertGreater(os.stat(out_dir + file_name).st_size, 0)
