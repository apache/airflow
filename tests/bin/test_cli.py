# -*- coding: utf-8 -*-
#
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
import io
import unittest
from contextlib import redirect_stdout

from airflow import AirflowException
from airflow.bin.cli import exec_airflow_command


class ExecAirflowCommandTestCase(unittest.TestCase):
    def test_should_execute_cli(self):
        with redirect_stdout(io.StringIO()) as stdout:
            exec_airflow_command(["airflow", "config"])
        stdout = stdout.getvalue()
        self.assertIn("[core]", stdout)

    def test_should_accept_only_airflow(self):
        with self.assertRaisesRegex(AirflowException, 'The first element must be equal to "airflow".'):
            exec_airflow_command(["cat", "config"])

    def test_should_raise_exception_on_invalid_command(self):
        with self.assertRaises(SystemExit):
            exec_airflow_command(["airflow", "invalid-command"])
