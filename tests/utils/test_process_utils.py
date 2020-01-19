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
#
import unittest
from subprocess import CalledProcessError

from airflow.logging_config import configure_logging
from airflow.utils.process_utils import execute_in_subprocess


class TestExecuteInSubProcess(unittest.TestCase):

    def test_should_print_all_messages(self):
        with self.assertLogs() as logs:
            execute_in_subprocess(["bash", "-c", "echo CAT; echo KITTY;"])

        msgs = [record.getMessage() for record in logs.records]
        self.assertEqual([
            "Executing cmd: ['bash', '-c', 'echo CAT; echo KITTY;']",
            'Output:',
            'CAT',
            'KITTY'
        ], msgs)

    def test_should_raise_exception(self):
        with self.assertRaises(CalledProcessError):
            execute_in_subprocess(["bash", "-c", "exit 1"])
