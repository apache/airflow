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

import unittest
from unittest import mock

from airflow.providers.apache.pig.hooks.pig import PigCliHook
from airflow.providers.apache.pig.operators.pig import PigOperator

TEST_TASK_ID = "test_task_id"
TEST_CONTEXT_ID = "test_context_id"
PIG = "ls /;"


class TestPigOperator(unittest.TestCase):
    def test_prepare_template(self):
        pig = "sh echo $DATE;"
        task_id = TEST_TASK_ID

        operator = PigOperator(pig=pig, task_id=task_id)
        operator.prepare_template()
        self.assertEqual(pig, operator.pig)

        # converts when pigparams_jinja_translate = true
        operator = PigOperator(pig=pig, task_id=task_id, pigparams_jinja_translate=True)
        operator.prepare_template()
        self.assertEqual("sh echo {{ DATE }};", operator.pig)

    @mock.patch.object(PigCliHook, 'run_cli')
    def test_execute(self, mock_run_cli):
        pig_opts = "-x mapreduce"
        operator = PigOperator(pig=PIG, pig_opts=pig_opts, task_id=TEST_TASK_ID)
        operator.execute(context=TEST_CONTEXT_ID)

        mock_run_cli.assert_called_once_with(pig=PIG, pig_opts=pig_opts)

    @mock.patch.object(PigCliHook, 'run_cli')
    def test_execute_default_pig_opts_to_none(self, mock_run_cli):
        operator = PigOperator(pig=PIG, task_id=TEST_TASK_ID)
        operator.execute(context=TEST_CONTEXT_ID)

        mock_run_cli.assert_called_once_with(pig=PIG, pig_opts=None)

    @mock.patch.object(PigCliHook, 'run_cli')
    @mock.patch.object(PigCliHook, 'kill')
    def test_on_kill(self, mock_kill, mock_rul_cli):
        operator = PigOperator(pig=PIG, task_id=TEST_TASK_ID)
        operator.execute(context=TEST_CONTEXT_ID)
        operator.on_kill()

        mock_rul_cli.assert_called()
        mock_kill.assert_called()
