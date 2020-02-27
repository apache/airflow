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

from parameterized import parameterized

from airflow.executors.executor_loader import ExecutorLoader
from airflow.plugins_manager import executors_modules, make_module
from tests.test_utils.config import conf_vars

# Plugin Manager creates new modules, which is difficult to mock, so we use test isolation by a unique name.
TEST_PLUGIN_NAME = "unique_plugin_name_to_avoid_collision_i_love_kitties"


class FakeExecutor:
    pass


class TestExecutorLoader(unittest.TestCase):

    def setUp(self) -> None:
        ExecutorLoader._default_executor = None

    def tearDown(self) -> None:
        ExecutorLoader._default_executor = None

    @parameterized.expand([
        ("LocalExecutor", ),
        ("DebugExecutor", ),
    ])
    def test_should_support_executor_from_core(self, executor_name):
        with conf_vars({
            ("core", "executor"): executor_name
        }):
            executor = ExecutorLoader.get_default_executor()
            self.assertIsNotNone(executor)
            self.assertIn(executor_name, executor.__class__.__name__)

    def test_should_support_plugin(self):
        executors_modules.append(make_module('airflow.executors.' + TEST_PLUGIN_NAME, [FakeExecutor]))
        self.addCleanup(self.remove_executor_module)
        with conf_vars({
            ("core", "executor"): f"{TEST_PLUGIN_NAME}.FakeExecutor"
        }):
            executor = ExecutorLoader.get_default_executor()
            self.assertIsNotNone(executor)
            self.assertIn("FakeExecutor", executor.__class__.__name__)

    def remove_executor_module(self):
        executors_modules.pop()

    def test_should_support_custom_path(self):
        with conf_vars({
            ("core", "executor"): f"tests.executors.test_executor_loader.FakeExecutor"
        }):
            executor = ExecutorLoader.get_default_executor()
            self.assertIsNotNone(executor)
            self.assertIn("FakeExecutor", executor.__class__.__name__)
