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

import mock
from parameterized import parameterized

from airflow import plugins_manager
from airflow.executors.executor_loader import ExecutorLoader
from tests.test_utils.config import conf_vars

# Plugin Manager creates new modules, which is difficult to mock, so we use test isolation by a unique name.
TEST_PLUGIN_NAME = "unique_plugin_name_to_avoid_collision_i_love_kitties"


class FakeExecutor:
    pass


class FakePlugin(plugins_manager.AirflowPlugin):
    name = TEST_PLUGIN_NAME
    executors = [FakeExecutor]


class TestExecutorLoader(unittest.TestCase):

    def setUp(self) -> None:
        ExecutorLoader._default_executor = None

    def tearDown(self) -> None:
        ExecutorLoader._default_executor = None

    @parameterized.expand([
        ("CeleryExecutor", ),
        ("CeleryKubernetesExecutor", ),
        ("DebugExecutor", ),
        ("KubernetesExecutor", ),
        ("LocalExecutor", ),
    ])
    def test_should_support_executor_from_core(self, executor_name):
        with conf_vars({
            ("core", "executor"): executor_name
        }):
            executor = ExecutorLoader.get_default_executor()
            self.assertIsNotNone(executor)
            self.assertEqual(executor_name, executor.__class__.__name__)

    @mock.patch("airflow.plugins_manager.plugins", [
        FakePlugin()
    ])
    @mock.patch("airflow.plugins_manager.executors_modules", None)
    def test_should_support_plugins(self):
        with conf_vars({
            ("core", "executor"): f"{TEST_PLUGIN_NAME}.FakeExecutor"
        }):
            executor = ExecutorLoader.get_default_executor()
            self.assertIsNotNone(executor)
            self.assertEqual("FakeExecutor", executor.__class__.__name__)

    def test_should_support_custom_path(self):
        with conf_vars({
            ("core", "executor"): "tests.executors.test_executor_loader.FakeExecutor"
        }):
            executor = ExecutorLoader.get_default_executor()
            self.assertIsNotNone(executor)
            self.assertEqual("FakeExecutor", executor.__class__.__name__)
