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

from unittest import TestCase

from airflow.upgrade.rules.custom_executors_require_full_path_rule import CustomExecutorsRequireFullPathRule
from tests.compat import patch


class TestCustomExecutorsRequireFullPath(TestCase):
    @patch('airflow.plugins_manager.executors_modules',
           ["my_plugin.MyCustomExecutor", "my_acme.executors.MyCustomExecutor"])
    def test_invalid_check(self):
        rule = CustomExecutorsRequireFullPathRule()

        assert isinstance(rule.title, str)
        assert isinstance(rule.description, str)

        msg = (
            "Deprecation Warning: you do not need to load your custom executor via a plugin."
            "In Airflow 2.0, you only need to provide a full path to the the custom executor module."
            "see 'Custom executors is loaded using full import path' section at the link below:"
            "https://github.com/apache/airflow/blob/master/UPDATING.md"
        )

        assert msg == rule.check()

    @patch('airflow.plugins_manager.executors_modules', [])
    def test_check(self):
        rule = CustomExecutorsRequireFullPathRule()

        assert isinstance(rule.title, str)
        assert isinstance(rule.description, str)

        assert rule.check() is None
