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
from __future__ import annotations

from unittest import mock

from airflow.providers.google.go_module_utils import init_module, install_dependencies


class TestGoModule:
    @mock.patch("airflow.providers.google.go_module_utils._execute_in_subprocess")
    def test_should_init_go_module(self, mock_execute_in_subprocess):
        init_module(go_module_name="example.com/main", go_module_path="/home/example/go")
        mock_execute_in_subprocess.assert_called_once_with(
            ["go", "mod", "init", "example.com/main"], cwd="/home/example/go"
        )

    @mock.patch("airflow.providers.google.go_module_utils._execute_in_subprocess")
    def test_should_install_module_dependencies(self, mock_execute_in_subprocess):
        install_dependencies(go_module_path="/home/example/go")
        mock_execute_in_subprocess.assert_called_once_with(["go", "mod", "tidy"], cwd="/home/example/go")
