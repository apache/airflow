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

import subprocess

from docker_tests.command_utils import run_command
from docker_tests.docker_tests_utils import display_dependency_conflict_message, docker_image


class TestPythonPackages:
    def test_pip_dependencies_conflict(self):
        try:
            run_command(
                ["docker", "run", "--rm", "--entrypoint", "/bin/bash", docker_image, "-c", "pip check"]
            )
        except subprocess.CalledProcessError as ex:
            display_dependency_conflict_message()
            raise ex
