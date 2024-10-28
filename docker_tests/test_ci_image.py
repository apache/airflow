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

from python_on_whales import DockerException

from docker_tests.docker_utils import (
    display_dependency_conflict_message,
    run_bash_in_docker,
)


def test_pip_dependencies_conflict(default_docker_image):
    try:
        run_bash_in_docker("pip check", image=default_docker_image)
    except DockerException:
        display_dependency_conflict_message()
        raise


def test_providers_present():
    try:
        run_bash_in_docker("airflow providers list")
    except DockerException:
        display_dependency_conflict_message()
        raise
