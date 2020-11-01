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
import subprocess
import sys
import pytest

# We should set these before loading _any_ of the rest of airflow so that the
# unit test mode config is set as early as possible.
tests_directory = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(autouse=True, scope="session")
def upgrade_helm():
    """
    Upgrade Helm repo
    """
    subprocess.check_output(
        ["helm", "repo", "add", "stable", "https://kubernetes-charts.storage.googleapis.com/"]
    )
    subprocess.check_output(["helm", "dep", "update", sys.path[0]])
