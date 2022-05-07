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

import subprocess
from pathlib import Path

import pytest
from filelock import FileLock

chart_dir = (Path(__file__).parent / ".." / ".." / "chart").resolve()


@pytest.fixture(autouse=True, scope="session")
def initialize_airflow_tests(request):
    # Skip airflow tests initialization for all Helm tests
    return


@pytest.fixture(autouse=True, scope="session")
def upgrade_helm(tmp_path_factory, worker_id):
    """
    Upgrade Helm repo
    """

    def _upgrade_helm():
        subprocess.check_output(
            ["helm", "repo", "add", "stable", "https://charts.helm.sh/stable/"], cwd=chart_dir
        )
        subprocess.check_output(["helm", "dep", "update", chart_dir], cwd=chart_dir)

    if worker_id == "main":
        # not executing in with multiple workers, just update
        _upgrade_helm()
        return

    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    lock_fn = root_tmp_dir / "upgrade_helm.lock"
    flag_fn = root_tmp_dir / "upgrade_helm.done"

    with FileLock(str(lock_fn)):
        if not flag_fn.is_file():
            _upgrade_helm()
            flag_fn.touch()
