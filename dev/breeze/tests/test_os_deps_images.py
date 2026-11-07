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

import shutil
import subprocess

from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH


def test_hardened_flavor_explains_a_base_image_without_python(tmp_path):
    result = subprocess.run(
        [
            shutil.which("bash"),
            AIRFLOW_ROOT_PATH / "scripts" / "docker" / "install_os_dependencies.sh",
            "dev",
        ],
        env={"PATH": str(tmp_path)},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "AIRFLOW_IMAGE_FLAVOR to 'legacy'" in result.stdout
