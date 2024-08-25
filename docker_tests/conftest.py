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

import os
import shutil
import tempfile
from pathlib import Path

import pytest

from docker_tests.constants import DEFAULT_DOCKER_IMAGE


@pytest.fixture
def default_docker_image() -> str:
    return os.environ.get("DOCKER_IMAGE") or DEFAULT_DOCKER_IMAGE


@pytest.fixture
def shared_tmp_path():
    """
    Create a shared temporary directory for CI runners with DinD enabled.
    """
    try:
        path = Path("/home/runner/_work/")
        path.mkdir(parents=True, exist_ok=True)
        tmp_path = Path(tempfile.mkdtemp(dir=path))
    except Exception:
        tmp_path = Path(tempfile.mkdtemp())
    yield tmp_path
    shutil.rmtree(tmp_path)
