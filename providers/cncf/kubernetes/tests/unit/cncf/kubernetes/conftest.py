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

from pathlib import Path
from unittest import mock

import pytest

DATA_FILE_DIRECTORY = Path(__file__).resolve().parent / "data_files"


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "no_wait_patch_disabled: disable autouse WaitRetryAfterOrExponential patch"
    )


@pytest.fixture(autouse=True)
def no_retry_wait(request):
    # Skip patching if test has marker
    if request.node.get_closest_marker("no_wait_patch_disabled"):
        yield
        return
    patcher = mock.patch(
        "airflow.providers.cncf.kubernetes.kubernetes_helper_functions.WaitRetryAfterOrExponential.__call__",
        return_value=0,
    )
    patcher.start()
    yield
    patcher.stop()


@pytest.fixture
def data_file():
    """Helper fixture for obtain data file from data directory."""
    if not DATA_FILE_DIRECTORY.exists():
        msg = f"Data Directory {DATA_FILE_DIRECTORY.as_posix()!r} does not exist."
        raise FileNotFoundError(msg)
    if not DATA_FILE_DIRECTORY.is_dir():
        msg = f"Data Directory {DATA_FILE_DIRECTORY.as_posix()!r} expected to be a directory."
        raise NotADirectoryError(msg)

    def wrapper(filepath: str | Path) -> Path:
        return DATA_FILE_DIRECTORY.joinpath(filepath).resolve(strict=True)

    return wrapper
