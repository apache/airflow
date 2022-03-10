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
import re
from pathlib import Path

import pytest

OLD_EXECUTOR = os.environ.get("AIRFLOW__CORE__EXECUTOR", default=None)


def pytest_configure(config):
    os.environ["AIRFLOW__CORE__EXECUTOR"] = "DebugExecutor"


def pytest_unconfigure(config):
    if OLD_EXECUTOR is not None:
        os.environ["AIRFLOW__CORE__EXECUTOR"] = OLD_EXECUTOR


def pytest_collection_modifyitems(config, items):
    """Add @pytest.mark.system(provider_name) for every system test"""
    rootdir = Path(config.rootdir)
    for item in items:
        if item.get_closest_marker("system"):  # already marked
            continue
        rel_path = Path(item.fspath).relative_to(rootdir)
        match = re.match(".+/providers/([^/]+)", str(rel_path))
        if not match:
            continue
        provider = match.group(1)
        item.add_marker(pytest.mark.system(provider))
