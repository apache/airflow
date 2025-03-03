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

import pytest
from fastapi.testclient import TestClient

from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager


@pytest.fixture(scope="module")
def fab_auth_manager():
    return FabAuthManager(None)


@pytest.fixture(scope="module")
def test_client(fab_auth_manager):
    return TestClient(fab_auth_manager.get_fastapi_app())
