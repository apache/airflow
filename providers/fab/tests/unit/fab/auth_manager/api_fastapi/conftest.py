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

import types
from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

from airflow.api_fastapi.core_api.security import get_user as get_user_dep
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager


@pytest.fixture(scope="module")
def fab_auth_manager():
    return FabAuthManager()


@pytest.fixture(scope="module")
def test_client(fab_auth_manager):
    return TestClient(fab_auth_manager.get_fastapi_app())


@pytest.fixture
def override_deps(test_client):
    """
    Context-managed helper for app.dependency_overrides.

    Usage:
        with override_deps({dep_func: override_func, ...}):
            # do requests
    """
    app = test_client.app

    @contextmanager
    def _use(mapping: dict):
        for dep, override in mapping.items():
            app.dependency_overrides[dep] = override
        try:
            yield
        finally:
            for dep in mapping.keys():
                app.dependency_overrides.pop(dep, None)

    return _use


@pytest.fixture
def as_user(override_deps):
    @contextmanager
    def _as(u=types.SimpleNamespace(id=1, username="tester")):
        with override_deps({get_user_dep: lambda: u}):
            yield u

    return _as
