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

pytest_plugins = "tests_common.pytest_plugin"


@pytest.fixture(autouse=True, scope="session")
def _create_edge_tables():
    """Create edge3 tables for tests since they are managed separately from Base.metadata."""
    from airflow import settings

    if not settings.engine:
        yield
        return

    from airflow.providers.edge3.models.db import _edge_metadata

    _edge_metadata.create_all(settings.engine)
    yield
    _edge_metadata.drop_all(settings.engine)
