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

from airflow.api_fastapi.app import create_app


@pytest.fixture
def test_client():
    return TestClient(create_app())


@pytest.fixture
def client():
    """This fixture is more flexible than test_client, as it allows to specify which apps to include."""

    def create_test_client(apps="all"):
        app = create_app(apps=apps)
        return TestClient(app)

    return create_test_client


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    dagbag_instance = DagBag(include_examples=True, read_dags_from_db=False)
    dagbag_instance.sync_to_db()
    return dagbag_instance
