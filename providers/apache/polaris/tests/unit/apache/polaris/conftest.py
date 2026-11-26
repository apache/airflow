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

import json

import pytest

from airflow.models import Connection

from unit.apache.polaris.fake_polaris import serve


@pytest.fixture
def fake_polaris():
    """A fake Polaris server; the real SDK and ``requests`` talk to it over HTTP."""
    with serve() as fake:
        yield fake


@pytest.fixture
def polaris_conn(create_connection_without_db, fake_polaris):
    """Create a ``polaris`` connection pointing at the fake server; returns its conn_id."""

    def _create(
        conn_id: str = "polaris_test",
        login: str | None = "client-id",
        password: str | None = "s3cr3t",
        **extra,
    ):
        create_connection_without_db(
            Connection(
                conn_id=conn_id,
                conn_type="polaris",
                host=fake_polaris.url,
                login=login,
                password=password,
                extra=json.dumps(extra),
            )
        )
        return conn_id

    return _create
