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

from airflow.models.team import Team

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_teams,
)

pytestmark = pytest.mark.db_test


def _clean_db():
    clear_db_teams()


@pytest.fixture(autouse=True)
def clean_db(session):
    _clean_db()
    yield
    _clean_db()


class TestListTeams:
    @conf_vars({("core", "multi_team"): "true"})
    def test_should_response_200(self, test_client, session):
        session.add(Team(name="team1"))
        session.add(Team(name="team2"))
        session.add(Team(name="team3"))
        session.commit()
        with assert_queries_count(3):
            response = test_client.get("/teams", params={})
        assert response.status_code == 200
        assert response.json() == {
            "teams": [
                {
                    "name": "team1",
                },
                {
                    "name": "team2",
                },
                {
                    "name": "team3",
                },
            ],
            "total_entries": 3,
        }

    @conf_vars({("core", "multi_team"): "true"})
    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/teams", params={})
        assert response.status_code == 401

    def test_should_response_403_flag_off(self, test_client):
        response = test_client.get("/teams", params={})
        assert response.status_code == 403
