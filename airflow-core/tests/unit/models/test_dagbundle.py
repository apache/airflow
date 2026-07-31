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

from typing import TYPE_CHECKING

import pytest

from airflow.models.dagbundle import DagBundleModel

from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_teams

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.team import Team

pytestmark = pytest.mark.db_test


class TestDagBundleModel:
    def teardown_method(self):
        clear_db_dag_bundles()
        clear_db_teams()

    def test_get_team_name(self, testing_team: Team, session: Session):
        bundle = DagBundleModel(name="test_bundle")
        bundle.teams.append(testing_team)
        session.add(bundle)
        session.flush()

        assert DagBundleModel.get_team_name("test_bundle", session=session) == "testing"

    def test_get_team_name_no_team(self, session: Session):
        bundle = DagBundleModel(name="test_bundle")
        session.add(bundle)
        session.flush()

        assert DagBundleModel.get_team_name("test_bundle", session=session) is None

    def test_get_team_name_unknown_bundle(self, session: Session):
        assert DagBundleModel.get_team_name("does_not_exist", session=session) is None

    def test_get_team_names(self, testing_team: Team, session: Session):
        mapped = DagBundleModel(name="mapped_bundle")
        mapped.teams.append(testing_team)
        unmapped = DagBundleModel(name="unmapped_bundle")
        session.add_all([mapped, unmapped])
        session.flush()

        result = DagBundleModel.get_team_names(
            ["mapped_bundle", "unmapped_bundle", "does_not_exist"], session=session
        )

        # Only bundles actually mapped to a team are returned; callers treat absent keys as None.
        assert result == {"mapped_bundle": "testing"}

    def test_get_team_names_empty(self, session: Session):
        assert DagBundleModel.get_team_names([], session=session) == {}
