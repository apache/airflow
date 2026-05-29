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

from airflow.sdk.definitions.asset.access_control import AssetAccessControl


class TestAssetAccessControl:
    def test_defaults(self):
        ac = AssetAccessControl()
        assert ac.producer_teams == []
        assert ac.consumer_teams == []
        assert ac.allow_global is True

    def test_explicit_values(self):
        ac = AssetAccessControl(
            producer_teams=["team_a", "team_b"],
            consumer_teams=["team_c"],
            allow_global=False,
        )
        assert ac.producer_teams == ["team_a", "team_b"]
        assert ac.consumer_teams == ["team_c"]
        assert ac.allow_global is False

    @pytest.mark.parametrize(
        "teams",
        [
            [""],
            [123],
            [None],
            [True],
            [{}],
            ["team_a", "  ", "team_b"],
        ],
    )
    def test_rejects_invalid_producer_teams(self, teams):
        with pytest.raises(ValueError, match="producer_teams"):
            AssetAccessControl(producer_teams=teams)

    @pytest.mark.parametrize(
        "teams",
        [
            [""],
            [123],
            [None],
            [True],
            [{}],
            ["team_a", "  ", "team_b"],
        ],
    )
    def test_rejects_invalid_consumer_teams(self, teams):
        with pytest.raises(ValueError, match="consumer_teams"):
            AssetAccessControl(consumer_teams=teams)

    @pytest.mark.parametrize(
        "teams",
        [
            [],
            ["team_a"],
            ["team_a", "team_b"],
            ["team-with-dashes"],
            ["team_with_underscores"],
        ],
    )
    def test_accepts_valid_producer_teams(self, teams):
        ac = AssetAccessControl(producer_teams=teams)
        assert ac.producer_teams == teams

    @pytest.mark.parametrize(
        "teams",
        [
            [],
            ["team_a"],
            ["team_a", "team_b"],
            ["team-with-dashes"],
            ["team_with_underscores"],
        ],
    )
    def test_accepts_valid_consumer_teams(self, teams):
        ac = AssetAccessControl(consumer_teams=teams)
        assert ac.consumer_teams == teams

    def test_allow_global_must_be_bool(self):
        with pytest.raises(TypeError):
            AssetAccessControl(allow_global="yes")
