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

import uuid
from unittest.mock import MagicMock, patch

from airflow.models.team import Team


class TestTeam:
    """Unit tests for Team model class methods."""

    def test_get_all_teams_id_to_name_mapping_with_teams(self):
        """Test get_all_teams_id_to_name_mapping returns correct mapping when teams exist."""
        team1_id = str(uuid.uuid4())
        team2_id = str(uuid.uuid4())
        team3_id = str(uuid.uuid4())

        mock_query_result = [
            (team1_id, "team_alpha"),
            (team2_id, "team_beta"),
            (team3_id, "team_gamma"),
        ]

        # Create a mock session
        mock_session = MagicMock()
        mock_session.execute.return_value.all.return_value = mock_query_result

        # Call the method directly with our mock session
        result = Team.get_all_teams_id_to_name_mapping(session=mock_session)

        expected = {
            team1_id: "team_alpha",
            team2_id: "team_beta",
            team3_id: "team_gamma",
        }
        assert result == expected

        # Verify the execute method was called correctly
        mock_session.execute.assert_called_once()
        mock_session.execute.return_value.all.assert_called_once()

    def test_get_all_team_names_with_teams(self):
        """Test get_all_team_names returns correct set of names when teams exist."""
        mock_mapping = {
            "uuid-1": "team_alpha",
            "uuid-2": "team_beta",
            "uuid-3": "team_gamma",
        }

        with patch.object(Team, "get_all_teams_id_to_name_mapping", return_value=mock_mapping):
            result = Team.get_all_team_names()

            assert result == {"team_alpha", "team_beta", "team_gamma"}
            assert isinstance(result, set)
