#
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

import re

import pytest

from airflow.cli.commands.team_command import TEAM_NAME_PATTERN
from airflow.secrets.environment_variables import (
    CONN_ENV_PREFIX,
    VAR_ENV_PREFIX,
    EnvironmentVariablesBackend,
)

# A team specific secret is stored as ``<PREFIX>_<TEAM_NAME>___<SECRET_ID>``. Team names cannot
# contain an underscore (``^[a-zA-Z0-9-]{3,50}$``), which is what keeps that name unambiguous to
# read back; both a hyphenated and a plain name are exercised.
TEAM_NAMES = ["team-a", "teama"]
OTHER_TEAM_NAME = "team-b"
SECRET_ID = "dbconn"
TEAM_VALUE = "team-scoped-value"
GLOBAL_VALUE = "team-agnostic-value"

# ``(env prefix, backend method)`` of the two lookups sharing the team scoping rules.
LOOKUPS = [
    pytest.param(CONN_ENV_PREFIX, "get_conn_value", id="connection"),
    pytest.param(VAR_ENV_PREFIX, "get_variable", id="variable"),
]


def team_env_var(env_prefix: str, team_name: str, secret_id: str = SECRET_ID) -> str:
    return f"{env_prefix}_{team_name.upper()}___{secret_id.upper()}"


def team_scoped_id(team_name: str, secret_id: str = SECRET_ID) -> str:
    """Spell the ``_<TEAM_NAME>___<SECRET_ID>`` namespace out as a secret id."""
    return f"_{team_name}___{secret_id}"


def lookup(env_prefix: str, method: str, secret_id: str, team_name: str | None) -> str | None:
    return getattr(EnvironmentVariablesBackend(), method)(secret_id, team_name)


class TestEnvironmentVariablesBackendTeamScope:
    """A team specific secret must only be resolvable for the team it is stored for."""

    @pytest.mark.parametrize(("env_prefix", "method"), LOOKUPS)
    @pytest.mark.parametrize("team_name", TEAM_NAMES)
    def test_team_scoped_secret_is_not_resolved_without_a_team_scope(
        self, monkeypatch, env_prefix, method, team_name
    ):
        monkeypatch.setenv(team_env_var(env_prefix, team_name), TEAM_VALUE)

        assert lookup(env_prefix, method, team_scoped_id(team_name), None) is None

    @pytest.mark.parametrize(("env_prefix", "method"), LOOKUPS)
    @pytest.mark.parametrize("team_name", TEAM_NAMES)
    def test_team_scoped_secret_is_not_resolved_for_another_team(
        self, monkeypatch, env_prefix, method, team_name
    ):
        monkeypatch.setenv(team_env_var(env_prefix, team_name), TEAM_VALUE)

        assert lookup(env_prefix, method, team_scoped_id(team_name), OTHER_TEAM_NAME) is None

    @pytest.mark.parametrize(("env_prefix", "method"), LOOKUPS)
    @pytest.mark.parametrize("team_name", TEAM_NAMES)
    def test_team_scoped_secret_is_resolved_for_its_own_team(
        self, monkeypatch, env_prefix, method, team_name
    ):
        monkeypatch.setenv(team_env_var(env_prefix, team_name), TEAM_VALUE)

        assert lookup(env_prefix, method, SECRET_ID, team_name) == TEAM_VALUE

    @pytest.mark.parametrize(("env_prefix", "method"), LOOKUPS)
    @pytest.mark.parametrize("team_name", TEAM_NAMES)
    def test_id_spelling_out_a_team_namespace_is_never_resolved(
        self, monkeypatch, env_prefix, method, team_name
    ):
        """Even for the team that owns it: an id of that shape is not attributable to a team.

        A caller reaches its own team's secret with the bare id plus its team scope. Allowing
        the namespaced spelling as well is what made the caller's own prefix look like proof of
        ownership, which it is not -- see the collision test below.
        """
        monkeypatch.setenv(team_env_var(env_prefix, team_name), TEAM_VALUE)

        assert lookup(env_prefix, method, team_scoped_id(team_name), team_name) is None

    @pytest.mark.parametrize(("env_prefix", "method"), LOOKUPS)
    @pytest.mark.parametrize("secret_id", ["_a___b", "_ab___x"])
    @pytest.mark.parametrize("team_name", [None, *TEAM_NAMES])
    def test_id_that_cannot_name_a_team_is_still_resolved(
        self, monkeypatch, env_prefix, method, secret_id, team_name
    ):
        """Refusing every ``_x___y`` id would block legitimate team agnostic secrets.

        Team names are validated as ``^[a-zA-Z0-9_-]{3,50}$``, so a one- or two-character
        leading segment cannot be a team. Such an id names no team's namespace and must stay
        resolvable through the team agnostic lookup, for any caller.
        """
        monkeypatch.setenv(env_prefix + secret_id.upper(), GLOBAL_VALUE)

        assert lookup(env_prefix, method, secret_id, team_name) == GLOBAL_VALUE

    @pytest.mark.parametrize(("env_prefix", "method"), LOOKUPS)
    def test_a_team_name_cannot_span_the_separator(self, monkeypatch, env_prefix, method):
        """The namespace is unambiguous only because a team name cannot contain an underscore.

        ``_team-a___prod___dbconn`` has exactly one reading -- team ``team-a`` with the id
        ``prod___dbconn`` -- and is refused as that team's namespace. Were ``team-a___prod``
        also a legal team name, the same string would equally read as *that* team with the id
        ``dbconn``, and nothing in it would choose between them. That second reading is what
        the name rule removes, and what previously forced this guard to try every split.
        """
        assert not re.match(TEAM_NAME_PATTERN, "team-a___prod")

        monkeypatch.setenv(f"{env_prefix}_TEAM-A___PROD___DBCONN", TEAM_VALUE)

        assert lookup(env_prefix, method, "_team-a___prod___dbconn", "team-b") is None
        assert lookup(env_prefix, method, "_team-a___prod___dbconn", None) is None
        # and its owner reaches it the normal way, with the bare id and its team scope
        assert lookup(env_prefix, method, "prod___dbconn", "team-a") == TEAM_VALUE

    @pytest.mark.parametrize(("env_prefix", "method"), LOOKUPS)
    @pytest.mark.parametrize("team_name", TEAM_NAMES)
    def test_team_scoped_secret_wins_over_the_team_agnostic_one(
        self, monkeypatch, env_prefix, method, team_name
    ):
        monkeypatch.setenv(team_env_var(env_prefix, team_name), TEAM_VALUE)
        monkeypatch.setenv(env_prefix + SECRET_ID.upper(), GLOBAL_VALUE)

        assert lookup(env_prefix, method, SECRET_ID, team_name) == TEAM_VALUE
        assert lookup(env_prefix, method, SECRET_ID, None) == GLOBAL_VALUE

    @pytest.mark.parametrize(("env_prefix", "method"), LOOKUPS)
    @pytest.mark.parametrize("team_name", [None, *TEAM_NAMES])
    def test_team_agnostic_secret_is_resolved_for_any_team_scope(
        self, monkeypatch, env_prefix, method, team_name
    ):
        monkeypatch.setenv(env_prefix + SECRET_ID.upper(), GLOBAL_VALUE)

        assert lookup(env_prefix, method, SECRET_ID, team_name) == GLOBAL_VALUE

    @pytest.mark.parametrize(("env_prefix", "method"), LOOKUPS)
    @pytest.mark.parametrize("team_name", [None, *TEAM_NAMES])
    def test_unset_secret_is_not_resolved(self, monkeypatch, env_prefix, method, team_name):
        monkeypatch.delenv(env_prefix + SECRET_ID.upper(), raising=False)

        assert lookup(env_prefix, method, SECRET_ID, team_name) is None
