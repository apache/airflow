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

import asyncio
import json
from unittest.mock import MagicMock

import pytest
from fastapi import Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.logging.decorators import (
    _mask_connection_fields,
    _mask_variable_fields,
    _sanitize_for_stdlib_log,
    action_logging,
)
from airflow.models import Connection, Log, Pool, Variable
from airflow.models.dag import DagModel, clear_team_name_cache
from airflow.models.dagbundle import DagBundleModel
from airflow.models.team import Team

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_connections,
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_logs,
    clear_db_pools,
    clear_db_teams,
    clear_db_variables,
)


class TestSanitizeForStdlibLog:
    """User input passed to stdlib ``%s``-style logging must have CR/LF stripped.

    On a deployment configured with a non-JSON (plain-text) log formatter, a newline in the
    value would otherwise let an attacker forge log lines (CWE-117). The audit logging path
    in ``action_logging`` passes ``logical_date`` from the request through stdlib's ``logger``,
    so this sanitisation is unconditional regardless of the formatter actually in use.
    """

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
            ("bad\ndate", "bad date"),
            ("bad\r\ndate", "bad  date"),
            ("bad\rdate", "bad date"),
            ("a\nb\nc", "a b c"),
            ("", ""),
        ],
    )
    def test_strips_cr_and_lf(self, raw, expected):
        assert _sanitize_for_stdlib_log(raw) == expected


class TestMaskConnectionFields:
    """Connection ``extra`` values must never be recorded verbatim in the audit log.

    Secrets routinely live in connection ``extra`` under key names that are not in the
    masker's sensitive-key list (``sas_url``, ``endpoint``, ``jdbc_url``, …). Masking by
    key name alone therefore lets those values through. ``_mask_connection_fields`` fails
    closed instead: every ``extra`` value is masked and only the key names are kept.
    """

    def test_masks_extra_value_under_non_sensitive_key(self):
        result = _mask_connection_fields(
            {"extra": json.dumps({"sas_url": "SAS_LEAK_value", "account_name": "prodacct"})}
        )
        assert result["extra"] == {"sas_url": "***", "account_name": "***"}

    def test_masks_extra_value_under_sensitive_key(self):
        result = _mask_connection_fields({"extra": json.dumps({"password": "hunter2"})})
        assert result["extra"] == {"password": "***"}

    def test_preserves_extra_key_names(self):
        result = _mask_connection_fields({"extra": json.dumps({"host": "h", "login": "l", "endpoint": "e"})})
        assert set(result["extra"]) == {"host", "login", "endpoint"}
        assert all(v == "***" for v in result["extra"].values())

    @pytest.mark.enable_redact
    def test_top_level_password_is_masked_by_key_name(self):
        # Top-level connection fields keep the existing key-name redaction path
        # (unchanged by this fix); it only masks when the secrets masker is active.
        result = _mask_connection_fields({"connection_id": "c1", "password": "hunter2"})
        assert result["connection_id"] == "c1"
        assert result["password"] == "***"

    def test_non_dict_json_extra_returns_informational_string(self):
        result = _mask_connection_fields({"extra": json.dumps(["not", "a", "dict"])})
        assert result["extra"] == "Expected JSON object in `extra` field, got non-dict JSON"

    def test_non_json_extra_returns_informational_string(self):
        result = _mask_connection_fields({"extra": "this is not json"})
        assert result["extra"] == "Encountered non-JSON in `extra` field"

    def test_empty_extra_passes_through_key_name_redaction(self):
        # falsy ``extra`` skips the JSON branch and goes through key-name redaction
        result = _mask_connection_fields({"extra": ""})
        assert result["extra"] == ""


class TestMaskVariableFields:
    """The variable value is masked unconditionally in the audit log.

    A secret stored as a Variable under an innocuous key name (e.g.
    ``campaign_signing_material``) must not be persisted to the audit log, so the value is
    masked regardless of the key name; the key and description are kept.
    """

    def test_masks_value_under_non_sensitive_key(self):
        result = _mask_variable_fields(
            {"key": "campaign_signing_material", "value": "VARVAL_LEAK_token", "description": "d"}
        )
        assert result == {"key": "campaign_signing_material", "value": "***", "description": "d"}

    def test_masks_value_under_sensitive_key(self):
        result = _mask_variable_fields({"key": "api_secret", "value": "topsecret"})
        assert result == {"key": "api_secret", "value": "***"}

    def test_masks_val_alias(self):
        result = _mask_variable_fields({"key": "k", "val": "secretval"})
        assert result == {"key": "k", "val": "***"}

    def test_value_without_key_is_still_masked(self):
        result = _mask_variable_fields({"value": "secretval"})
        assert result == {"value": "***"}


class TestMaskBulkFields:
    """The bulk endpoints nest their entities, and the masking has to reach them."""

    def test_bulk_variable_values_are_masked(self):
        result = _mask_variable_fields(
            {
                "actions": [
                    {
                        "action": "create",
                        "entities": [
                            {"key": "campaign_signing_material", "value": "VARVAL_LEAK_token"},
                            {"key": "other", "val": "VARVAL_LEAK_alias"},
                        ],
                        "action_on_existence": "overwrite",
                    }
                ]
            }
        )
        assert result == {
            "actions": [
                {
                    "action": "create",
                    "entities": [
                        {"key": "campaign_signing_material", "value": "***"},
                        {"key": "other", "val": "***"},
                    ],
                    "action_on_existence": "overwrite",
                }
            ]
        }

    def test_bulk_connection_extra_is_masked(self):
        """``extra`` is the load-bearing case for connections.

        Key-name redaction already covered a nested ``password`` -- but only while
        ``hide_sensitive_var_conn_fields`` is enabled, which is a deployment setting and is off
        in this test environment. ``extra`` was never covered by it at all: the name is not a
        recognised sensitive field, and the value is a JSON *string*, which ``redact`` returns
        unchanged. Masking ``extra`` is structural here, so it does not depend on that setting.
        """
        result = _mask_connection_fields(
            {
                "actions": [
                    {
                        "action": "create",
                        "entities": [
                            {
                                "connection_id": "c1",
                                "conn_type": "http",
                                "password": "CONN_LEAK_pw",
                                "extra": json.dumps({"token": "CONN_LEAK_token", "region": "eu"}),
                            }
                        ],
                    }
                ]
            }
        )
        entity = result["actions"][0]["entities"][0]
        assert entity["extra"] == {"token": "***", "region": "***"}
        assert entity["connection_id"] == "c1"
        # every value is gone, only the key names of ``extra`` remain
        assert "CONN_LEAK_token" not in json.dumps(result)

    @pytest.mark.parametrize(
        ("body", "masker"),
        [
            (
                {"actions": [{"action": "create", "entities": [{"key": "k", "value": "LEAK_v"}]}]},
                _mask_variable_fields,
            ),
            (
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [{"connection_id": "c", "extra": json.dumps({"t": "LEAK_v"})}],
                        }
                    ]
                },
                _mask_connection_fields,
            ),
        ],
        ids=["variables", "connections"],
    )
    def test_no_secret_survives_in_the_serialized_entry(self, body, masker):
        """The audit entry is serialized whole, so assert on the serialization, not one field."""
        assert "LEAK_v" not in json.dumps(masker(body))

    def test_multiple_actions_and_entities_are_all_masked(self):
        result = _mask_variable_fields(
            {
                "actions": [
                    {
                        "action": "create",
                        "entities": [{"key": "a", "value": "s1"}, {"key": "b", "value": "s2"}],
                    },
                    {"action": "update", "entities": [{"key": "c", "value": "s3"}]},
                ]
            }
        )
        values = [e["value"] for a in result["actions"] for e in a["entities"]]
        assert values == ["***", "***", "***"]

    def test_delete_by_key_entities_are_left_alone(self):
        """``delete`` may list bare keys rather than entity objects; those carry no secret."""
        body = {"actions": [{"action": "delete", "entities": ["key_one", "key_two"]}]}
        assert _mask_variable_fields(body) == body

    @pytest.mark.parametrize(
        "body",
        [
            {"key": "k", "value": "secret"},
            {"actions": "not-a-list"},
            {"actions": [{"action": "create"}]},
            {"actions": [{"action": "create", "entities": "not-a-list"}]},
            {"actions": ["not-a-dict"]},
        ],
        ids=["flat-body", "actions-not-list", "no-entities", "entities-not-list", "action-not-dict"],
    )
    def test_non_bulk_and_malformed_shapes_do_not_raise(self, body):
        """The masker runs on request bodies before validation, so it must not add a failure mode."""
        _mask_variable_fields(body)
        _mask_connection_fields(body)

    @pytest.mark.parametrize(
        "extra",
        [
            pytest.param({"token": "CONN_LEAK_token"}, id="already-decoded-object"),
            pytest.param(["CONN_LEAK_token"], id="already-decoded-array"),
            pytest.param(123, id="number"),
            pytest.param(True, id="bool"),
        ],
    )
    def test_non_string_extra_does_not_raise_and_does_not_leak(self, extra):
        """``extra`` reaches this before validation, so it need not be a string.

        ``json.loads`` raises ``TypeError`` rather than ``JSONDecodeError`` for a non-string, which
        would otherwise escape the audit-log decorator.
        """
        body = {"actions": [{"action": "create", "entities": [{"connection_id": "c1", "extra": extra}]}]}

        result = _mask_connection_fields(body)

        assert "CONN_LEAK_token" not in json.dumps(result)
        assert result["actions"][0]["entities"][0]["connection_id"] == "c1"

    def test_flat_body_still_takes_the_single_entity_path(self):
        assert _mask_variable_fields({"key": "k", "value": "secret"}) == {"key": "k", "value": "***"}


class TestActionLoggingUserFields:
    """The audit Log records the raw identifier in ``owner`` and the human-friendly
    ``get_display_name()`` in ``owner_display_name``."""

    def test_owner_and_display_name_use_the_matching_user_methods(self):
        class FakeUser(BaseUser):
            def get_id(self) -> str:
                return "id"

            def get_name(self) -> str:
                return "jdoe"

            def get_display_name(self) -> str:
                return "Jane Doe"

        request = Request({"type": "http", "method": "GET", "headers": [], "query_string": b""})
        session = MagicMock(spec=Session)

        asyncio.run(action_logging(event="test_event")(request=request, session=session, user=FakeUser()))

        (logged,) = session.add.call_args.args
        assert logged.owner == "jdoe"
        assert logged.owner_display_name == "Jane Doe"


class TestActionLoggingTeamName:
    """A team-scoped resource that owns no Dag records its team on the Log row itself, since
    there is no ``dag_id`` to resolve one through."""

    @pytest.mark.parametrize(
        ("query_string", "expected_team_name"),
        [
            pytest.param(b"team_name=payments", "payments", id="team-scoped"),
            pytest.param(b"", None, id="not-team-scoped"),
        ],
    )
    def test_team_name_is_taken_from_the_request(self, query_string, expected_team_name):
        request = Request({"type": "http", "method": "POST", "headers": [], "query_string": query_string})
        session = MagicMock(spec=Session)

        asyncio.run(action_logging(event="post_pool")(request=request, session=session, user=None))

        (logged,) = session.add.call_args.args
        assert logged.team_name == expected_team_name

    @pytest.mark.parametrize(
        "query_string",
        [
            pytest.param(b"team_name=" + b"a" * 60, id="longer-than-the-column"),
            pytest.param(b"team_name=Payments", id="not-a-name-a-team-can-have"),
        ],
    )
    def test_a_name_no_team_can_have_is_not_recorded(self, query_string):
        """The row is committed before the endpoint validates the request, so a name the column
        cannot hold would fail the insert instead of the request."""
        request = Request({"type": "http", "method": "POST", "headers": [], "query_string": query_string})
        session = MagicMock(spec=Session)

        asyncio.run(action_logging(event="post_pool")(request=request, session=session, user=None))

        (logged,) = session.add.call_args.args
        assert logged.team_name is None


@pytest.mark.db_test
class TestActionLoggingResourceTeamName:
    """An action that names no team -- a deletion, or a patch leaving ``team_name`` out -- takes the
    team from the resource it acts on, which this dependency can still read because it runs before
    the endpoint."""

    def teardown_method(self):
        clear_db_logs()
        clear_db_pools()
        clear_db_variables()
        clear_db_connections()
        clear_db_dags()
        clear_db_dag_bundles()
        clear_db_teams()

    @staticmethod
    def _create_resources_owned_by_a_team(session, team_name="payments"):
        session.add(Team(name=team_name))
        session.flush()
        session.add(Pool(pool="team_pool", slots=1, include_deferred=False, team_name=team_name))
        session.add(Variable(key="team_var", val="something", team_name=team_name))
        session.add(Connection(conn_id="team_conn", conn_type="http", team_name=team_name))
        session.commit()

    @staticmethod
    def _log_action(session, path_params):
        request = Request(
            {
                "type": "http",
                "method": "DELETE",
                "headers": [],
                "query_string": b"",
                "path_params": path_params,
            }
        )
        asyncio.run(action_logging(event="delete_resource")(request=request, session=session, user=None))
        return session.scalar(select(Log).order_by(Log.id.desc()))

    @pytest.mark.parametrize(
        "path_params",
        [
            pytest.param({"pool_name": "team_pool"}, id="pool"),
            pytest.param({"variable_key": "team_var"}, id="variable"),
            pytest.param({"connection_id": "team_conn"}, id="connection"),
        ],
    )
    @conf_vars({("core", "multi_team"): "True"})
    def test_team_comes_from_the_resource_being_acted_on(self, session, path_params):
        self._create_resources_owned_by_a_team(session)

        assert self._log_action(session, path_params).team_name == "payments"

    @conf_vars({("core", "multi_team"): "True"})
    def test_no_team_is_recorded_for_a_resource_owned_by_none(self, session):
        session.add(Pool(pool="team_pool", slots=1, include_deferred=False))
        session.commit()

        assert self._log_action(session, {"pool_name": "team_pool"}).team_name is None

    def test_the_resource_is_not_read_when_multi_team_is_off(self, session):
        self._create_resources_owned_by_a_team(session)

        assert self._log_action(session, {"pool_name": "team_pool"}).team_name is None

    @conf_vars({("core", "multi_team"): "True"})
    def test_a_dag_scoped_event_keeps_the_team_of_its_dag(self, session):
        """The Dag's team is stamped when the row is inserted, so a resource named alongside it
        must not take precedence."""
        self._create_resources_owned_by_a_team(session)
        bundle = DagBundleModel(name="team-bundle")
        bundle.teams.append(Team(name="infra"))
        session.add(bundle)
        session.flush()
        session.add(DagModel(dag_id="dag_owned_by_infra", bundle_name="team-bundle", is_stale=False))
        session.commit()
        clear_team_name_cache()

        log = self._log_action(session, {"dag_id": "dag_owned_by_infra", "pool_name": "team_pool"})

        assert log.team_name == "infra"
