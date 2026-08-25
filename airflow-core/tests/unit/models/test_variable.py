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

import logging
import os
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import select

from airflow.models import Variable, crypto, variable
from airflow.sdk import SecretCache
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.sqlalchemy import prohibit_commit

from tests_common.test_utils import db
from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.team import Team

pytestmark = pytest.mark.db_test


class _SessionUnawareMetastoreBackend(MetastoreBackend):
    """A custom backend whose ``get_variable`` override predates the ``session`` keyword."""

    # The signature mismatch with the base class is the point of this fixture, so mypy's
    # override check has to be waived here rather than fixed.
    def get_variable(self, key: str, team_name: str | None = None) -> str | None:  # type: ignore[override]
        return "from_subclass"


class _SessionAwareMetastoreBackend(MetastoreBackend):
    """A custom backend whose ``get_variable`` override does accept ``session``."""

    def __init__(self):
        super().__init__()
        self.received_session: Session | None = None

    def get_variable(
        self, key: str, team_name: str | None = None, *, session: Session | None = None
    ) -> str | None:
        self.received_session = session
        return "from_subclass"


class TestVariable:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        db.clear_db_variables()
        SecretCache.reset()
        with conf_vars({("secrets", "use_cache"): "true"}):
            SecretCache.init()
        with mock.patch("airflow.models.variable.mask_secret", autospec=True) as m:
            self.mask_secret = m
            yield
        db.clear_db_variables()

    @conf_vars({("core", "fernet_key"): "", ("core", "unit_test_mode"): "True"})
    def test_variable_no_encryption(self, session):
        """
        Test variables without encryption
        """
        crypto.get_fernet.cache_clear()
        Variable.set(key="key", value="value", session=session)
        test_var = session.scalar(select(Variable).where(Variable.key == "key"))
        assert not test_var.is_encrypted
        assert test_var.val == "value"
        # We always call mask_secret for variables, and let the SecretsMasker decide based on the name if it
        # should mask anything. That logic is tested in test_secrets_masker.py
        self.mask_secret.assert_called_once_with("value", "key")

    @conf_vars({("core", "fernet_key"): Fernet.generate_key().decode()})
    def test_variable_with_encryption(self, session):
        """
        Test variables with encryption
        """
        crypto.get_fernet.cache_clear()
        Variable.set(key="key", value="value", session=session)
        test_var = session.scalar(select(Variable).where(Variable.key == "key"))
        assert test_var.is_encrypted
        assert test_var.val == "value"

    @pytest.mark.parametrize("test_value", ["value", ""])
    def test_var_with_encryption_rotate_fernet_key(self, test_value, session):
        """
        Tests rotating encrypted variables.
        """
        key1 = Fernet.generate_key()
        key2 = Fernet.generate_key()

        with conf_vars({("core", "fernet_key"): key1.decode()}):
            crypto.get_fernet.cache_clear()
            Variable.set(key="key", value=test_value, session=session)
            test_var = session.scalar(select(Variable).where(Variable.key == "key"))
            assert test_var.is_encrypted
            assert test_var.val == test_value
            assert Fernet(key1).decrypt(test_var._val.encode()) == test_value.encode()

        # Test decrypt of old value with new key
        with conf_vars({("core", "fernet_key"): f"{key2.decode()},{key1.decode()}"}):
            crypto.get_fernet.cache_clear()
            assert test_var.val == test_value

            # Test decrypt of new value with new key
            test_var.rotate_fernet_key()
            assert test_var.is_encrypted
            assert test_var.val == test_value
            assert Fernet(key2).decrypt(test_var._val.encode()) == test_value.encode()

    @conf_vars({("core", "multi_team"): "True"})
    def test_get_variable_with_team(self, testing_team, session):
        Variable.set(key="key", value="value", team_name=testing_team.name, session=session)
        result = Variable.get(key="key", team_name=testing_team.name)
        assert result == "value"

    @conf_vars({("core", "multi_team"): "True"})
    def test_get_global_variable_with_team(self, testing_team, session):
        Variable.set(key="key", value="value", session=session)
        result = Variable.get(key="key", team_name=testing_team.name)
        assert result == "value"

    @conf_vars({("core", "multi_team"): "True"})
    def test_get_team_variable_without_team(self, testing_team, session):
        Variable.set(key="key", value="value", team_name=testing_team.name, session=session)
        with pytest.raises(KeyError):
            Variable.get(key="key")

    def test_variable_set_get_round_trip(self):
        Variable.set("tested_var_set_id", "Monday morning breakfast")
        assert Variable.get("tested_var_set_id") == "Monday morning breakfast"

    def test_set_val_rejects_non_string_with_clear_error(self):
        var = Variable(key="a_key")
        with pytest.raises(TypeError, match="Variable value must be a string, got list"):
            var.val = ["a", "b"]

    def test_variable_set_with_env_variable(self, caplog, session):
        caplog.set_level(logging.WARNING, logger=variable.log.name)
        Variable.set(key="key", value="db-value", session=session)
        with mock.patch.dict("os.environ", AIRFLOW_VAR_KEY="env-value"):
            # setting value while shadowed by an env variable will generate a warning
            Variable.set(key="key", value="new-db-value", session=session)
            # value set above is not returned because the env variable value takes priority
            assert Variable.get("key") == "env-value"
        # invalidate the cache to re-evaluate value
        SecretCache.invalidate_variable("key")
        # now that env var is not here anymore, we see the value we set before.
        assert Variable.get("key") == "new-db-value"

        assert caplog.messages[0] == (
            "The variable key is defined in the EnvironmentVariablesBackend secrets backend, "
            "which takes precedence over reading from the database. The value in the database "
            "will be updated, but to read it you have to delete the conflicting variable from "
            "EnvironmentVariablesBackend"
        )

    def test_variable_set_update_existing(self, session):
        Variable.set(key="test_key", value="initial_value", session=session)

        initial_var = session.scalar(select(Variable).where(Variable.key == "test_key"))
        initial_id = initial_var.id

        # Need to expire session cache to fetch fresh data from db on next query
        # Without this, SQLAlchemy will return the cached object with old values
        # instead of querying the database again for the updated values
        session.expire(initial_var)

        Variable.set(key="test_key", value="updated_value", session=session)

        updated_var = session.scalar(select(Variable).where(Variable.key == "test_key"))

        # 1. The ID remains the same (no delete-insert)
        assert updated_var.id == initial_id, "Variable ID should remain the same after update"

        # 2. The value is updated to the new value
        assert updated_var.val == "updated_value", "Variable value should be updated to the new value"

    @mock.patch("airflow.models.variable.ensure_secrets_loaded")
    def test_variable_set_with_extra_secret_backend(self, mock_ensure_secrets, caplog, session):
        caplog.set_level(logging.WARNING, logger=variable.log.name)
        mock_backend = mock.Mock()
        mock_backend.get_variable.return_value = "secret_val"
        mock_backend.__class__.__name__ = "MockSecretsBackend"
        mock_ensure_secrets.return_value = [mock_backend, MetastoreBackend]

        Variable.set(key="key", value="new-db-value", session=session)
        assert Variable.get("key") == "secret_val"

        assert caplog.messages[0] == (
            "The variable key is defined in the MockSecretsBackend secrets backend, "
            "which takes precedence over reading from the database. The value in the database "
            "will be updated, but to read it you have to delete the conflicting variable from "
            "MockSecretsBackend"
        )
        Variable.delete(key="key", session=session)

    @mock.patch.object(MetastoreBackend, "get_variable", autospec=True)
    @mock.patch("airflow.models.variable.ensure_secrets_loaded")
    def test_get_forwards_session_to_metastore_backend(self, mock_ensure_secrets, mock_get_variable, session):
        mock_get_variable.return_value = "from_db"
        mock_ensure_secrets.return_value = [MetastoreBackend()]

        assert Variable.get("some_key", session=session) == "from_db"
        assert mock_get_variable.call_args.kwargs["session"] is session

    @mock.patch.object(MetastoreBackend, "get_variable", autospec=True)
    @mock.patch("airflow.models.variable.ensure_secrets_loaded")
    def test_get_without_session_omits_session_kwarg(self, mock_ensure_secrets, mock_get_variable):
        mock_get_variable.return_value = "from_db"
        mock_ensure_secrets.return_value = [MetastoreBackend()]

        assert Variable.get("some_key") == "from_db"
        assert "session" not in mock_get_variable.call_args.kwargs

    @mock.patch("airflow.models.variable.ensure_secrets_loaded")
    def test_get_does_not_forward_session_to_other_backends(self, mock_ensure_secrets, session):
        """Only the metastore backend reads the metadata database, so only it accepts a session."""
        mock_backend = mock.Mock()
        mock_backend.get_variable.return_value = "from_backend"
        mock_backend.__class__.__name__ = "MockSecretsBackend"
        mock_ensure_secrets.return_value = [mock_backend]

        assert Variable.get("some_key", session=session) == "from_backend"
        assert "session" not in mock_backend.get_variable.call_args.kwargs

    @mock.patch("airflow.models.variable.ensure_secrets_loaded")
    def test_get_omits_session_for_session_unaware_metastore_subclass(self, mock_ensure_secrets, session):
        """Forwarding a session to an override that predates it would read as a missing Variable."""
        mock_ensure_secrets.return_value = [_SessionUnawareMetastoreBackend()]

        assert Variable.get("some_key", session=session) == "from_subclass"

    @mock.patch("airflow.models.variable.ensure_secrets_loaded")
    def test_get_forwards_session_to_session_aware_metastore_subclass(self, mock_ensure_secrets, session):
        """A subclass that does accept a session still receives it, so it reuses the transaction."""
        backend = _SessionAwareMetastoreBackend()
        mock_ensure_secrets.return_value = [backend]

        assert Variable.get("some_key", session=session) == "from_subclass"
        assert backend.received_session is session

    def test_get_with_session_does_not_commit_under_prohibit_commit(self, session):
        """
        A caller holding an open transaction can read a Variable without its session being committed.

        Without the session being forwarded, ``MetastoreBackend.get_variable``'s ``provide_session``
        takes the same scoped session and commits it, which the guard rejects.
        """
        Variable.set(key="interval_key", value="60", session=session)
        session.commit()
        SecretCache.invalidate_variable("interval_key")

        with prohibit_commit(session):
            assert Variable.get("interval_key", session=session) == "60"

    def test_update_with_session_does_not_commit_under_prohibit_commit(self, session):
        """``update`` verifies existence through the secrets chain, which must reuse the session too."""
        Variable.set(key="interval_key", value="60", session=session)
        session.commit()
        SecretCache.invalidate_variable("interval_key")

        with prohibit_commit(session):
            Variable.update(key="interval_key", value="120", session=session)

    def test_setdefault_with_session_does_not_commit_under_prohibit_commit(self, session):
        """``setdefault`` reads through the secrets chain before deciding whether to write."""
        Variable.set(key="interval_key", value="60", session=session)
        session.commit()
        SecretCache.invalidate_variable("interval_key")

        with prohibit_commit(session):
            assert Variable.setdefault("interval_key", "120", session=session) == "60"

    def test_setdefault_writes_default_with_session_under_prohibit_commit(self, session):
        """The write half must reuse the session too, so the miss path stays inside the transaction."""
        with prohibit_commit(session):
            assert Variable.setdefault("absent_key", "30", session=session) == "30"
        session.commit()

        assert Variable.get("absent_key", session=session) == "30"

    def test_get_rejects_session_in_execution_context(self):
        """Reads from an execution context go via the Execution API, where a session is meaningless."""
        task_runner = mock.Mock(SUPERVISOR_COMMS=mock.Mock())
        with (
            mock.patch.dict("sys.modules", {"airflow.sdk.execution_time.task_runner": task_runner}),
            pytest.raises(ValueError, match="cannot use a metadata database session"),
        ):
            Variable.get("some_key", session=mock.Mock())

    def test_variable_set_get_round_trip_json(self):
        value = {"a": 17, "b": 47}
        Variable.set(key="tested_var_set_id", value=value, serialize_json=True)
        assert value == Variable.get("tested_var_set_id", deserialize_json=True)

    def test_variable_update(self, session):
        Variable.set(key="test_key", value="value1", session=session)
        assert Variable.get(key="test_key") == "value1"
        Variable.update(key="test_key", value="value2", session=session)
        assert Variable.get("test_key") == "value2"

    def test_variable_update_fails_on_non_metastore_variable(self, session):
        with mock.patch.dict("os.environ", AIRFLOW_VAR_KEY="env-value"):
            with pytest.raises(AttributeError):
                Variable.update(key="key", value="new-value", session=session)

    def test_variable_update_preserves_description(self, session):
        Variable.set(key="key", value="value", description="a test variable", session=session)
        assert Variable.get("key") == "value"
        Variable.update("key", "value2")
        test_var = session.scalar(select(Variable).where(Variable.key == "key"))
        assert test_var.val == "value2"
        assert test_var.description == "a test variable"

    @conf_vars({("core", "multi_team"): "True"})
    def test_variable_update_with_team(self, testing_team, session):
        Variable.set(key="test_key", value="value1", team_name=testing_team.name, session=session)
        Variable.update(key="test_key", value="value2", team_name=testing_team.name, session=session)
        assert Variable.get("test_key", team_name=testing_team.name) == "value2"

    @conf_vars({("core", "multi_team"): "True"})
    def test_variable_update_with_team_global(self, testing_team, session):
        Variable.set(key="test_key", value="value1", session=session)
        Variable.update(key="test_key", value="value2", team_name=testing_team.name, session=session)
        assert Variable.get("test_key", team_name=testing_team.name) == "value2"

    @conf_vars({("core", "multi_team"): "True"})
    def test_variable_update_with_wrong_team(self, testing_team, session):
        Variable.set(key="test_key", value="value1", team_name=testing_team.name, session=session)
        with pytest.raises(KeyError):
            Variable.update(key="test_key", value="value2", session=session)

    def test_set_variable_sets_description(self, session):
        Variable.set(key="key", value="value", description="a test variable", session=session)
        test_var = session.scalar(select(Variable).where(Variable.key == "key"))
        assert test_var.description == "a test variable"
        assert test_var.val == "value"

    @conf_vars({("core", "multi_team"): "True"})
    def test_set_variable_sets_team(self, testing_team, session):
        Variable.set(key="key", value="value", team_name=testing_team.name, session=session)
        test_var = session.scalar(select(Variable).where(Variable.key == "key"))
        assert test_var.team_name == testing_team.name
        assert test_var.val == "value"

    def test_set_variable_sets_team_multi_team_off(self, testing_team, session):
        with pytest.raises(ValueError, match=r"Multi-team mode is not configured in the Airflow environment"):
            Variable.set(key="key", value="value", team_name=testing_team.name, session=session)

    def test_variable_set_existing_value_to_blank(self, session):
        test_value = "Some value"
        test_key = "test_key"
        Variable.set(key=test_key, value=test_value, session=session)
        Variable.set(key=test_key, value="", session=session)
        assert Variable.get("test_key") == ""

    def test_get_non_existing_var_should_return_default(self):
        default_value = "some default val"
        assert default_value == Variable.get("thisIdDoesNotExist", default_var=default_value)

    def test_get_non_existing_var_should_raise_key_error(self):
        with pytest.raises(KeyError):
            Variable.get("thisIdDoesNotExist")

    def test_update_non_existing_var_should_raise_key_error(self, session):
        with pytest.raises(KeyError):
            Variable.update(key="thisIdDoesNotExist", value="value", session=session)

    def test_get_non_existing_var_with_none_default_should_return_none(self):
        assert Variable.get("thisIdDoesNotExist", default_var=None) is None

    def test_get_non_existing_var_should_not_deserialize_json_default(self):
        default_value = "}{ this is a non JSON default }{"
        assert default_value == Variable.get(
            "thisIdDoesNotExist", default_var=default_value, deserialize_json=True
        )

    def test_variable_setdefault_round_trip(self, session):
        key = "tested_var_setdefault_1_id"
        value = "Monday morning breakfast in Paris"
        Variable.setdefault(key=key, default=value)
        assert value == Variable.get(key)

    def test_variable_setdefault_round_trip_json(self, session):
        key = "tested_var_setdefault_2_id"
        value = {"city": "Paris", "Happiness": True}
        Variable.setdefault(key=key, default=value, deserialize_json=True)
        assert value == Variable.get(key, deserialize_json=True)

    def test_variable_setdefault_existing_json(self, session):
        key = "tested_var_setdefault_2_id"
        value = {"city": "Paris", "Happiness": True}
        Variable.set(key=key, value=value, serialize_json=True, session=session)
        val = Variable.setdefault(key=key, default=value, deserialize_json=True)
        # Check the returned value, and the stored value are handled correctly.
        assert value == val
        assert value == Variable.get(key, deserialize_json=True)

    def test_variable_delete(self, session):
        key = "tested_var_delete"
        value = "to be deleted"

        # No-op if the variable doesn't exist
        Variable.delete(key=key, session=session)
        with pytest.raises(KeyError):
            Variable.get(key)

        # Set the variable
        Variable.set(key=key, value=value, session=session)
        assert value == Variable.get(key)

        # Delete the variable
        Variable.delete(key=key, session=session)
        with pytest.raises(KeyError):
            Variable.get(key)

    @conf_vars({("core", "multi_team"): "True"})
    def test_variable_delete_with_team(self, testing_team, session):
        key = "tested_var_delete"
        value = "to be deleted"

        # No-op if the variable doesn't exist
        Variable.delete(key=key, team_name=testing_team.name, session=session)
        with pytest.raises(KeyError):
            Variable.get(key)

        # Delete same team variable
        Variable.set(key=key, value=value, team_name=testing_team.name, session=session)
        Variable.delete(key=key, team_name=testing_team.name, session=session)
        with pytest.raises(KeyError):
            Variable.get(key)

        # Delete global variable
        Variable.set(key=key, value=value, session=session)
        Variable.delete(key=key, team_name=testing_team.name, session=session)
        with pytest.raises(KeyError):
            Variable.get(key)

        # Attempt to delete a team variable from another one
        Variable.set(key=key, value=value, team_name=testing_team.name, session=session)
        assert Variable.delete(key=key, session=session) == 0

    def test_masking_from_db(self, session):
        """Test secrets are masked when loaded directly from the DB"""
        # Normally people will use `Variable.get`, but just in case, catch direct DB access too
        try:
            var = Variable(
                key=f"password-{os.getpid()}",
                val="s3cr3t",
            )
            session.add(var)
            session.flush()

            # Make sure we re-load it, not just get the cached object back
            session.expunge(var)

            self.mask_secret.reset_mock()

            session.get(Variable, var.id)

            assert self.mask_secret.mock_calls == [
                # We should have called it _again_ when loading from the DB
                mock.call("s3cr3t", var.key),
            ]
        finally:
            session.rollback()

    @conf_vars({("core", "multi_team"): "True"})
    def test_variable_set_does_not_change_team_name_on_update(self, testing_team, session):
        Variable.set(key="k", value="v1", session=session)

        Variable.set(key="k", value="v2", team_name=testing_team.name, session=session)

        assert Variable.get("k") == "v2"

    @mock.patch("airflow.models.variable.ensure_secrets_loaded")
    def test_caching_caches(self, mock_ensure_secrets: mock.Mock):
        mock_backend = mock.Mock()
        mock_backend.get_variable.return_value = "secret_val"
        mock_backend.__class__.__name__ = "MockSecretsBackend"
        mock_ensure_secrets.return_value = [mock_backend, MetastoreBackend]

        key = "doesn't matter"
        first = Variable.get(key)
        second = Variable.get(key)

        mock_backend.get_variable.assert_called_once()  # second call was not made because of cache
        assert first == second

    def test_cache_invalidation_on_set(self, session):
        with mock.patch.dict("os.environ", AIRFLOW_VAR_KEY="from_env"):
            a = Variable.get("key")  # value is saved in cache
        with mock.patch.dict("os.environ", AIRFLOW_VAR_KEY="from_env_two"):
            b = Variable.get("key")  # value from cache is used
        assert a == b

        # setting a new value invalidates the cache
        Variable.set(key="key", value="new_value", session=session)

        c = Variable.get("key")  # cache should not be used

        assert c != b

    def test_get_team_name(self, testing_team: Team, session: Session):
        var = Variable(key="key", val="value", team_name=testing_team.name)
        session.add(var)
        session.flush()

        assert Variable.get_team_name("key", session=session) == "testing"

    def test_get_key_to_team_name_mapping(self, testing_team: Team, session: Session):
        var1 = Variable(key="key1", val="value1", team_name=testing_team.name)
        var2 = Variable(key="key2", val="value2")
        session.add(var1)
        session.add(var2)
        session.flush()

        assert Variable.get_key_to_team_name_mapping(["key1", "key2"], session=session) == {
            "key1": "testing",
            "key2": None,
        }


@pytest.mark.parametrize(
    ("variable_value", "deserialize_json", "expected_masked_values"),
    [
        ("s3cr3t", False, ["s3cr3t"]),
        ('{"api_key": "s3cr3t"}', True, ["s3cr3t"]),
        ('{"api_key": "s3cr3t", "normal_key": "normal_value"}', True, ["s3cr3t"]),
        ('{"api_key": "s3cr3t", "another_secret": "123456"}', True, ["s3cr3t", "123456"]),
    ],
)
def test_masking_only_secret_values(variable_value, deserialize_json, expected_masked_values, session):
    from airflow._shared.secrets_masker import _secrets_masker

    SecretCache.reset()

    try:
        var = Variable(
            key=f"password-{os.getpid()}",
            val=variable_value,
        )
        session.add(var)
        session.commit()
        # Make sure we re-load it, not just get the cached object back
        session.expunge(var)
        _secrets_masker().patterns = set()

        Variable.get(var.key, deserialize_json=deserialize_json)

        for expected_masked_value in expected_masked_values:
            assert expected_masked_value in _secrets_masker().patterns
    finally:
        db.clear_db_variables()
