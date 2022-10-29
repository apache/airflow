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

import os
import unittest
from unittest import mock

import pytest
from cryptography.fernet import Fernet
from parameterized import parameterized

from airflow import settings
from airflow.models import Variable, crypto, variable
from airflow.secrets.metastore import MetastoreBackend
from tests.test_utils import db
from tests.test_utils.config import conf_vars


class TestVariable(unittest.TestCase):
    def setUp(self):
        crypto._fernet = None
        db.clear_db_variables()
        patcher = mock.patch("airflow.models.variable.mask_secret", autospec=True)
        self.mask_secret = patcher.start()

        self.addCleanup(patcher.stop)

    def tearDown(self):
        crypto._fernet = None

    @classmethod
    def tearDownClass(cls):
        db.clear_db_variables()

    @conf_vars({("core", "fernet_key"): ""})
    def test_variable_no_encryption(self):
        """
        Test variables without encryption
        """
        Variable.set("key", "value")
        session = settings.Session()
        test_var = session.query(Variable).filter(Variable.key == "key").one()
        assert not test_var.is_encrypted
        assert test_var.val == "value"
        # We always call mask_secret for variables, and let the SecretsMasker decide based on the name if it
        # should mask anything. That logic is tested in test_secrets_masker.py
        self.mask_secret.assert_called_once_with("value", "key")

    @conf_vars({("core", "fernet_key"): Fernet.generate_key().decode()})
    def test_variable_with_encryption(self):
        """
        Test variables with encryption
        """
        Variable.set("key", "value")
        session = settings.Session()
        test_var = session.query(Variable).filter(Variable.key == "key").one()
        assert test_var.is_encrypted
        assert test_var.val == "value"

    @parameterized.expand(["value", ""])
    def test_var_with_encryption_rotate_fernet_key(self, test_value):
        """
        Tests rotating encrypted variables.
        """
        key1 = Fernet.generate_key()
        key2 = Fernet.generate_key()

        with conf_vars({("core", "fernet_key"): key1.decode()}):
            Variable.set("key", test_value)
            session = settings.Session()
            test_var = session.query(Variable).filter(Variable.key == "key").one()
            assert test_var.is_encrypted
            assert test_var.val == test_value
            assert Fernet(key1).decrypt(test_var._val.encode()) == test_value.encode()

        # Test decrypt of old value with new key
        with conf_vars({("core", "fernet_key"): ",".join([key2.decode(), key1.decode()])}):
            crypto._fernet = None
            assert test_var.val == test_value

            # Test decrypt of new value with new key
            test_var.rotate_fernet_key()
            assert test_var.is_encrypted
            assert test_var.val == test_value
            assert Fernet(key2).decrypt(test_var._val.encode()) == test_value.encode()

    def test_variable_set_get_round_trip(self):
        Variable.set("tested_var_set_id", "Monday morning breakfast")
        assert "Monday morning breakfast" == Variable.get("tested_var_set_id")

    def test_variable_set_with_env_variable(self):
        Variable.set("key", "db-value")
        with self.assertLogs(variable.log) as log_context:
            with mock.patch.dict("os.environ", AIRFLOW_VAR_KEY="env-value"):
                Variable.set("key", "new-db-value")
                assert "env-value" == Variable.get("key")
            assert "new-db-value" == Variable.get("key")

        assert log_context.records[0].message == (
            "The variable key is defined in the EnvironmentVariablesBackend secrets backend, "
            "which takes precedence over reading from the database. The value in the database "
            "will be updated, but to read it you have to delete the conflicting variable from "
            "EnvironmentVariablesBackend"
        )

    @mock.patch("airflow.models.variable.ensure_secrets_loaded")
    def test_variable_set_with_extra_secret_backend(self, mock_ensure_secrets):

        mock_backend = mock.Mock()
        mock_backend.get_variable.return_value = "secret_val"
        mock_backend.__class__.__name__ = "MockSecretsBackend"
        mock_ensure_secrets.return_value = [mock_backend, MetastoreBackend]

        with self.assertLogs(variable.log) as log_context:
            Variable.set("key", "new-db-value")

        assert Variable.get("key") == "secret_val"

        assert log_context.records[0].message == (
            "The variable key is defined in the MockSecretsBackend secrets backend, "
            "which takes precedence over reading from the database. The value in the database "
            "will be updated, but to read it you have to delete the conflicting variable from "
            "MockSecretsBackend"
        )

    def test_variable_set_get_round_trip_json(self):
        value = {"a": 17, "b": 47}
        Variable.set("tested_var_set_id", value, serialize_json=True)
        assert value == Variable.get("tested_var_set_id", deserialize_json=True)

    def test_variable_update(self):
        Variable.set("test_key", "value1")
        assert "value1" == Variable.get("test_key")
        Variable.update("test_key", "value2")
        assert "value2" == Variable.get("test_key")

    def test_variable_update_fails_on_non_metastore_variable(self):
        with mock.patch.dict("os.environ", AIRFLOW_VAR_KEY="env-value"):
            with pytest.raises(AttributeError):
                Variable.update("key", "new-value")

    def test_variable_update_preserves_description(self):
        Variable.set("key", "value", description="a test variable")
        assert Variable.get("key") == "value"
        Variable.update("key", "value2")
        session = settings.Session()
        test_var = session.query(Variable).filter(Variable.key == "key").one()
        assert test_var.val == "value2"
        assert test_var.description == "a test variable"

    def test_set_variable_sets_description(self):
        Variable.set("key", "value", description="a test variable")
        session = settings.Session()
        test_var = session.query(Variable).filter(Variable.key == "key").one()
        assert test_var.description == "a test variable"
        assert test_var.val == "value"

    def test_variable_set_existing_value_to_blank(self):
        test_value = "Some value"
        test_key = "test_key"
        Variable.set(test_key, test_value)
        Variable.set(test_key, "")
        assert "" == Variable.get("test_key")

    def test_get_non_existing_var_should_return_default(self):
        default_value = "some default val"
        assert default_value == Variable.get("thisIdDoesNotExist", default_var=default_value)

    def test_get_non_existing_var_should_raise_key_error(self):
        with pytest.raises(KeyError):
            Variable.get("thisIdDoesNotExist")

    def test_update_non_existing_var_should_raise_key_error(self):
        with pytest.raises(KeyError):
            Variable.update("thisIdDoesNotExist", "value")

    def test_get_non_existing_var_with_none_default_should_return_none(self):
        assert Variable.get("thisIdDoesNotExist", default_var=None) is None

    def test_get_non_existing_var_should_not_deserialize_json_default(self):
        default_value = "}{ this is a non JSON default }{"
        assert default_value == Variable.get(
            "thisIdDoesNotExist", default_var=default_value, deserialize_json=True
        )

    def test_variable_setdefault_round_trip(self):
        key = "tested_var_setdefault_1_id"
        value = "Monday morning breakfast in Paris"
        Variable.setdefault(key, value)
        assert value == Variable.get(key)

    def test_variable_setdefault_round_trip_json(self):
        key = "tested_var_setdefault_2_id"
        value = {"city": "Paris", "Happiness": True}
        Variable.setdefault(key, value, deserialize_json=True)
        assert value == Variable.get(key, deserialize_json=True)

    def test_variable_setdefault_existing_json(self):
        key = "tested_var_setdefault_2_id"
        value = {"city": "Paris", "Happiness": True}
        Variable.set(key, value, serialize_json=True)
        val = Variable.setdefault(key, value, deserialize_json=True)
        # Check the returned value, and the stored value are handled correctly.
        assert value == val
        assert value == Variable.get(key, deserialize_json=True)

    def test_variable_delete(self):
        key = "tested_var_delete"
        value = "to be deleted"

        # No-op if the variable doesn't exist
        Variable.delete(key)
        with pytest.raises(KeyError):
            Variable.get(key)

        # Set the variable
        Variable.set(key, value)
        assert value == Variable.get(key)

        # Delete the variable
        Variable.delete(key)
        with pytest.raises(KeyError):
            Variable.get(key)

    def test_masking_from_db(self):
        """Test secrets are masked when loaded directly from the DB"""

        # Normally people will use `Variable.get`, but just in case, catch direct DB access too

        session = settings.Session()

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

            session.query(Variable).get(var.id)

            assert self.mask_secret.mock_calls == [
                # We should have called it _again_ when loading from the DB
                mock.call("s3cr3t", var.key),
            ]
        finally:
            session.rollback()
