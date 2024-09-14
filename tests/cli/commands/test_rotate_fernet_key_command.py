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

from unittest import mock

import pytest
from cryptography.fernet import Fernet

from airflow.cli import cli_parser
from airflow.cli.commands import rotate_fernet_key_command
from airflow.hooks.base import BaseHook
from airflow.models import Connection, Variable
from airflow.utils.session import provide_session
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_connections, clear_db_variables

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


class TestRotateFernetKeyCommand:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def setup_method(self) -> None:
        clear_db_connections(add_default_connections_back=False)
        clear_db_variables()

    def teardown_method(self) -> None:
        clear_db_connections(add_default_connections_back=False)
        clear_db_variables()

    @provide_session
    def test_should_rotate_variable(self, session):
        fernet_key1 = Fernet.generate_key()
        fernet_key2 = Fernet.generate_key()
        var1_key = f"{__file__}_var1"
        var2_key = f"{__file__}_var2"

        # Create unencrypted variable
        with conf_vars({("core", "fernet_key"): ""}), mock.patch("airflow.models.crypto._fernet", None):
            Variable.set(key=var1_key, value="value")

        # Create encrypted variable
        with conf_vars({("core", "fernet_key"): fernet_key1.decode()}), mock.patch(
            "airflow.models.crypto._fernet", None
        ):
            Variable.set(key=var2_key, value="value")

        # Rotate fernet key
        with conf_vars(
            {("core", "fernet_key"): f"{fernet_key2.decode()},{fernet_key1.decode()}"}
        ), mock.patch("airflow.models.crypto._fernet", None):
            args = self.parser.parse_args(["rotate-fernet-key"])
            rotate_fernet_key_command.rotate_fernet_key(args)

        # Assert correctness using a new fernet key
        with conf_vars({("core", "fernet_key"): fernet_key2.decode()}), mock.patch(
            "airflow.models.crypto._fernet", None
        ):
            var1 = session.query(Variable).filter(Variable.key == var1_key).first()
            # Unencrypted variable should be unchanged
            assert Variable.get(key=var1_key) == "value"
            assert var1._val == "value"
            assert Variable.get(key=var2_key) == "value"

    @provide_session
    def test_should_rotate_connection(self, session):
        fernet_key1 = Fernet.generate_key()
        fernet_key2 = Fernet.generate_key()
        var1_key = f"{__file__}_var1"
        var2_key = f"{__file__}_var2"

        # Create unencrypted variable
        with conf_vars({("core", "fernet_key"): ""}), mock.patch("airflow.models.crypto._fernet", None):
            session.add(Connection(conn_id=var1_key, uri="mysql://user:pass@localhost"))
            session.commit()

        # Create encrypted variable
        with conf_vars({("core", "fernet_key"): fernet_key1.decode()}), mock.patch(
            "airflow.models.crypto._fernet", None
        ):
            session.add(Connection(conn_id=var2_key, uri="mysql://user:pass@localhost"))
            session.commit()

        # Rotate fernet key
        with conf_vars(
            {("core", "fernet_key"): f"{fernet_key2.decode()},{fernet_key1.decode()}"}
        ), mock.patch("airflow.models.crypto._fernet", None):
            args = self.parser.parse_args(["rotate-fernet-key"])
            rotate_fernet_key_command.rotate_fernet_key(args)

        # Assert correctness using a new fernet key
        with conf_vars({("core", "fernet_key"): fernet_key2.decode()}), mock.patch(
            "airflow.models.crypto._fernet", None
        ):
            # Unencrypted variable should be unchanged
            conn1: Connection = BaseHook.get_connection(var1_key)
            assert conn1.password == "pass"
            assert conn1._password == "pass"
            assert BaseHook.get_connection(var2_key).password == "pass"
