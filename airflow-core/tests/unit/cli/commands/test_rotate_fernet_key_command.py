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
from cryptography.fernet import Fernet
from sqlalchemy import select

from airflow.cli import cli_parser
from airflow.cli.commands import rotate_fernet_key_command
from airflow.models import Connection, Variable
from airflow.models.crypto import get_fernet
from airflow.sdk import BaseHook
from airflow.utils.session import provide_session

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_connections, clear_db_variables

pytestmark = pytest.mark.db_test


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
        with conf_vars({("core", "fernet_key"): ""}):
            get_fernet.cache_clear()  # Clear cached fernet
            Variable.set(key=var1_key, value="value")

        # Create encrypted variable
        with conf_vars({("core", "fernet_key"): fernet_key1.decode()}):
            get_fernet.cache_clear()  # Clear cached fernet
            Variable.set(key=var2_key, value="value")

        # Rotate fernet key
        with conf_vars({("core", "fernet_key"): f"{fernet_key2.decode()},{fernet_key1.decode()}"}):
            get_fernet.cache_clear()  # Clear cached fernet
            args = self.parser.parse_args(["rotate-fernet-key"])
            rotate_fernet_key_command.rotate_fernet_key(args)

        # Assert correctness using a new fernet key
        with conf_vars({("core", "fernet_key"): fernet_key2.decode()}):
            get_fernet.cache_clear()  # Clear cached fernet
            var1 = session.execute(select(Variable).where(Variable.key == var1_key)).scalar_one_or_none()
            # Unencrypted variable should be unchanged
            assert Variable.get(key=var1_key) == "value"
            assert var1._val == "value"
            assert Variable.get(key=var2_key) == "value"

    @provide_session
    def test_should_rotate_connection(self, session, mock_supervisor_comms):
        fernet_key1 = Fernet.generate_key()
        fernet_key2 = Fernet.generate_key()
        var1_key = f"{__file__}_var1"
        var2_key = f"{__file__}_var2"

        # Create unencrypted variable
        with conf_vars({("core", "fernet_key"): ""}):
            get_fernet.cache_clear()  # Clear cached fernet
            session.add(Connection(conn_id=var1_key, uri="mysql://user:pass@localhost"))
            session.commit()

        # Create encrypted variable
        with conf_vars({("core", "fernet_key"): fernet_key1.decode()}):
            get_fernet.cache_clear()  # Clear cached fernet
            session.add(Connection(conn_id=var2_key, uri="mysql://user:pass@localhost"))
            session.commit()

        # Rotate fernet key
        with conf_vars({("core", "fernet_key"): f"{fernet_key2.decode()},{fernet_key1.decode()}"}):
            get_fernet.cache_clear()  # Clear cached fernet
            args = self.parser.parse_args(["rotate-fernet-key"])
            rotate_fernet_key_command.rotate_fernet_key(args)

        def mock_get_connection(conn_id):
            conn = session.execute(
                select(Connection).where(Connection.conn_id == conn_id)
            ).scalar_one_or_none()
            if conn:
                from airflow.sdk.execution_time.comms import ConnectionResult

                return ConnectionResult(
                    conn_id=conn.conn_id,
                    conn_type=conn.conn_type or "mysql",  # Provide a default conn_type
                    host=conn.host,
                    login=conn.login,
                    password=conn.password,
                    schema_=conn.schema,
                    port=conn.port,
                    extra=conn.extra,
                )
            raise Exception(f"Connection {conn_id} not found")

        # Mock the send method to return our connection data
        mock_supervisor_comms.send.return_value = mock_get_connection(var1_key)

        # Assert correctness using a new fernet key
        with conf_vars({("core", "fernet_key"): fernet_key2.decode()}):
            get_fernet.cache_clear()  # Clear cached fernet

            # Unencrypted variable should be unchanged
            conn1: Connection = BaseHook.get_connection(var1_key)
            assert conn1.password == "pass"

            # Mock for the second connection
            mock_supervisor_comms.send.return_value = mock_get_connection(var2_key)
            assert BaseHook.get_connection(var2_key).password == "pass"
