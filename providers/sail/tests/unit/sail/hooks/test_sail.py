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

import pytest

from airflow.models import Connection
from airflow.providers.sail.hooks.sail import SailHook


class TestSailHook:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="sail-default",
                conn_type="sail",
                host="sc://sail-host",
                port=50051,
                login="sail-user",
                password="secret-token",
            )
        )

        create_connection_without_db(
            Connection(
                conn_id="sail-no-creds",
                conn_type="sail",
                host="sail-server",
            )
        )

        create_connection_without_db(
            Connection(
                conn_id="sail-with-ssl",
                conn_type="sail",
                host="sail-host",
                port=50051,
                extra='{"use_ssl": "True"}',
            )
        )

        create_connection_without_db(
            Connection(
                conn_id="sail-with-path",
                conn_type="sail",
                host="sc://cluster/app",
                login="sail-user",
            )
        )

    def test_get_connection_url_full(self):
        expected_url = "sc://sail-host:50051/;user_id=sail-user;token=secret-token"
        hook = SailHook(conn_id="sail-default")
        assert hook.get_connection_url() == expected_url

    def test_get_connection_url_no_creds(self):
        expected_url = "sc://sail-server/"
        hook = SailHook(conn_id="sail-no-creds")
        assert hook.get_connection_url() == expected_url

    def test_get_connection_url_with_ssl(self):
        expected_url = "sc://sail-host:50051/;use_ssl=True"
        hook = SailHook(conn_id="sail-with-ssl")
        assert hook.get_connection_url() == expected_url

    def test_get_connection_url_with_path_raises(self):
        hook = SailHook(conn_id="sail-with-path")
        with pytest.raises(ValueError, match="not supported in Sail connection URL"):
            hook.get_connection_url()

    def test_hook_attributes(self):
        assert SailHook.conn_type == "sail"
        assert SailHook.hook_name == "Sail"
        assert SailHook.default_conn_name == "sail_default"
