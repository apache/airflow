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

from unittest.mock import Mock, patch

import pytest
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy, TokenAwarePolicy

from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook


@pytest.fixture
@patch("airflow.providers.apache.cassandra.hooks.cassandra.BaseHook.get_connection")
def mock_cassandra_hook(mock_get_connection):
    conn = Mock()
    conn.host = "127.0.0.1,127.0.0.2"
    conn.port = "9042"
    conn.login = "user"
    conn.password = "pass"
    conn.schema = "test_keyspace"
    conn.extra_dejson = {
        "load_balancing_policy": "DCAwareRoundRobinPolicy",
        "load_balancing_policy_args": {
            "local_dc": "dc1",
            "used_hosts_per_remote_dc": 2,
        },
        "cql_version": "3.4.4",
        "ssl_options": {"ca_certs": "/path/to/certs"},
        "protocol_version": 4,
    }

    mock_get_connection.return_value = conn
    return CassandraHook("test_conn_id")


class TestCassandraHook:
    def test_init_with_valid_connection(self, mock_cassandra_hook):
        assert isinstance(mock_cassandra_hook.cluster, Cluster)
        assert mock_cassandra_hook.cluster.contact_points == ["127.0.0.1", "127.0.0.2"]
        assert mock_cassandra_hook.cluster.port == 9042
        assert isinstance(mock_cassandra_hook.cluster.auth_provider, PlainTextAuthProvider)
        assert (
            mock_cassandra_hook.cluster.load_balancing_policy.__class__.__name__ == "DCAwareRoundRobinPolicy"
        )
        assert mock_cassandra_hook.cluster.cql_version == "3.4.4"
        assert mock_cassandra_hook.cluster.ssl_options == {"ca_certs": "/path/to/certs"}
        assert mock_cassandra_hook.cluster.protocol_version == 4
        assert mock_cassandra_hook.keyspace == "test_keyspace"

    def test_get_conn_session_exist(self, mock_cassandra_hook):
        mock_cassandra_hook.session = Mock()
        mock_cassandra_hook.session.is_shutdown = False
        hook_session = mock_cassandra_hook.get_conn()

        assert isinstance(hook_session, Mock)
        assert not hook_session.is_shutdown

    def test_get_conn_session_not_exist(self, mock_cassandra_hook):
        mock_cassandra_hook.cluster.connect = Mock()
        hook_session = mock_cassandra_hook.get_conn()
        assert isinstance(hook_session, Mock)

    def test_get_cluster(self, mock_cassandra_hook):
        cluster = mock_cassandra_hook.get_cluster()
        assert cluster.contact_points == ["127.0.0.1", "127.0.0.2"]
        assert cluster.port == 9042
        assert isinstance(cluster.auth_provider, PlainTextAuthProvider)
        assert cluster.load_balancing_policy.__class__.__name__ == "DCAwareRoundRobinPolicy"
        assert cluster.cql_version == "3.4.4"
        assert cluster.ssl_options == {"ca_certs": "/path/to/certs"}
        assert cluster.protocol_version == 4
        assert mock_cassandra_hook.keyspace == "test_keyspace"

    def test_shutdown_cluster(self, mock_cassandra_hook):
        mock_cassandra_hook.cluster = Mock()
        mock_cassandra_hook.cluster.is_shutdown = False
        mock_cassandra_hook.cluster.shutdown.return_value = None
        mock_cassandra_hook.shutdown_cluster()

        assert not mock_cassandra_hook.cluster.is_shutdown
        mock_cassandra_hook.cluster.shutdown.assert_called_once()

    def test_get_lb_policy_dc_aware_round_robin_policy(self, mock_cassandra_hook):
        policy_args = {"local_dc": "dc1", "used_hosts_per_remote_dc": 2}
        lb_policy = mock_cassandra_hook.get_lb_policy("DCAwareRoundRobinPolicy", policy_args)

        assert isinstance(lb_policy, DCAwareRoundRobinPolicy)
        assert lb_policy.local_dc == "dc1"
        assert lb_policy.used_hosts_per_remote_dc == 2

    def test_get_lb_policy_token_aware_policy_dc_aware_round_robin_policy(self, mock_cassandra_hook):
        policy_args = {
            "child_load_balancing_policy": "DCAwareRoundRobinPolicy",
            "child_load_balancing_policy_args": {
                "local_dc": "dc1",
                "used_hosts_per_remote_dc": 3,
            },
        }
        lb_policy = mock_cassandra_hook.get_lb_policy("TokenAwarePolicy", policy_args)

        assert isinstance(lb_policy, TokenAwarePolicy)
        assert isinstance(lb_policy._child_policy, DCAwareRoundRobinPolicy)
        assert lb_policy._child_policy.local_dc == "dc1"
        assert lb_policy._child_policy.used_hosts_per_remote_dc == 3

    @patch("airflow.providers.apache.cassandra.hooks.cassandra.CassandraHook.get_conn")
    def test_table_exists(self, mock_get_conn, mock_cassandra_hook):
        mock_cluster_metadata = Mock()
        mock_cluster_metadata.keyspaces = {"test_keyspace": Mock(tables={"test_table": None})}
        mock_get_conn.return_value.cluster.metadata = mock_cluster_metadata

        result = mock_cassandra_hook.table_exists("test_keyspace.test_table")
        assert result
        mock_get_conn.assert_called_once()

    def test_sanitize_input_valid(self, mock_cassandra_hook):
        input_string = "valid_table_name"
        sanitized_string = mock_cassandra_hook._sanitize_input(input_string)
        assert sanitized_string == input_string

    def test_sanitize_input_invalid(self, mock_cassandra_hook):
        with pytest.raises(ValueError, match=r"Invalid input: invalid_table_name_with_%_characters"):
            mock_cassandra_hook._sanitize_input("invalid_table_name_with_%_characters")

    @patch("airflow.providers.apache.cassandra.hooks.cassandra.CassandraHook.get_conn")
    @patch("airflow.providers.apache.cassandra.hooks.cassandra.CassandraHook._sanitize_input")
    def test_record_exists_table(self, mock_sanitize_input, mock_get_conn, mock_cassandra_hook):
        table = "test_table"
        keys = {"key1": "value1", "key2": "value2"}
        mock_sanitize_input.return_value = table
        mock_get_conn.return_value.execute.return_value.one = Mock()
        result = mock_cassandra_hook.record_exists(table, keys)
        assert result
        mock_sanitize_input.assert_called_with(table)
        assert mock_sanitize_input.call_count == 2
        mock_get_conn.return_value.execute.assert_called_once_with(
            "SELECT * FROM test_table.test_table WHERE key1=%(key1)s AND key2=%(key2)s",
            {"key1": "value1", "key2": "value2"},
        )

    @patch("airflow.providers.apache.cassandra.hooks.cassandra.CassandraHook.get_conn")
    @patch("airflow.providers.apache.cassandra.hooks.cassandra.CassandraHook._sanitize_input")
    def test_record_exists_keyspace_table(self, mock_sanitize_input, mock_get_conn, mock_cassandra_hook):
        table = "test_keyspace.test_table"
        keys = {"key1": "value1", "key2": "value2"}
        mock_sanitize_input.return_value = "test_table"
        mock_get_conn.return_value.execute.return_value.one = Mock()
        result = mock_cassandra_hook.record_exists(table, keys)
        assert result
        mock_sanitize_input.assert_called_with("test_table")
        assert mock_sanitize_input.call_count == 3
        mock_get_conn.return_value.execute.assert_called_once_with(
            "SELECT * FROM test_table.test_table WHERE key1=%(key1)s AND key2=%(key2)s",
            {"key1": "value1", "key2": "value2"},
        )

    @patch("airflow.providers.apache.cassandra.hooks.cassandra.CassandraHook.get_conn")
    @patch("airflow.providers.apache.cassandra.hooks.cassandra.CassandraHook._sanitize_input")
    def test_record_exists_exception(self, mock_sanitize_input, mock_get_conn, mock_cassandra_hook):
        table = "test_keyspace.test_table"
        keys = {"key1": "value1", "key2": "value2"}
        mock_sanitize_input.return_value = "test_table"
        mock_get_conn.return_value.execute.side_effect = Exception("Test exception")
        result = mock_cassandra_hook.record_exists(table, keys)
        assert not result
        mock_sanitize_input.assert_called_with("test_table")
        assert mock_sanitize_input.call_count == 3
        mock_get_conn.return_value.execute.assert_called_once_with(
            "SELECT * FROM test_table.test_table WHERE key1=%(key1)s AND key2=%(key2)s",
            {"key1": "value1", "key2": "value2"},
        )
