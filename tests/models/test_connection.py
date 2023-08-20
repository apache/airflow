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

from airflow import AirflowException
from airflow.models import Connection


class TestConnection:
    @pytest.mark.parametrize(
        "uri, expected_conn_type, expected_host, expected_login, expected_password,"
        " expected_port, expected_schema, expected_extra_dict, expected_exception_message",
        [
            (
                "type://user:pass@host:100/schema",
                "type",
                "host",
                "user",
                "pass",
                100,
                "schema",
                {},
                None,
            ),
            (
                "type://user:pass@host/schema",
                "type",
                "host",
                "user",
                "pass",
                None,
                "schema",
                {},
                None,
            ),
            (
                "type://user:pass@host/schema?param1=val1&param2=val2",
                "type",
                "host",
                "user",
                "pass",
                None,
                "schema",
                {"param1": "val1", "param2": "val2"},
                None,
            ),
            (
                "type://host",
                "type",
                "host",
                None,
                None,
                None,
                "type",
                {},
                None,
            ),
            (
                "spark://mysparkcluster.com:80?deploy-mode=cluster&spark_binary=command&namespace=kube+namespace",
                "spark",
                "mysparkcluster.com",
                None,
                None,
                80,
                "spark",
                {"deploy-mode": "cluster", "spark_binary": "command", "namespace": "kube namespace"},
                None,
            ),
            (
                "spark://k8s://100.68.0.1:443?deploy-mode=cluster",
                "spark",
                "k8s://100.68.0.1",
                None,
                None,
                443,
                "spark",
                {"deploy-mode": "cluster"},
                None,
            ),
            (
                "type://protocol://user:pass@host:123?param=value",
                "type",
                "protocol://host",
                "user",
                "pass",
                123,
                "type",
                {"param": "value"},
                None,
            ),
            (
                "type://user:pass@protocol://host:port?param=value",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                r"Invalid connection string: type://user:pass@protocol://host:port?param=value.",
            ),
            (
                "postgres://jdbc:postgresql://10.0.0.1",
                "postgres",
                "jdbc:postgresql://10.0.0.1",
                None,
                None,
                None,
                "",
                {},
                r"Invalid connection string: postgres://jdbc:postgresql://10.0.0.1.",
            ),
            (
                "jdbc:postgresql://user:host@10.0.0.1/database",
                "jdbc",
                "postgresql://user:host@10.0.0.1/database",
                None,
                None,
                None,
                "jdbc",
                {},
                None,
            ),
            (
                "hive://jdbc+hive2://conn_id:conn_pass@localhost:10000/default?auth_mechanism=LDAP",
                "hive",
                "jdbc+hive2://localhost",
                "conn_id",
                "conn_pass",
                10000,
                "default",
                {"auth_mechanism": "LDAP"},
                None,
            ),
        ],
    )
    def test_parse_from_uri(
        self,
        uri,
        expected_conn_type,
        expected_host,
        expected_login,
        expected_password,
        expected_port,
        expected_schema,
        expected_extra_dict,
        expected_exception_message,
    ):
        if expected_exception_message is not None:
            with pytest.raises(AirflowException, match=re.escape(expected_exception_message)):
                Connection(uri=uri)
        else:
            conn = Connection(uri=uri)
            assert conn.conn_type == expected_conn_type
            assert conn.login == expected_login
            assert conn.password == expected_password
            assert conn.host == expected_host
            assert conn.port == expected_port
            assert conn.schema == expected_schema
            assert conn.extra_dejson == expected_extra_dict

    @pytest.mark.parametrize(
        "connection, expected_uri",
        [
            (
                Connection(
                    conn_type="type",
                    login="user",
                    password="pass",
                    host="host",
                    port=100,
                    schema="schema",
                    extra={"param1": "val1", "param2": "val2"},
                ),
                "type://user:pass@host:100/schema?param1=val1&param2=val2",
            ),
            (
                Connection(
                    conn_type="type",
                    host="protocol://host",
                    port=100,
                    schema="schema",
                    extra={"param1": "val1", "param2": "val2"},
                ),
                "type://protocol://host:100/schema?param1=val1&param2=val2",
            ),
            (
                Connection(
                    conn_type="type",
                    login="user",
                    password="pass",
                    host="protocol://host",
                    port=100,
                    schema="schema",
                    extra={"param1": "val1", "param2": "val2"},
                ),
                "type://protocol://user:pass@host:100/schema?param1=val1&param2=val2",
            ),
            (
                Connection(
                    conn_type="postgres",
                    login=None,
                    password=None,
                    host="jdbc:postgresql://user:pass@10.0.0.1:5432",
                    port=None,
                ),
                "postgres://jdbc:postgresql://user%3Apass%4010.0.0.1%3A5432",
            ),
            (
                Connection(
                    conn_type="postgres",
                    host="jdbc:postgresql://10.0.0.1",
                    port=5432,
                ),
                "postgres://jdbc:postgresql://10.0.0.1:5432",
            ),
        ],
    )
    def test_get_uri(self, connection, expected_uri):
        assert connection.get_uri() == expected_uri
