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

import json

import pytest

from airflow.exceptions import AirflowException
from airflow.sdk import Connection


class TestConnectionMethods:
    """Tests for the newly implemented Connection methods: parse_from_uri, from_json, and as_json."""

    def test_parse_from_uri(self):
        """Test parsing connection parameters from URI."""
        # Create an empty connection
        conn = Connection(conn_id="test_conn", conn_type="")
        
        # Parse connection parameters from URI
        conn.parse_from_uri("mysql://user:password@localhost:3306/test_schema")
        
        # Verify all parameters were parsed correctly
        assert conn.conn_type == "mysql"
        assert conn.host == "localhost"
        assert conn.login == "user"
        assert conn.password == "password"
        assert conn.schema == "test_schema"
        assert conn.port == 3306
        
    def test_parse_from_uri_with_protocol(self):
        """Test parsing connection with protocol in host."""
        conn = Connection(conn_id="test_conn", conn_type="")
        
        # URI with protocol in host
        conn.parse_from_uri("mysql://https://localhost:3306/test_schema")
        
        # Verify the protocol is part of the host
        assert conn.conn_type == "mysql"
        assert conn.host == "https://localhost"
        assert conn.port == 3306
        assert conn.schema == "test_schema"
        
    def test_parse_from_uri_with_extra(self):
        """Test parsing connection with extra parameters."""
        conn = Connection(conn_id="test_conn", conn_type="")
        
        # URI with extra parameters
        conn.parse_from_uri("mysql://localhost:3306/test_schema?param1=value1&param2=value2")
        
        # Verify extra parameters were parsed into JSON
        assert conn.conn_type == "mysql"
        assert conn.host == "localhost"
        assert conn.port == 3306
        assert conn.schema == "test_schema"
        assert conn.extra is not None
        
        # Parse the extra parameters to verify
        extra_dict = json.loads(conn.extra)
        assert extra_dict["param1"] == "value1"
        assert extra_dict["param2"] == "value2"
        
    def test_parse_from_uri_invalid(self):
        """Test parsing an invalid URI."""
        conn = Connection(conn_id="test_conn", conn_type="")
        
        # Invalid URI with too many schemes
        with pytest.raises(AirflowException, match="Invalid connection string"):
            conn.parse_from_uri("mysql://https://http://localhost")
            
    def test_from_json(self):
        """Test creating a connection from JSON string."""
        json_str = json.dumps({
            "conn_type": "mysql",
            "host": "localhost",
            "login": "user",
            "password": "password",
            "schema": "test_schema",
            "port": 3306,
            "extra": {"param1": "value1"}
        })
        
        conn = Connection.from_json(json_str, conn_id="test_conn")
        
        assert conn.conn_id == "test_conn"
        assert conn.conn_type == "mysql"
        assert conn.host == "localhost"
        assert conn.login == "user"
        assert conn.password == "password"
        assert conn.schema == "test_schema"
        assert conn.port == 3306
        assert conn.extra is not None
        
        # Verify extra was properly converted to JSON string
        extra_dict = json.loads(conn.extra)
        assert extra_dict["param1"] == "value1"
        
    def test_from_json_invalid_port(self):
        """Test creating a connection with invalid port from JSON."""
        json_str = json.dumps({
            "conn_type": "mysql",
            "port": "invalid_port"
        })
        
        with pytest.raises(ValueError, match="Expected integer value for `port`"):
            Connection.from_json(json_str)
            
    def test_as_json(self):
        """Test converting a connection to JSON string."""
        conn = Connection(
            conn_id="test_conn",
            conn_type="mysql",
            host="localhost",
            login="user",
            password="password",
            schema="test_schema",
            port=3306,
            extra=json.dumps({"param1": "value1"})
        )
        
        json_str = conn.as_json()
        conn_dict = json.loads(json_str)
        
        # Verify the connection was properly serialized to JSON
        assert "conn_id" not in conn_dict  # conn_id should be excluded
        assert conn_dict["conn_type"] == "mysql"
        assert conn_dict["host"] == "localhost"
        assert conn_dict["login"] == "user"
        assert conn_dict["password"] == "password"
        assert conn_dict["schema"] == "test_schema"
        assert conn_dict["port"] == 3306
        assert conn_dict["extra"] == json.dumps({"param1": "value1"}) 