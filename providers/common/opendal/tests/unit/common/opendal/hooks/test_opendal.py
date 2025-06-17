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

from typing import Any
from unittest.mock import Mock, patch

import pytest
from opendal import Operator

from airflow.providers.common.opendal.connections.connection_parser import OpenDALAirflowConnectionParser
from airflow.providers.common.opendal.hooks.opendal import OpenDALHook
from airflow.sdk import Connection


class TestOpenDALHook:
    def test_fetch_conn_with_opendal_input_config(self):
        """
        Test the fetch_conn method of OpenDALHook.
        """
        # Create a mock connection
        hook = OpenDALHook()
        hook.config = {"conn_id": "opendal_fs", "operator_args": {}, "path": "/tmp/file/hello.txt"}
        conn = Mock()
        conn.extra_dejson = {
            "source_config": {
                "operator_args": {"key": "value"},
            },
            "destination_config": {
                "operator_args": {"key": "value"},
            },
        }

        hook.get_connection = Mock(return_value=conn)

        result = hook.fetch_conn()

        assert result == conn

    @pytest.mark.parametrize("config_type", ["source", "destination"])
    def test_fetch_conn_with_opendal_default_conn_id_with_source(self, config_type):
        """
        Test the fetch_conn method of OpenDALHook with default connection ID.
        """
        # Create a mock connection
        hook = OpenDALHook(config_type=config_type, opendal_conn_id="opendal_default")
        hook.config = {"operator_args": {}, "path": "/tmp/file/hello.txt"}
        default_conn = Mock()
        default_conn.extra_dejson = {
            "source_config": {
                "conn_id": "aws_default",
                "operator_args": {"key": "value"},
            },
            "destination_config": {
                "conn_id": "aws_default",
                "operator_args": {"key": "value"},
            },
        }

        aws_default_conn = Mock()
        aws_default_conn.extra_dejson = {
            "aws_access_key_id": "test_aws_access_key_id",
            "aws_secret_access_key": "test_aws_secret_access_key",
            "region_name": "eu-central-1",
        }

        hook.get_connection = Mock(side_effect=[default_conn, aws_default_conn])
        result = hook.fetch_conn()

        assert result == aws_default_conn
        assert hook.get_connection.call_count == 2
        assert hook.get_connection.call_args_list[0][0][0] == "opendal_default"
        assert hook.get_connection.call_args_list[1][0][0] == "aws_default"

    @pytest.mark.parametrize("config_type", ["source", "destination"])
    @patch("airflow.providers.common.opendal.hooks.opendal.OpenDALHook.fetch_conn")
    def test_get_operator_use_operator_args_from_conn(self, mock_fetch_conn, config_type):
        """
        Test the get_operator method of OpenDALHook.
        """

        hook = OpenDALHook(config_type=config_type, opendal_conn_id="opendal_default")
        hook.config = {"operator_args": {}, "path": "/tmp/file/hello.txt"}

        mock_fetch_conn.return_value = Mock(
            conn_type="opendal",
            extra_dejson={
                "source_config": {
                    "operator_args": {"scheme": "fs", "root": "/tmp/"},
                },
                "destination_config": {
                    "operator_args": {"scheme": "fs", "root": "/tmp/"},
                },
            },
        )
        operator = hook.get_operator
        assert isinstance(operator, Operator)
        assert repr(operator) == 'Operator("fs", root="/tmp")'

    @pytest.mark.parametrize("config_type", ["source", "destination"])
    @patch("airflow.providers.common.opendal.hooks.opendal.OpenDALHook.fetch_conn")
    def test_get_operator_use_operator_args_from_input_config(self, mock_fetch_conn, config_type):
        """
        Test the get_operator method of OpenDALHook.
        """

        hook = OpenDALHook(config_type=config_type, opendal_conn_id="opendal_default")
        hook.config = {"operator_args": {"scheme": "fs", "root": "/tmp/"}, "path": "/tmp/file/hello.txt"}

        mock_fetch_conn.return_value = Mock(
            conn_type="opendal",
            extra_dejson={
                "source_config": {
                    "operator_args": {},
                },
                "destination_config": {
                    "operator_args": {},
                },
            },
        )
        operator = hook.get_operator
        assert isinstance(operator, Operator)
        assert repr(operator) == 'Operator("fs", root="/tmp")'

    def test_register_connection_parser(self):
        """Test register a custom connection parser."""

        class CustomOpenDALAirflowConnectionParser(OpenDALAirflowConnectionParser):
            airflow_conn_type = "opendal"

            def parse(self, conn: Connection) -> dict[str, Any]:
                return {"scheme": "opendal_scheme", "path": conn.extra_dejson.get("path")}

        hook = OpenDALHook(config_type="source", opendal_conn_id="opendal_default")
        hook.register_parsers(CustomOpenDALAirflowConnectionParser())

        assert len(hook._opendal_conn_factory._connection_parsers) >= 1
        assert any(
            isinstance(parser, CustomOpenDALAirflowConnectionParser)
            for parser in hook._opendal_conn_factory._connection_parsers
        )

    def test_register_connection_parser_and_get_operator_args(self):
        """Test register a custom connection parser and get operator args."""

        class CustomOpenDALAirflowConnectionParser(OpenDALAirflowConnectionParser):
            airflow_conn_type = "opendal"

            def parse(self, conn: Connection) -> dict[str, Any]:
                return {"scheme": "opendal_scheme", "path": conn.extra_dejson.get("path")}

        hook = OpenDALHook(config_type="source", opendal_conn_id="opendal_default")

        hook.config = {
            "operator_args": {"path": "/tmp/file.txt/"},
        }
        hook.register_parsers(CustomOpenDALAirflowConnectionParser())
        conn = Connection(
            conn_id="opendal_default",
            conn_type="opendal",
            extra='{"source_config": {"operator_args": {"root": "/tmp/"}}}',
        )

        assert hook._opendal_conn_factory.get_opendal_operator_args(
            conn,
            hook.config.get("operator_args", {}),
            hook.config_type,
        ) == {
            "scheme": "opendal_scheme",
            "path": "/tmp/file.txt/",
            "root": "/tmp/",
        }
