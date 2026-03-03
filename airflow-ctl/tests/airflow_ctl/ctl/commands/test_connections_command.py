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
from unittest.mock import patch

import pytest

from airflowctl.api.client import ClientKind
from airflowctl.api.datamodels.generated import (
    BulkActionResponse,
    BulkResponse,
    ConnectionBody,
    ConnectionCollectionResponse,
    ConnectionResponse,
)
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import connection_command


class TestCliConnectionCommands:
    connection_id = "test_connection"
    export_file_name = "exported_json.json"
    parser = cli_parser.get_parser()
    connection_collection_response = ConnectionCollectionResponse(
        connections=[
            ConnectionResponse(
                connection_id=connection_id,
                conn_type="test_type",
                host="test_host",
                login="test_login",
                password="test_password",
                port=1234,
                extra="{}",
                description="Test connection description",
            )
        ],
        total_entries=1,
    )
    bulk_response_success = BulkResponse(
        create=BulkActionResponse(success=[connection_id], errors=[]), update=None, delete=None
    )
    bulk_response_error = BulkResponse(
        create=BulkActionResponse(
            success=[],
            errors=[
                {
                    "error": f"The connection with these connection_ids: {{'{connection_id}'}} already exist.",
                    "status_code": 409,
                }
            ],
        ),
        update=None,
        delete=None,
    )

    def test_import_success(self, api_client_maker, tmp_path, monkeypatch):
        api_client = api_client_maker(
            path="/api/v2/connections",
            response_json=self.bulk_response_success.model_dump(),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )

        monkeypatch.chdir(tmp_path)
        expected_json_path = tmp_path / self.export_file_name
        connection_file = {
            self.connection_id: {
                "conn_type": "test_type",
                "host": "test_host",
                "login": "test_login",
                "password": "test_password",
                "port": 1234,
                "extra": "{}",
                "description": "Test connection description",
                "connection_id": self.connection_id,
            }
        }

        expected_json_path.write_text(json.dumps(connection_file))
        connection_command.import_(
            self.parser.parse_args(["connections", "import", expected_json_path.as_posix()]),
            api_client=api_client,
        )

    def test_import_error(self, api_client_maker, tmp_path, monkeypatch):
        api_client = api_client_maker(
            path="/api/v2/connections",
            response_json=self.bulk_response_error.model_dump(),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )

        monkeypatch.chdir(tmp_path)
        expected_json_path = tmp_path / self.export_file_name
        connection_file = {
            self.connection_id: {
                "conn_type": "test_type",
                "host": "test_host",
                "login": "test_login",
                "password": "test_password",
                "port": 1234,
                "extra": "{}",
                "description": "Test connection description",
                "connection_id": self.connection_id,
            }
        }

        expected_json_path.write_text(json.dumps(connection_file))
        with pytest.raises(SystemExit):
            connection_command.import_(
                self.parser.parse_args(["connections", "import", expected_json_path.as_posix()]),
                api_client=api_client,
            )

    def test_import_without_extra_field(self, api_client_maker, tmp_path, monkeypatch):
        """Import succeeds when JSON omits the ``extra`` field (#62653).

        Before the fix, ``v.get("extra", {})`` returned ``{}`` (a dict) when
        the key was absent, but ``ConnectionBody.extra`` expects ``str | None``,
        causing a Pydantic ``ValidationError``.
        """
        api_client = api_client_maker(
            path="/api/v2/connections",
            response_json=self.bulk_response_success.model_dump(),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )

        monkeypatch.chdir(tmp_path)
        json_path = tmp_path / self.export_file_name
        # Intentionally omit "extra" (and several other optional keys) to
        # mirror a minimal real-world connection JSON export.
        connection_file = {
            self.connection_id: {
                "conn_type": "test_type",
                "host": "test_host",
            }
        }

        json_path.write_text(json.dumps(connection_file))

        with patch(
            "airflowctl.ctl.commands.connection_command.ConnectionBody",
            wraps=ConnectionBody,
        ) as mock_body:
            connection_command.import_(
                self.parser.parse_args(["connections", "import", json_path.as_posix()]),
                api_client=api_client,
            )

        # Verify that ``extra`` was passed as None (not {} which would fail
        # Pydantic validation) and all other absent keys default correctly.
        mock_body.assert_called_once_with(
            connection_id=self.connection_id,
            conn_type="test_type",
            host="test_host",
            login=None,
            password=None,
            port=None,
            extra=None,
            description="",
        )
