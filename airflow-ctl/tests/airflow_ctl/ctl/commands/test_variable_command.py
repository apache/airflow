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
from types import SimpleNamespace

import pytest

from airflowctl.api.client import ClientKind
from airflowctl.api.datamodels.generated import (
    BulkActionResponse,
    BulkResponse,
    VariableCollectionResponse,
    VariableResponse,
)
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import variable_command


class TestCliVariableCommands:
    key = "key"
    value = "value"
    description = "description"
    export_file_name = "exported_json.json"
    parser = cli_parser.get_parser()
    variable_collection_response = VariableCollectionResponse(
        variables=[
            VariableResponse(
                key=key,
                value=value,
                description=description,
                is_encrypted=False,
            ),
        ],
        total_entries=1,
    )
    bulk_response_success = BulkResponse(
        create=BulkActionResponse(success=[key], errors=[]), update=None, delete=None
    )
    bulk_response_error = BulkResponse(
        create=BulkActionResponse(
            success=[],
            errors=[
                {"error": f"The variables with these keys: {{'{key}'}} already exist.", "status_code": 409}
            ],
        ),
        update=None,
        delete=None,
    )

    def test_import_success(self, api_client_maker, tmp_path, monkeypatch):
        api_client = api_client_maker(
            path="/api/v2/variables",
            response_json=self.bulk_response_success.model_dump(),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )

        monkeypatch.chdir(tmp_path)
        expected_json_path = tmp_path / self.export_file_name
        variable_file = {
            self.key: self.value,
        }

        expected_json_path.write_text(json.dumps(variable_file))
        response = variable_command.import_(
            self.parser.parse_args(["variables", "import", expected_json_path.as_posix()]),
            api_client=api_client,
        )
        assert response == [self.key]

    @pytest.mark.parametrize(
        "falsy_value",
        [
            "",
            0,
            False,
            None,
        ],
        ids=["empty_string", "zero", "false", "null"],
    )
    def test_import_falsy_values(self, tmp_path, monkeypatch, falsy_value):
        """Test that falsy values (empty string, 0, False) are correctly imported."""
        captured_variables = None

        def bulk(variables):
            nonlocal captured_variables
            captured_variables = variables
            return self.bulk_response_success

        api_client = SimpleNamespace(variables=SimpleNamespace(bulk=bulk))

        monkeypatch.chdir(tmp_path)
        expected_json_path = tmp_path / self.export_file_name
        variable_file = {
            self.key: {"value": falsy_value, "description": "test falsy value"},
        }

        expected_json_path.write_text(json.dumps(variable_file))
        response = variable_command.import_(
            self.parser.parse_args(["variables", "import", expected_json_path.as_posix()]),
            api_client=api_client,
        )
        assert response == [self.key]
        entity = captured_variables.actions[0].entities[0]
        assert entity.value.root == falsy_value
        assert entity.description == "test falsy value"

    def test_import_rejects_non_object_json(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        expected_json_path = tmp_path / self.export_file_name
        expected_json_path.write_text(json.dumps([self.key]))

        with pytest.raises(SystemExit) as exit_info:
            variable_command.import_(
                self.parser.parse_args(["variables", "import", expected_json_path.as_posix()]),
            )

        assert exit_info.value.code == 1
        output = capsys.readouterr().out
        assert "Invalid variable file:" in output
        assert expected_json_path.as_posix() in output

    def test_import_error(self, api_client_maker, tmp_path, monkeypatch):
        api_client = api_client_maker(
            path="/api/v2/variables",
            response_json=self.bulk_response_error.model_dump(),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )

        monkeypatch.chdir(tmp_path)
        expected_json_path = tmp_path / self.export_file_name
        variable_file = {
            self.key: self.value,
        }

        expected_json_path.write_text(json.dumps(variable_file))
        with pytest.raises(SystemExit):
            variable_command.import_(
                self.parser.parse_args(["variables", "import", expected_json_path.as_posix()]),
                api_client=api_client,
            )
