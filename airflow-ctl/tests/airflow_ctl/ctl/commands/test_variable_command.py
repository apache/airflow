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
import os

from airflowctl.api.client import ClientKind
from airflowctl.api.datamodels.generated import (
    BulkActionResponse,
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
    bulk_action_response = BulkActionResponse(success=[key], errors=[])

    def test_import(self, api_client_maker, tmp_path, monkeypatch):
        api_client = api_client_maker(
            path="/api/v2/variables",
            response_json=self.bulk_action_response.model_dump(),
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
        assert response == ([self.key], [])

    def test_export(self, api_client_maker, tmp_path, monkeypatch):
        api_client = api_client_maker(
            path="/api/v2/variables",
            response_json=self.variable_collection_response.model_dump(),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )

        monkeypatch.chdir(tmp_path)
        expected_json_path = (tmp_path / self.export_file_name).as_posix()
        variable_command.export(
            self.parser.parse_args(["variables", "export", expected_json_path]),
            api_client=api_client,
        )
        assert os.path.exists(tmp_path / self.export_file_name)

        with open(expected_json_path) as f:
            assert json.load(f) == {self.key: {"description": self.description, "value": self.value}}
