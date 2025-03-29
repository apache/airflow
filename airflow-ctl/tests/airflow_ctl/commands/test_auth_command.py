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
from unittest import mock
from unittest.mock import patch

from platformdirs import user_config_path

from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import auth_command


class TestCliAuthCommands:
    parser = cli_parser.get_parser()

    @patch.dict(os.environ, {"AIRFLOW_CLI_TOKEN": "TEST_TOKEN"})
    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_AUTH_LOGIN"})
    @patch("airflowctl.api.client.keyring")
    def test_login(self, mock_keyring):
        mock_keyring.set_password = mock.MagicMock()
        mock_keyring.get_password.return_value = None
        env = "TEST_AUTH_LOGIN"

        auth_command.login(self.parser.parse_args(["auth", "login", "--api-url", "http://localhost:8080"]))
        default_config_dir = user_config_path("airflow", "Apache Software Foundation")
        assert os.path.exists(default_config_dir)
        with open(os.path.join(default_config_dir, f"{env}.json")) as f:
            assert json.load(f) == {
                "api_url": "http://localhost:8080",
            }
