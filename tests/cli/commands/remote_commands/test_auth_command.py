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

import base64
import json
import os
from contextlib import redirect_stdout
from io import StringIO

from airflow.cli import cli_parser
from airflow.cli.commands.remote_commands import auth_command


class TestCliAuthCommands:
    parser = cli_parser.get_parser()

    def test_login(self):
        with redirect_stdout(StringIO()) as stdout:
            auth_command.login(
                self.parser.parse_args(["auth", "login", "--api-url", "http://localhost:8080"])
            )
        stdout = stdout.getvalue()
        default_config_dir = os.path.expanduser("~/.config/airflow")
        assert f"Saving credentials to {default_config_dir}" in stdout
        assert os.path.exists(default_config_dir)
        with open(os.path.join(default_config_dir, "credentials.json")) as f:
            assert json.load(f) == {
                "api_url": "http://localhost:8080",
                "api_token": base64.b64encode(b"NO_TOKEN").decode(),
            }

    def test_configure(self):
        with redirect_stdout(StringIO()) as stdout:
            auth_command.configure(
                self.parser.parse_args(
                    ["auth", "configure", "--api-url", "http://localhost:8080", "--api-token", "test_token"]
                )
            )
        stdout = stdout.getvalue()
        default_config_dir = os.path.expanduser("~/.config/airflow")
        assert f"Saving credentials to {default_config_dir}" in stdout
        assert os.path.exists(default_config_dir)
        with open(os.path.join(default_config_dir, "credentials.json")) as f:
            assert json.load(f) == {
                "api_url": "http://localhost:8080",
                "api_token": base64.b64encode(b"test_token").decode(),
            }
