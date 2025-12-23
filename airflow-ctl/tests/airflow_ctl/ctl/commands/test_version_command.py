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

from contextlib import redirect_stdout
from io import StringIO
from unittest import mock

import pytest

from airflowctl.api.client import Client
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands.version_command import version_info


@pytest.fixture
def mock_client():
    """create a mock client"""
    with mock.patch("airflowctl.api.client.get_client") as mock_get_client:
        client = mock.MagicMock(spec=Client)
        mock_get_client.return_value.__enter__.return_value = client

    client.version.get.return_value.model_dump.return_value = {
        "version": "3.1.0",
        "git_version": None,
        "airflowctl_version": "0.1.0",
    }

    return client


class TestVersionCommand:
    """Test the version command."""

    parser = cli_parser.get_parser()

    def test_ctl_version_remote(self, mock_client):
        with redirect_stdout(StringIO()) as stdout:
            version_info(self.parser.parse_args(["version", "--remote"]), api_client=mock_client)
            assert "version" in stdout.getvalue()
            assert "git_version" in stdout.getvalue()
            assert "airflowctl_version" in stdout.getvalue()

    def test_ctl_version_only_local_version(self, mock_client):
        """Test the version command with an exception."""
        with redirect_stdout(StringIO()) as stdout:
            version_info(self.parser.parse_args(["version"]), api_client=mock_client)
            output = stdout.getvalue()
        assert "airflowctl_version" in output
