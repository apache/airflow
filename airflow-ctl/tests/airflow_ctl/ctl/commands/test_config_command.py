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

import contextlib
from io import StringIO
from unittest import mock

from airflowctl.api.client import Client
from airflowctl.ctl import cli_config, cli_parser


class TestCliConfigGetValue:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_should_display_value(self):
        mock_client = mock.MagicMock(spec=Client)
        mock_client.configs.get_value.return_value = {"value": "test_value"}

        args = self.parser.parse_args(["config", "get-value", "--section", "core", "--option", "test_key"])

        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            cli_config.get_value(args, cli_api_client=mock_client)

        assert temp_stdout.getvalue().strip() == "test_value"
        mock_client.configs.get_value.assert_called_once_with(section="core", option="test_key")

    # def test_should_not_raise_exception_when_section_for_config_with_value_defined_elsewhere_is_missing(self):
    #     pass

    # def test_should_raise_exception_when_option_is_missing(self):
    #     pass
