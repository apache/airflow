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

from argparse import ArgumentParser

import airflow.providers.cncf.kubernetes.cli.definition as kubernetes_cli_definition
from airflow.cli.cli_config import GroupCommand


class TestCliDefinition:
    def test_kubernetes_group_commands(self):
        assert isinstance(kubernetes_cli_definition.KUBERNETES_COMMANDS, list)
        assert len(kubernetes_cli_definition.KUBERNETES_COMMANDS) > 0
        assert isinstance(kubernetes_cli_definition.KUBERNETES_COMMANDS[0], GroupCommand)

    def test__get_parser(self):
        parser = kubernetes_cli_definition._get_parser()
        assert isinstance(parser, ArgumentParser)
