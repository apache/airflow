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

import io
import json
from contextlib import redirect_stdout

from airflow.cli import cli_parser
from airflow.cli.commands import provider_command


class TestCliProvidersAuth:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_list_auth_backend_module(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            provider_command.auth_backend_list(self.parser.parse_args(["providers", "auth", "--output=json"]))
            stdout = temp_stdout.getvalue()
        auth_mods = json.loads(stdout)
        for mod in auth_mods:
            assert mod.get("api_auth_backand_module") is not None


class TestCliProvidersBehaviours:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_list_connection_field(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            provider_command.connection_field_behaviours(
                self.parser.parse_args(["providers", "behaviours", "--output=json"])
            )
            stdout = temp_stdout.getvalue()
        fields = json.loads(stdout)
        for field in fields:
            assert field.get("field_behaviours") is not None


class TestCliProvidersGet:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_get_provider_info(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            provider_command.provider_get(
                self.parser.parse_args(
                    ["providers", "get", "apache-airflow-providers-http", "--full", "--output=json"]
                )
            )
            stdout = temp_stdout.getvalue()
        provider = json.loads(stdout)
        for info in provider:
            assert info.get("connection-types") is not None


class TestCliProvidersHooks:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_list_hook_module(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            provider_command.hooks_list(self.parser.parse_args(["providers", "hooks", "--output=json"]))
            stdout = temp_stdout.getvalue()
        hook_modules = json.loads(stdout)
        for mod in hook_modules:
            assert mod.get("class") is not None
            assert mod.get("conn_id_attribute_name") is not None
            assert mod.get("connection_type") is not None
            assert mod.get("hook_name") is not None


class TestCliProvidersLinks:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_list_link_module(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            provider_command.extra_links_list(self.parser.parse_args(["providers", "links", "--output=json"]))
            stdout = temp_stdout.getvalue()
        link_classes = json.loads(stdout)
        for link_class in link_classes:
            assert link_class.get("extra_link_class_name") is not None


class TestCliProvidersList:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_list_provider_module(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            provider_command.providers_list(self.parser.parse_args(["providers", "list", "--output=json"]))
            stdout = temp_stdout.getvalue()
        provider_modules = json.loads(stdout)
        for mod in provider_modules:
            assert mod.get("description") is not None
            assert mod.get("package_name") is not None
            assert mod.get("version") is not None


class TestCliProvidersLogging:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_list_logging_module(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            provider_command.logging_list(self.parser.parse_args(["providers", "logging", "--output=json"]))
            stdout = temp_stdout.getvalue()
        logging_modules = json.loads(stdout)
        for mod in logging_modules:
            assert mod.get("logging_class_name") is not None


class TestCliProvidersSecrets:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_list_secret_backend_module(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            provider_command.secrets_backends_list(
                self.parser.parse_args(["providers", "secrets", "--output=json"])
            )
            stdout = temp_stdout.getvalue()
        secret_backend_modules = json.loads(stdout)
        for mod in secret_backend_modules:
            assert mod.get("secrets_backend_class_name") is not None


class TestCliProvidersTriggers:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_list_trigger_module(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            provider_command.triggers_list(self.parser.parse_args(["providers", "triggers", "--output=json"]))
            stdout = temp_stdout.getvalue()
        trigger_modules = json.loads(stdout)
        for mod in trigger_modules:
            assert mod.get("class") is not None
            assert mod.get("integration_name") is not None
            assert mod.get("package_name") is not None


class TestCliProvidersWidgets:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_list_custom_connections(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            provider_command.connection_form_widget_list(
                self.parser.parse_args(["providers", "widgets", "--output=json"])
            )
            stdout = temp_stdout.getvalue()
        connections = json.loads(stdout)
        for conn in connections:
            assert conn.get("class") is not None
            assert conn.get("connection_parameter_name") is not None
            assert conn.get("field_type") is not None
            assert conn.get("package_name") is not None
