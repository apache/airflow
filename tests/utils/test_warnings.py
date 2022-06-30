#
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

import unittest

from airflow.utils.warnings import warn_list_secrets_alternative_backend


class TestWarnings(unittest.TestCase):
    def test_warn_list_secrets_alternative_backend_cli_connections(self):
        assert warn_list_secrets_alternative_backend("cli", "connection") == (
            "The Airflow CLI will not return Connections "
            "stored in an alternative secrets backend such as BaseSecretsBackend or "
            "EnvironmentVariablesBackend."
        )

    def test_warn_list_secrets_alternative_backend_cli_variables(self):
        assert warn_list_secrets_alternative_backend("cli", "variable") == (
            "The Airflow CLI will not return Variables "
            "stored in an alternative secrets backend such as BaseSecretsBackend or "
            "EnvironmentVariablesBackend."
        )

    def test_warn_list_secrets_alternative_backend_ui_connections(self):
        assert warn_list_secrets_alternative_backend("ui", "connection") == (
            "The Airflow UI will not return Connections "
            "stored in an alternative secrets backend such as BaseSecretsBackend or "
            "EnvironmentVariablesBackend."
        )

    def test_warn_list_secrets_alternative_backend_ui_variables(self):
        assert warn_list_secrets_alternative_backend("ui", "variable") == (
            "The Airflow UI will not return Variables "
            "stored in an alternative secrets backend such as BaseSecretsBackend or "
            "EnvironmentVariablesBackend."
        )
